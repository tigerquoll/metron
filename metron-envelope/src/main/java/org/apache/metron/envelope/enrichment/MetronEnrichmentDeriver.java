/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.metron.envelope.enrichment;

import com.cloudera.labs.envelope.component.ProvidesAlias;
import com.cloudera.labs.envelope.derive.Deriver;
import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.google.common.collect.Iterables;
import com.typesafe.config.Config;
import envelope.shaded.com.google.common.collect.FluentIterable;
import org.apache.metron.common.Constants;
import org.apache.metron.common.configuration.enrichment.SensorEnrichmentConfig;
import org.apache.metron.common.error.MetronError;
import org.apache.metron.common.performance.PerformanceLogger;
import org.apache.metron.common.utils.LazyLogger;
import org.apache.metron.common.utils.LazyLoggerFactory;
import org.apache.metron.enrichment.cache.CacheKey;
import org.apache.metron.enrichment.configuration.Enrichment;
import org.apache.metron.enrichment.interfaces.EnrichmentAdapter;
import org.apache.metron.enrichment.utils.EnrichmentUtils;
import org.apache.metron.envelope.encoding.CborEncodingStrategy;
import org.apache.metron.envelope.encoding.SparkRowEncodingStrategy;
import org.apache.metron.envelope.parsing.MetronSparkPartitionParser;
import org.apache.metron.stellar.dsl.Context;
import org.apache.metron.stellar.dsl.StellarFunctions;
import org.apache.metron.storm.common.bolt.ConfiguredEnrichmentBolt;
import org.apache.metron.storm.common.utils.StormErrorUtils;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.json.simple.JSONObject;
import parquet.Preconditions;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import static org.apache.metron.common.Constants.STELLAR_CONTEXT_CONF;

/**
 * Uses an adapter to enrich telemetry messages with additional metadata
 * entries. For a list of available enrichment adapters see
 * {@link org.apache.metron.enrichment.adapters}.
 * <p>
 * At the moment of release the following enrichment adapters are available:
 * <p>
 * <ul>
 * <li>geo = attaches geo coordinates to IPs
 * <li>whois = attaches whois information to domains
 * <li>host = attaches reputation information to known hosts
 * <li>CIF = attaches information from threat intelligence feeds
 * </ul>
 * <p>
 * Enrichments are optional
 **/

@SuppressWarnings({"rawtypes", "serial"})
public class MetronEnrichmentDeriver implements Deriver, ProvidesAlias {
  private static final String ALIAS = "MetronEnricher";
  private static final String ZOOKEEPER = "zookeeper";
  private static LazyLogger LOG = LazyLoggerFactory.getLogger(MetronEnrichmentDeriver.class);
  private String zookeeperQuorum = null;
  private SparkRowEncodingStrategy encodingStrategy = new CborEncodingStrategy();

  @Override
  // envelope configuration
  public void configure(Config config) {
    zookeeperQuorum = Objects.requireNonNull(config.getString(ZOOKEEPER), "Zookeeper quorum is required");
  }

  @Override
  public String getAlias() {
    return ALIAS;
  }

  @Override
  public Dataset<Row> derive(Map<String, Dataset<Row>> srcDataset) throws Exception {
    Preconditions.checkArgument(srcDataset.size() == 1, getAlias() + " should only have one dependant dataset");
    final Dataset<Row> src = Iterables.getOnlyElement(srcDataset.entrySet()).getValue();
    final SparkRowEncodingStrategy encodingStrategy = this.encodingStrategy;
    final String zookeeperQuorum = this.zookeeperQuorum;

    final Dataset<Row> dst = src.mapPartitions(new MapPartitionsFunction<Row, Row>() {
      // The following function gets created from scratch for every spark partition processed
      @Override
      public Iterator<Row> call(Iterator<Row> iterator) throws Exception {
        final MetronSparkPartitionEnricher metronSparkPartitionEnricher = new MetronSparkPartitionEnricher(zookeeperQuorum, encodingStrategy);
        metronSparkPartitionEnricher.init();
        // we can get away with this as long as the iterator is only used once;
        // we use iterator transforms so we can stream iterator processing
        // which means we do not require the entire dataset to be pulled into memory at once
        return FluentIterable.from(() -> iterator)
                // transformAndConcat is the guava equivalent of a flatmap
                .transformAndConcat(metronSparkPartitionEnricher)
                .iterator();
      }
    }, RowEncoder.apply(encodingStrategy.getParserOutputSchema()));

    return dst;
  }



  // Made protected to allow for error testing in integration test. Directly flaws inputs while everything is functioning hits other
  // errors, so this is made available in order to ensure ERROR_STREAM is output properly.
  protected void handleError(String key, JSONObject rawMessage, String subGroup, JSONObject enrichedMessage, Exception e) {
    LOG.error("[Metron] Unable to enrich message: {}", rawMessage, e);
    if (key != null) {
      collector.emit(enrichmentType, new Values(key, enrichedMessage, subGroup));
    }
    MetronError error = new MetronError()
            .withErrorType(Constants.ErrorType.ENRICHMENT_ERROR)
            .withThrowable(e)
            .addRawMessage(rawMessage);
    StormErrorUtils.handleError(collector, error);
  }


}
