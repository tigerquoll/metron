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
import com.cloudera.labs.envelope.spark.Contexts;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigRenderOptions;
import envelope.shaded.com.google.common.base.Preconditions;
import envelope.shaded.com.google.common.collect.Iterables;
import org.apache.metron.common.utils.LazyLogger;
import org.apache.metron.common.utils.LazyLoggerFactory;
import org.apache.metron.envelope.encoding.SparkRowEncodingStrategy;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

import static org.apache.metron.envelope.utils.ClassUtils.instantiateClass;

/**
 * Uses an adapter to enrich telemetry messages with additional metadata
 * entries from multiple sources is one go. For a list of available enrichment adapters see
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
public class MetronUnifiedEnrichmentDeriver implements Deriver, ProvidesAlias {
  @NotNull private static final String ALIAS = "MetronUnifiedDeriver";
  @NotNull private static LazyLogger LOG = LazyLoggerFactory.getLogger(MetronUnifiedEnrichmentDeriver.class);
  @NotNull private static final String SPARK_ROW_ENCODING = "SparkRowEncodingStrategy";
  private SparkRowEncodingStrategy encodingStrategy = null;
  transient private Broadcast<String> workerConfigBroadcast;

  @Override
  // envelope configuration - this happens on the driver
  // We should push JSON string to workers to process
  public void configure(Config config) {
    if (workerConfigBroadcast != null) {
      // wait for all current workers to finish
      workerConfigBroadcast.unpersist(true);
    }
    final JavaSparkContext jSparkContext = new JavaSparkContext(Contexts.getSparkSession().sparkContext());
    workerConfigBroadcast = jSparkContext.broadcast(config.root().render(ConfigRenderOptions.concise()));
    final String rowEncodingStrategy  = Objects.requireNonNull(config.getString(SPARK_ROW_ENCODING));
    encodingStrategy = (SparkRowEncodingStrategy) instantiateClass(rowEncodingStrategy);
    encodingStrategy.init();
  }

  @Override
  public String getAlias() {
    return ALIAS;
  }

  @Override
  /**
   * This is still happening on the spark driver
   */
  public Dataset<Row> derive(@NotNull Map<String, Dataset<Row>> srcDataset) throws IOException {
    Preconditions.checkArgument(srcDataset.size() == 1, getAlias() + " should only have one dependant dataset");
    final Dataset<Row> src = Iterables.getOnlyElement(srcDataset.entrySet()).getValue();
    final SparkRowEncodingStrategy encodingStrategy = this.encodingStrategy;
    final Broadcast<String> workerConfigBroadcast = this.workerConfigBroadcast;
    final MetronUnifiedPartitionMapper unifiedPartitionMapper = new MetronUnifiedPartitionMapper(workerConfigBroadcast);
    // todo - preemptively serialise and validate configuration items here for ease of debugging
    // The contents of this call are executed on worker nodes
    final Dataset<Row> dst = src.mapPartitions(unifiedPartitionMapper, RowEncoder.apply(encodingStrategy.getOutputSparkSchema()));
    return dst;
  }
}
