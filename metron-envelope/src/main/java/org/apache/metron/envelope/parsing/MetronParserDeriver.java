/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.metron.envelope.parsing;

import com.cloudera.labs.envelope.component.ProvidesAlias;
import com.cloudera.labs.envelope.derive.Deriver;
import com.google.common.collect.Iterables;
import com.typesafe.config.Config;
import envelope.shaded.com.google.common.collect.FluentIterable;
import org.apache.metron.envelope.encoding.SparkRowEncodingStrategy;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import parquet.Preconditions;

import java.util.Iterator;
import java.util.Map;
import java.util.Objects;

import static org.apache.metron.envelope.utils.ClassUtils.instantiateClass;

/**
 * Adapts metron parsers to the Envelope Deriver abstraction
 */
public class MetronParserDeriver implements Deriver, ProvidesAlias {
  private static final String ALIAS = "metronParserAdapter";
  private static final String ZOOKEEPER = "ZookeeperQuorum";
  private static final String SPARK_ROW_ENCODING = "SparkRowEncodingStrategy";

  private String parserName = null;
  private String zookeeperQuorum = null;
  private SparkRowEncodingStrategy encodingStrategy = null;

  @Override
  public String getAlias() {
    return ALIAS;
  }

  // envelope configuration
  @Override
  public void configure(Config config) {
    zookeeperQuorum = Objects.requireNonNull(config.getString(ZOOKEEPER), "Zookeeper quorum is required");
    parserName = Objects.requireNonNull(config.getString("parser-name"), "parser name is required");

    final String rowEncodingStrategy  = Objects.requireNonNull(config.getString(SPARK_ROW_ENCODING));
    encodingStrategy = (SparkRowEncodingStrategy) instantiateClass(rowEncodingStrategy);
    encodingStrategy.init();
  }

  @Override
  public Dataset<Row> derive(Map<String, Dataset<Row>> srcDataset) {
    Preconditions.checkArgument(srcDataset.size() == 1, getAlias() + " should only have one dependant dataset");
    final Dataset<Row> src = Iterables.getOnlyElement(srcDataset.entrySet()).getValue();
    final SparkRowEncodingStrategy encodingStrategy = this.encodingStrategy;
    final String zookeeperQuorum = this.zookeeperQuorum;

    final Dataset<Row> dst = src.mapPartitions(new MapPartitionsFunction<Row, Row>() {
      // The following function gets created from scratch for every spark partition processed
      @Override
      public Iterator<Row> call(Iterator<Row> iterator) throws Exception {
        final MetronSparkPartitionParser metronSparkPartitionParser = new MetronSparkPartitionParser(zookeeperQuorum, encodingStrategy);
        metronSparkPartitionParser.init();
        // we can get away with this as long as the iterator is only used once;
        // we use iterator transforms so we can stream iterator processing
        // which means we do not require the entire dataset to be pulled into memory at once
        // Could we use java 8 streams for this?
        // If we did, I'm not sure the entire expression would be lazy, best to be sure.
        return FluentIterable.from(() -> iterator)
                // transformAndConcat is the guava equivalent of a flatMap
                .transformAndConcat(metronSparkPartitionParser)
                .iterator();
      }
    }, RowEncoder.apply(encodingStrategy.getOutputSparkSchema()));

    return dst;
  }

}
