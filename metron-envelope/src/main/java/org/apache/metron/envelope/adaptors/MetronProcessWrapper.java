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

package org.apache.metron.envelope.adaptors;

import com.cloudera.labs.envelope.component.ProvidesAlias;
import com.cloudera.labs.envelope.derive.Deriver;
import com.cloudera.labs.envelope.spark.Contexts;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigRenderOptions;
import envelope.shaded.com.google.common.collect.Iterables;
import org.apache.metron.common.utils.LazyLogger;
import org.apache.metron.common.utils.LazyLoggerFactory;
import org.apache.metron.envelope.encoding.SparkRowEncodingStrategy;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.jetbrains.annotations.NotNull;

import java.util.Map;
import java.util.Objects;

import static org.apache.metron.envelope.utils.ClassUtils.instantiateClass;



@SuppressWarnings({"rawtypes", "serial"})
/**
 * Adapts an envelope deriver (which wraps a spark partition mapper) for use by Metron.
 */
public class MetronProcessWrapper implements Deriver, ProvidesAlias {
  protected static LazyLogger LOG = LazyLoggerFactory.getLogger(MetronProcessWrapper.class);
  private static final String ALIAS = "MetronEnrichmentDeriver";

  private static final String SPARK_ROW_ENCODING = "SparkRowEncodingStrategy";
  private SparkRowEncodingStrategy encodingStrategy = null;

  private static final String ENRICHMENT_PARALLEL_TYPE = "enrichmentParallelType";

  private BaseMapper partitionMapper;

  /**
   * To make sure future config changes are actioned, we broadcast any changes of config to a client
   */
  private transient Broadcast<String> workerConfigBroadcast;

  @Override
  public String getAlias() {
    return ALIAS;
  }

  /**
   * Handles processing envelope configuratons form the lightbend config library
   * @param config config structure
   */
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

    final String partitionMapperName = config.getString(ENRICHMENT_PARALLEL_TYPE);
    // todo - preemptively read and validate configuration items here for ease of debugging
    partitionMapper = ((BaseMapper) instantiateClass(partitionMapperName))
            .withWorkerConfig(workerConfigBroadcast);
  }



  /**
   * Responsible for setting up the Spark processing stage
   * Runs in the context of the Spark Driver
   * @param srcDataset source dataset
   * @return Enriched Metron Data
   */
  @Override
  public Dataset<Row> derive(@NotNull Map<String, Dataset<Row>> srcDataset) {
    final Dataset<Row> src = Iterables.getOnlyElement(srcDataset.entrySet()).getValue();
    final SparkRowEncodingStrategy encodingStrategy = this.encodingStrategy;
    final MapPartitionsFunction<Row, Row> partitionEnricher = this.partitionMapper;
    // The execution of unifiedPartitionMapper happens on worker nodes
    final Dataset<Row> dst = src.mapPartitions(partitionEnricher, RowEncoder.apply(encodingStrategy.getOutputSparkSchema()));
    return dst;
  }
}
