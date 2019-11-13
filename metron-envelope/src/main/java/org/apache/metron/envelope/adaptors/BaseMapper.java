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

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import envelope.shaded.com.google.common.base.Charsets;
import org.apache.metron.envelope.encoding.SparkRowEncodingStrategy;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Row;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.apache.metron.envelope.utils.ClassUtils.instantiateClass;

/**
 * Abstracts out common code for use by individual processors
 * MapPartitionsFunction<Row, Row> is the type that spark expects
 * to bulk processes Data Partitions with
 */
public abstract class BaseMapper implements MapPartitionsFunction<Row, Row> {
  private static final String ZOOKEEPER = "ZookeeperQuorum";
  private static final String KAFKA_BROKERS = "KafkaBrokers";
  private static final String SPARK_ROW_ENCODING = "SparkRowEncodingStrategy";
  protected transient Map<String,String> additionalConfig;
  protected transient String zookeeperQuorum;
  protected transient String kafkaBrokers;
  protected transient SparkRowEncodingStrategy encodingStrategy;
  /**
   * String encoding of worker configuration - it only gets
   * de-serialised once the worker is up and running
   */
  private transient Broadcast<String> workerConfigBroadcast;

  private transient String jsonConfig;

  public BaseMapper() {
  }

  public BaseMapper(Broadcast<String> workerConfigBroadcast) {
    this.workerConfigBroadcast = workerConfigBroadcast;
  }

  public BaseMapper withWorkerConfig(Broadcast<String> workerConfigBroadcast) {
    this.workerConfigBroadcast = workerConfigBroadcast;
    return this;
  }

  /**
   * Hacky bit of coding to translate a jsonString into a lightbend Config object
   * For some reason lighbend do not think people would want to do this.
   * @param jsonData String of json data to render into a config structure
   * @return Config structure
   * @throws IOException if unable to create or write to temp file
   */
  private static Config extractConfig(String jsonData) throws IOException {
    Objects.requireNonNull(jsonData, "Broadcast config data is null");
    Path tempConfigFile = null;
    Config config;
    try {
      tempConfigFile = Files.createTempFile("worker-config", ".json.tmp");
      Files.write(tempConfigFile, jsonData.getBytes(Charsets.UTF_8));
      // read back in as config structure
      config = ConfigFactory.parseFile(tempConfigFile.toFile());
    } finally {
      if (tempConfigFile != null) {
        Files.deleteIfExists(tempConfigFile);
      }
    }
    return config;
  }

  /**
   * If current config is null, or the new config is different then a call to init is required
   * @return true if init needed, false otherwise
   */
  protected boolean initNeeded() {
    final String tmpConfig = workerConfigBroadcast.getValue();
    Objects.requireNonNull(tmpConfig, "Broadcast configuration is null");
    if ((jsonConfig == null) || (!jsonConfig.equals(tmpConfig))) {
      jsonConfig = tmpConfig;
      return true;
    } else {
      return false;
    }
  }

  /**
   * This should be called in the context of the worker - so it come hydrate the
   * configuration on the worker's JVM, creating all the relevant enrichment objects and Kafka clients
   * This function assumes that jsonConfig has been extracted by a previous call to
   * initNeeded()
   */
  protected Config init() throws IOException {
    Objects.requireNonNull(jsonConfig);
    Config config = extractConfig(jsonConfig);
    // set variables
    zookeeperQuorum = Objects.requireNonNull(config.getString(ZOOKEEPER), "Zookeeper quorum is required");
    kafkaBrokers = Objects.requireNonNull(config.getString(KAFKA_BROKERS), "KafkaBrokers is required");
    final String rowEncodingStrategy  = Objects.requireNonNull(config.getString(SPARK_ROW_ENCODING));
    encodingStrategy = (SparkRowEncodingStrategy) instantiateClass(rowEncodingStrategy);
    encodingStrategy.init();

    // Read in additional config, flatten values to strings even if they are nested JSON objects
    additionalConfig = config
            .getObject("config")
            .unwrapped()
            .entrySet()
            .stream()
            .collect(Collectors.toMap(Object::toString, Object::toString));

    // For use by overiding implementations
    return config;
  }
}
