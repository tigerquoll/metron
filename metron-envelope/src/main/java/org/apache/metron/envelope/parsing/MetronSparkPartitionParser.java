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

import com.cloudera.labs.envelope.translate.Translator;
import com.github.benmanes.caffeine.cache.Cache;
import com.google.common.collect.Iterables;
import envelope.shaded.com.google.common.collect.ImmutableMap;
import org.apache.metron.common.configuration.SensorParserConfig;
import org.apache.metron.common.error.MetronError;
import org.apache.metron.common.message.metadata.MetadataUtil;
import org.apache.metron.common.message.metadata.RawMessage;
import org.apache.metron.common.message.metadata.RawMessageStrategy;
import org.apache.metron.common.utils.LazyLogger;
import org.apache.metron.common.utils.LazyLoggerFactory;
import org.apache.metron.envelope.ZookeeperClient;
import org.apache.metron.envelope.config.ParserConfigManager;
import org.apache.metron.envelope.encoding.SparkRowEncodingStrategy;
import org.apache.metron.parsers.ParserRunner;
import org.apache.metron.parsers.ParserRunnerImpl;
import org.apache.metron.parsers.ParserRunnerResults;
import org.apache.metron.stellar.common.CachingStellarProcessor;
import org.apache.metron.stellar.dsl.Context;
import org.apache.metron.stellar.dsl.StellarFunctions;
import org.apache.spark.sql.Row;
import org.json.simple.JSONObject;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Responsible for Initialising and running metron parsers over a a spark partition's worth of incoming data from Kafka.
 * This class assumes that data is incoming via Kafka, c.f. extractMetadata() for the implementation of that assumption
 */
public class MetronSparkPartitionParser implements envelope.shaded.com.google.common.base.Function<Row, Iterable<Row>> {
  private static final String KAFKA_TOPICNAME_FIELD = "topic";
  private static final String KAFKA_TIMESTAMP_FIELD = "timestamp";
  private static final String KAFKA_PARTITION_FIELD = "partition";
  private static final String KAFKA_OFFSET_FIELD = "offset";
  private static final String ENVELOPE_KAFKA_TOPICNAME_FIELD = KAFKA_TOPICNAME_FIELD;
  private static final String ENVELOPE_KAFKA_TIMESTAMP_FIELD = KAFKA_TIMESTAMP_FIELD;
  private static final String ENVELOPE_KAFKA_PARTITION_FIELD = KAFKA_PARTITION_FIELD;
  private static final String ENVELOPE_KAFKA_OFFSET_FIELD = KAFKA_OFFSET_FIELD;
  private static LazyLogger LOGGER = LazyLoggerFactory.getLogger(MetronSparkPartitionParser.class);

  private ParserConfigManager parserConfigManager;
  private String zookeeperQuorum;
  private SparkRowEncodingStrategy encodingStrategy;
  private ParserRunner<JSONObject> envelopeParserRunner;
  private Map<String, String> topicToSensorMap;

  /**
   * This object gets reconstructed for every partition of data,
   * so it re-reads its configuration fresh from zookeeper
   * @param zookeeperQuorum Zookeeper address of configuration
   */
  MetronSparkPartitionParser(String zookeeperQuorum, SparkRowEncodingStrategy rowEncodingStrategy) {
    this.zookeeperQuorum = zookeeperQuorum;
    this.encodingStrategy = rowEncodingStrategy;
    this.parserConfigManager = new ParserConfigManager(zookeeperQuorum);
  }

  /**
   * Encodes assumptions about input data format
   * @param row Row of data extracted from Kafka
   * @return extracted metadata
   */
  private Map<String, Object> extractKafkaMetadata(org.apache.spark.sql.Row row) {
    return ImmutableMap.of(
            ENVELOPE_KAFKA_TIMESTAMP_FIELD, row.getAs(KAFKA_TIMESTAMP_FIELD),
            ENVELOPE_KAFKA_TOPICNAME_FIELD, row.getAs(KAFKA_TOPICNAME_FIELD),
            ENVELOPE_KAFKA_PARTITION_FIELD, row.getAs(KAFKA_PARTITION_FIELD),
            ENVELOPE_KAFKA_OFFSET_FIELD, row.getAs(KAFKA_OFFSET_FIELD));
  }

  /**
   * Prepares the object for processing
   * @throws Exception on error
   */
  public void init() throws Exception {
    this.encodingStrategy.init();
    final List<String> sensorTypes = parserConfigManager.getConfigurations().getTypes();
    topicToSensorMap = createTopicToSensorMap(sensorTypes);
    envelopeParserRunner = new ParserRunnerImpl(new HashSet<>(sensorTypes));
    envelopeParserRunner.init(() -> parserConfigManager.getConfigurations(), initializeStellar(sensorTypes));
  }



  /**
   * Used to map incoming data to the processing configured for all data coming from that sensor
   * @param sensorTypes  All of the sensor types
   * @return Map of sensor data to configured processing settings
   */
  private Map<String, String> createTopicToSensorMap(final Collection<String> sensorTypes) {
    final Map<String, String> retval = new HashMap<>();
    for (String sensorType : sensorTypes) {
      final SensorParserConfig parserDriveConfig = parserConfigManager.getConfigurations().getSensorParserConfig(sensorType);
      if (parserDriveConfig != null) {
        parserDriveConfig.init();
        retval.put(parserDriveConfig.getSensorTopic(), sensorType);
      } else {
        throw new IllegalStateException("Unable to retrieve a parser config for " + sensorType);
      }
    }
    return retval;
  }

  /**
   * Run the list of provided parsers across the provided row and return
   * The Metron ParserRunnerResults
   * @param row Row of spark input data that Metron needs to process (as spark kafka input schema)
   * @return Metron parser results
   */
  private ParserRunnerResults<JSONObject> getResults(Row row) {
    // Get original message and extract metadata (such as source topic which we can get source) from it
    final byte[] originalMessage = row.getAs(Translator.VALUE_FIELD_NAME);
    Map<String, Object> metadata = extractKafkaMetadata(row);

    // Use kafka metadata to backtrack to the providing sensor so sensor-level configurations can be retrieved and actioned
    final String sensorType = topicToSensorMap.get(metadata.get(ENVELOPE_KAFKA_TOPICNAME_FIELD).toString());
    final SensorParserConfig parserDriverConfig = parserConfigManager.getConfigurations().getSensorParserConfig(sensorType);
    metadata = AddMetadataPrefixIfConfigured(metadata, parserDriverConfig);

    // Pre-process the raw message as configured by the parser driver configuration
    final RawMessage rawMessage = getRawMessage(originalMessage, metadata, parserDriverConfig);

    // return the results of running the configured metron parsers over the row
    return envelopeParserRunner.execute(sensorType, rawMessage, parserConfigManager.getConfigurations());
  }

  /**
   * Run the configured list of Metron Parsers across the passed Row
   * If any errors are present, pass them back encoded into the return row values
   * Input rows are expected to
   * Output spark sql rows of processed data (success and errors) with schema 'outputSchema'
   */
  @Nullable
  @Override
  public Iterable<Row> apply(Row row) {
    final ParserRunnerResults<JSONObject> runnerResults = getResults(row);

    final List<Row> encodedResults = runnerResults.getMessages().stream()
            .map(x -> encodingStrategy.encodeParserResultIntoSparkRow(x))
            .filter(Objects::nonNull)
            .collect(Collectors.toList());

    List<Row> encodedErrors = Collections.emptyList();
    final List<MetronError> metronErrors = runnerResults.getErrors();
    if ((metronErrors != null) && (metronErrors.size() > 0)) {
      // should we  direct errors into a separate kafka queue?
      // yes
      encodedErrors = metronErrors.stream()
              .map(x -> encodingStrategy.encodeParserErrorIntoSparkRow(x))
              .filter(Objects::nonNull)
              .collect(Collectors.toList());
    }

    return Iterables.concat(encodedResults, encodedErrors);
  }

  /**
   * Initialises Stellar Context
   * @param sensorTypes The different sensor types stellar may encounter
   * @return Stellar Context
   */
  private Context initializeStellar(final List<String> sensorTypes) {
    Map<String, Object> cacheConfig = new HashMap<>();
    for (String sensorType : sensorTypes) {
      SensorParserConfig config = parserConfigManager.getConfigurations().getSensorParserConfig(sensorType);
      if (config != null) {
        cacheConfig.putAll(config.getCacheConfig());
      }
    }
    Cache<CachingStellarProcessor.Key, Object> cache = CachingStellarProcessor.createCache(cacheConfig);

    Context.Builder builder = new Context.Builder()
            .with(Context.Capabilities.ZOOKEEPER_CLIENT, () -> ZookeeperClient.getZKInstance(zookeeperQuorum))
            .with(Context.Capabilities.GLOBAL_CONFIG, () -> parserConfigManager.getConfigurations().getGlobalConfig())
            .with(Context.Capabilities.STELLAR_CONFIG, () -> parserConfigManager.getConfigurations().getGlobalConfig());
    if (cache != null) {
      builder = builder.with(Context.Capabilities.CACHE, () -> cache);
    }
    Context stellarContext = builder.build();
    StellarFunctions.initialize(stellarContext);
    return stellarContext;
  }

  /**
   * Parsers can be configured to parse and pre-process messages in various ways, this is encapsulated in a raw message strategy
   * @param originalMessage starting message
   * @param metadata Metadata extracted from the starting message
   * @param parserDriverConfig Configuration on how to treat the message
   * @return The pre-processed message ready for parsing.
   */
  private RawMessage getRawMessage(byte[] originalMessage, Map<String, Object> metadata, SensorParserConfig parserDriverConfig) {
    final RawMessageStrategy rawMessageStrategy = parserDriverConfig.getRawMessageStrategy();
    return rawMessageStrategy.get(metadata,
            originalMessage,
            parserDriverConfig.getReadMetadata(),
            parserDriverConfig.getRawMessageStrategyConfig());
  }

  /**
   * Parsers can be configured to prefix metadata fields with a certain string
   * @param metadata  Metadata map
   * @param parserDriverConfig  Sensor configuration
   * @return Metadata map updated with prefixes if configured, else original metadata map
   */
  private Map<String, Object> AddMetadataPrefixIfConfigured(Map<String, Object> metadata, SensorParserConfig parserDriverConfig) {
    final String metadataPrefix = MetadataUtil.INSTANCE.getMetadataPrefix(parserDriverConfig.getRawMessageStrategyConfig());
    if ((metadataPrefix != null) && (metadataPrefix.length() > 0)) {
      // prefix all metadata keys with configured prefix
      metadata = metadata.entrySet().stream()
              .collect(Collectors.toMap(k -> metadataPrefix + k, Function.identity()));
    }
    return metadata;
  }
}



