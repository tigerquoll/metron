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

import com.cloudera.labs.envelope.spark.RowWithSchema;
import com.cloudera.labs.envelope.translate.Translator;
import com.github.benmanes.caffeine.cache.Cache;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.metron.common.Constants;
import org.apache.metron.common.configuration.SensorParserConfig;
import org.apache.metron.common.error.MetronError;
import org.apache.metron.common.message.metadata.MetadataUtil;
import org.apache.metron.common.message.metadata.RawMessage;
import org.apache.metron.common.message.metadata.RawMessageStrategy;
import org.apache.metron.common.utils.LazyLogger;
import org.apache.metron.common.utils.LazyLoggerFactory;
import org.apache.metron.envelope.utils.Either;
import org.apache.metron.envelope.ZookeeperClient;
import org.apache.metron.envelope.config.ParserConfigManager;
import org.apache.metron.envelope.encoding.SparkRowEncodingStrategy;
import org.apache.metron.envelope.utils.MetronErrorHandler;
import org.apache.metron.envelope.utils.MetronErrorHandlerImpl;
import org.apache.metron.envelope.utils.SparkKafkaUtils;
import org.apache.metron.parsers.ParserRunner;
import org.apache.metron.parsers.ParserRunnerImpl;
import org.apache.metron.parsers.ParserRunnerResults;
import org.apache.metron.stellar.common.CachingStellarProcessor;
import org.apache.metron.stellar.dsl.Context;
import org.apache.metron.stellar.dsl.StellarFunctions;
import org.apache.spark.sql.Row;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.json.simple.JSONObject;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.metron.envelope.utils.MetronErrorUtils.convertErrorsToMetronErrors;
import static org.apache.metron.envelope.utils.SparkKafkaUtils.ENVELOPE_KAFKA_TOPICNAME_FIELD;

/**
 * Responsible for Initialising and running metron parsers over a a spark partition's worth of incoming data from Kafka.
 * This class assumes that data is incoming via Kafka, c.f. extractMetadata() for the implementation of that assumption
 */
public class MetronSparkPartitionParser implements envelope.shaded.com.google.common.base.Function<Row, Iterable<Row>> {
  @NotNull public static final String UNKNOWN = "Unknown";
  public static final String METRON_SPARK_PARSER_ERROR_REPORTER = "Metron Spark Parser Error Reporter";
  @NotNull private static LazyLogger LOGGER = LazyLoggerFactory.getLogger(MetronSparkPartitionParser.class);

  @NotNull private ParserConfigManager parserConfigManager;
  @NotNull private String zookeeperQuorum;
  @NotNull private SparkRowEncodingStrategy encodingStrategy;
  @NotNull private MetronErrorHandler metronErrorHandler;
  @NotNull private String kafkaBrokers;

  private ParserRunner<JSONObject> envelopeParserRunner;
  private Map<String, String> topicToSensorMap;

  // Map between SensorType and the relevant errorHandling logic
  private Map<String, MetronErrorHandler> metronErrorHandlers = null;

  // Kafka Producer for handling errors
  private static volatile KafkaProducer<String,String> metronErrorKafkaProducer;

  static void initMetronErrorKafkaProducer(String kafkaBrokers) {
    synchronized (MetronSparkPartitionParser.class) {
      if (metronErrorKafkaProducer != null) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBrokers);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, METRON_SPARK_PARSER_ERROR_REPORTER);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        metronErrorKafkaProducer = new KafkaProducer<>(props);
      }
    }
  }

  /**
   * This object gets reconstructed for every partition of data,
   * so it re-reads its configuration fresh from zookeeper
   * @param zookeeperQuorum Zookeeper address of configuration
   */
  MetronSparkPartitionParser(@NotNull String zookeeperQuorum,
                             @NotNull String kafkaBrokers,
                             @NotNull SparkRowEncodingStrategy rowEncodingStrategy) {
    this.zookeeperQuorum = Objects.requireNonNull(zookeeperQuorum);
    this.encodingStrategy = Objects.requireNonNull(rowEncodingStrategy);
    this.kafkaBrokers = Objects.requireNonNull(kafkaBrokers);
  }

  /**
   * Prepares the object for processing
   * @throws Exception on error
   */
  public void init() throws Exception {
    parserConfigManager = new ParserConfigManager(zookeeperQuorum);
    if (metronErrorKafkaProducer == null) {
      initMetronErrorKafkaProducer(kafkaBrokers);
    }

    @NotNull final List<String> sensorTypes = parserConfigManager.getConfigurations().getTypes();
    metronErrorHandlers = createSensorTypeToErrorHandler(sensorTypes);
    topicToSensorMap = createTopicToSensorMap(sensorTypes);
    envelopeParserRunner = new ParserRunnerImpl(new HashSet<>(sensorTypes));
    envelopeParserRunner.init(() -> parserConfigManager.getConfigurations(), initializeStellar(sensorTypes));
  }

  private Map<String, MetronErrorHandler> createSensorTypeToErrorHandler(@NotNull List<String> sensorTypes) {
    return sensorTypes.stream()
            .collect(Collectors.toMap(
                    sensor -> sensor,
                    sensor -> new MetronErrorHandlerImpl(metronErrorKafkaProducer,
                            parserConfigManager.getConfigurations().getSensorParserConfig(sensor).getErrorTopic())));
  }


  /**
   * Used to map incoming data to the processing configured for all data coming from that sensor
   * @param sensorTypes  All of the sensor types
   * @return Map of sensor data to configured processing settings
   */
  @NotNull
  private Map<String, String> createTopicToSensorMap(@NotNull final Collection<String> sensorTypes) {
    @NotNull final Map<String, String> retval = new HashMap<>();
    for (String sensorType : sensorTypes) {
      @Nullable final SensorParserConfig parserDriveConfig = parserConfigManager.getConfigurations().getSensorParserConfig(sensorType);
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
  @Nullable
  private ParserRunnerResults<JSONObject> getResults(@NotNull Row row) {
    // Get original message and extract metadata (such as source topic which we can get source) from it
    @NotNull final byte[] originalMessage = Objects.requireNonNull(row.getAs(Translator.VALUE_FIELD_NAME));
    @NotNull Map<String, Object> metadata = SparkKafkaUtils.extractKafkaMetadata(row);
    @NotNull final String kafkaTopic = Objects.requireNonNull(metadata.get(ENVELOPE_KAFKA_TOPICNAME_FIELD)).toString();

    // Use kafka metadata to backtrack to the providing sensor so sensor-level configurations can be retrieved and actioned
    @NotNull final String sensorType = Objects.requireNonNull(topicToSensorMap.get(kafkaTopic));

    @NotNull final SensorParserConfig parserDriverConfig = Objects.requireNonNull(
            parserConfigManager.getConfigurations().getSensorParserConfig(sensorType));

    metadata = AddMetadataPrefixIfConfigured(metadata, parserDriverConfig);

    // Pre-process the raw message as configured by the parser driver configuration
    @NotNull final RawMessage rawMessage = Objects.requireNonNull(getRawMessage(originalMessage, metadata, parserDriverConfig));

    // return the results of running the configured metron parsers over the row
    return envelopeParserRunner.execute(sensorType, rawMessage, parserConfigManager.getConfigurations());
  }


  /**
   * Run the configured list of Metron Parsers across the passed Row
   * If any errors are present, pass them back encoded into the return row values
   * @param nullableRow Input rows to parse
   * @return  Output spark sql rows of processed data (success and errors) with schema 'outputSchema'
   */
  @NotNull
  @Override
  public Iterable<Row> apply(@Nullable Row nullableRow) {
    if (nullableRow == null) {
      return Collections.emptyList();
    }
    @NotNull Row row = Objects.requireNonNull(nullableRow);

    // Run Configured Metron parsers across the input data
    @NotNull final ParserRunnerResults<JSONObject> runnerResults = Objects.requireNonNull(getResults(row));

    // encode results into Spark Rows
    @NotNull final Stream<Either<MetronError, RowWithSchema>> encodedMessages =
            runnerResults.getMessages().stream()
                    .filter( x -> {
                      if (x == null) LOGGER.warn("Parsing produced a null message - ignoring");
                      return x != null;
                    })
                    .map(convertErrorsToMetronErrors(Constants.ErrorType.PARSER_ERROR,
                            x -> encodingStrategy.encodeParserResultIntoSparkRow(x)));

    // report any errors
    Stream.of(
            runnerResults.getErrors().stream(),
            encodedMessages.flatMap(Either::getErrors)
    )
    .flatMap(Function.identity()) // flatten to a simple stream of MetronErrors
    .forEach(err -> {
            LOGGER.error("Error parsing message {}", err.getJSONObject());
            metronErrorHandler.handleError(err);
            }
    );

    // return an iterable of our results
    return encodedMessages
            .flatMap(Either::getData)
            .collect(Collectors.toList());
  }

  /**
   * Initialises Stellar Context
   * @param sensorTypes The different sensor types stellar may encounter
   * @return Stellar Context
   */
  @NotNull
  private Context initializeStellar(@NotNull final List<String> sensorTypes) {
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
  @NotNull
  private RawMessage getRawMessage(@NotNull byte[] originalMessage, @NotNull Map<String, Object> metadata, @NotNull SensorParserConfig parserDriverConfig) {
    @NotNull final RawMessageStrategy rawMessageStrategy = Objects.requireNonNull(parserDriverConfig.getRawMessageStrategy());

    return Objects.requireNonNull(rawMessageStrategy.get(metadata,
            originalMessage,
            parserDriverConfig.getReadMetadata(),
            parserDriverConfig.getRawMessageStrategyConfig()));
  }

  /**
   * Parsers can be configured to prefix metadata fields with a certain string
   * @param metadata  Metadata map
   * @param parserDriverConfig  Sensor configuration
   * @return Metadata map updated with prefixes if configured, else original metadata map
   */
  @NotNull
  private Map<String, Object> AddMetadataPrefixIfConfigured(@NotNull Map<String, Object> metadata, @NotNull SensorParserConfig parserDriverConfig) {
    @Nullable final String metadataPrefix = MetadataUtil.INSTANCE.getMetadataPrefix(parserDriverConfig.getRawMessageStrategyConfig());
    if ((metadataPrefix != null) && (metadataPrefix.length() > 0)) {
      // prefix all metadata keys with configured prefix
      metadata = metadata.entrySet().stream()
              .collect(Collectors.toMap(k -> metadataPrefix + k, Function.identity()));
    }
    return metadata;
  }
}



