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
import com.typesafe.config.Config;
import org.apache.metron.common.Constants;
import org.apache.metron.common.configuration.SensorParserConfig;
import org.apache.metron.common.error.MetronError;
import org.apache.metron.common.message.metadata.MetadataUtil;
import org.apache.metron.common.message.metadata.RawMessage;
import org.apache.metron.common.message.metadata.RawMessageStrategy;
import org.apache.metron.common.utils.LazyLogger;
import org.apache.metron.common.utils.LazyLoggerFactory;
import org.apache.metron.envelope.adaptors.BaseMapper;
import org.apache.metron.envelope.utils.Either;
import org.apache.metron.envelope.ZookeeperClient;
import org.apache.metron.envelope.config.ParserConfigManager;
import org.apache.metron.envelope.utils.MetronErrorProcessor;
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

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static org.apache.metron.envelope.utils.ErrorUtils.convertErrorsToMetronErrors;
import static org.apache.metron.envelope.utils.SparkKafkaUtils.ENVELOPE_KAFKA_TOPICNAME_FIELD;

/**
 * Responsible for Initialising and running metron parsers over a a spark partition's worth of incoming data from Kafka.
 * This class assumes that data is incoming via Kafka, c.f. extractMetadata() for the implementation of that assumption
 */
public class MetronSparkParser extends BaseMapper {
  private static LazyLogger LOG = LazyLoggerFactory.getLogger(MetronSparkParser.class);
  // Made static so it does not need to be recreated for every spark partition processed
  private static transient volatile ParserConfigManager parserConfigManager;
  // Made static so it does not need to be recreated for every spark partition processed
  private static transient volatile Context stellarContext;

  private transient MetronErrorProcessor metronErrorProcessor;
  private transient ParserRunner<JSONObject> envelopeParserRunner;
  private transient Map<String, String> topicToSensorMap;

  /**
   * Parser config is kept static to prevent zookeeper clients having to be re-created for each partition
   * @param zookeeperQuorum Source of configuration information
   */
  static synchronized void initParserConfigManager(String zookeeperQuorum) {
    if (parserConfigManager != null) {
      parserConfigManager = new ParserConfigManager(zookeeperQuorum);
    }
    return;
  }

  /**
   * Initialises Stellar Context
   * @param sensorTypes The different sensor types stellar may encounter
   * @param zookeeperQuorum Source of configuration info for stellar
   * @param parserConfigManager Source of configuration info for stellar
   */
  @NotNull
  private static synchronized void initStellarContext(@NotNull final List<String> sensorTypes, String zookeeperQuorum, ParserConfigManager parserConfigManager) {
    if (stellarContext != null) {
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
      Context tmpStellarContext = builder.build();
      StellarFunctions.initialize(tmpStellarContext);
      stellarContext = tmpStellarContext;
    }
    return;
  }


  @Override
  protected Config init() throws IOException {
    Config config = super.init();

    if (parserConfigManager == null) {
      initParserConfigManager(zookeeperQuorum);
    }
    if (metronErrorProcessor != null) {
      metronErrorProcessor.close();
    }
    metronErrorProcessor = new MetronErrorProcessor(kafkaBrokers, parserConfigManager);
    final List<String> sensorTypes = parserConfigManager.getConfigurations().getTypes();

    if (stellarContext == null) {
      initStellarContext(sensorTypes,zookeeperQuorum,parserConfigManager);
    }

    topicToSensorMap = createTopicToSensorMap(sensorTypes);
    envelopeParserRunner = new ParserRunnerImpl(new HashSet<>(sensorTypes));
    envelopeParserRunner.init(() -> parserConfigManager.getConfigurations(), stellarContext);

    return config;
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
  public Stream<? extends Row> processMessage(@Nullable Row nullableRow) {
    if (nullableRow == null) {
      return Stream.empty();
    }
    Row row = Objects.requireNonNull(nullableRow);

    // Run Configured Metron parsers across the input data
    final ParserRunnerResults<JSONObject> runnerResults = Objects.requireNonNull(getResults(row));

    // encode results into Spark Rows
    final Stream<Either<MetronError, RowWithSchema>> encodedMessages =
            runnerResults.getMessages().stream()
                    .filter( x -> {
                      if (x == null) LOG.warn("Parsing produced a null message - ignoring");
                      return x != null;
                    })
                    .map(convertErrorsToMetronErrors(Constants.ErrorType.PARSER_ERROR,
                            x -> encodingStrategy.encodeParserResultIntoSparkRow(x)));

    // report any errors
    Stream.of(
            runnerResults.getErrors().stream(),
            encodedMessages.flatMap(Either::getErrorStream)
    )
    .flatMap(Function.identity()) // flatten to a simple stream of MetronErrors
    .forEach(err -> {
              LOG.error("Error parsing message {}", err.getJSONObject());
              metronErrorProcessor.handleMetronError(err);
            }
    );

    // return an iterable of our results
    return encodedMessages
            .flatMap(Either::getDataStream); // filters for and extracts data from the 'Either<>' struct
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

  @Override
  public Iterator<Row> call(Iterator<Row> input) throws Exception {
    if (initNeeded()) {
      init();
    }
    // Convert input iterator into a steam of spark input rows
    final Iterable<Row> iterable = () -> input;
    final Stream<Row> inputRowStream = StreamSupport.stream(iterable.spliterator(), false);
    return inputRowStream
            .flatMap(this::processMessage)
            .map(x -> (Row) x) // Java type system not good enough to realise x implements row already
            .iterator();
  }
}



