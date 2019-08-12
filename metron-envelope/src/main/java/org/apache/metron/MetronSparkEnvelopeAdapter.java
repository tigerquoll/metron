package org.apache.metron;

import com.cloudera.labs.envelope.component.CanReturnErroredData;
import com.cloudera.labs.envelope.component.ComponentFactory;
import com.cloudera.labs.envelope.component.InstantiatedComponent;
import com.cloudera.labs.envelope.component.InstantiatesComponents;
import com.cloudera.labs.envelope.component.ProvidesAlias;
import com.cloudera.labs.envelope.derive.Deriver;
import com.cloudera.labs.envelope.schema.Schema;
import com.cloudera.labs.envelope.translate.Translator;
import com.cloudera.labs.envelope.utils.SchemaUtils;
import com.cloudera.labs.envelope.validate.ProvidesValidations;
import com.cloudera.labs.envelope.validate.Validations;
import com.github.benmanes.caffeine.cache.Cache;
import com.typesafe.config.Config;
import envelope.shaded.com.google.common.collect.ImmutableMap;
import envelope.shaded.com.google.common.collect.ImmutableSet;
import envelope.shaded.com.google.common.collect.Maps;
import org.apache.curator.framework.CuratorFramework;
import org.apache.metron.common.configuration.ParserConfigurations;
import org.apache.metron.common.configuration.SensorParserConfig;
import org.apache.metron.common.configuration.SensorParserGroup;
import org.apache.metron.common.message.metadata.MetadataUtil;
import org.apache.metron.common.message.metadata.RawMessage;
import org.apache.metron.common.message.metadata.RawMessageStrategy;
import org.apache.metron.common.utils.JSONUtils;
import org.apache.metron.common.zookeeper.ConfigurationsCache;
import org.apache.metron.common.zookeeper.ZKConfigurationsCache;
import org.apache.metron.parsers.ParserRunner;
import org.apache.metron.parsers.ParserRunnerImpl;
import org.apache.metron.parsers.ParserRunnerResults;
import org.apache.metron.parsers.interfaces.MessageParser;
import org.apache.metron.parsers.interfaces.MessageParserResult;
import org.apache.metron.stellar.common.CachingStellarProcessor;
import org.apache.metron.stellar.dsl.Context;
import org.apache.metron.stellar.dsl.StellarFunctions;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.json.simple.JSONObject;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

public class MetronSparkEnvelopeAdapter implements Translator, ProvidesAlias, InstantiatesComponents, Deriver {
  private static final String ZOOKEEPER = "zookeeper";
  private static final String KAFKA_TOPICNAME_FIELD = "topic";
  private final static ImmutableSet<String> requiredMetadataFields = ImmutableSet.of(Translator.VALUE_FIELD_NAME, KAFKA_TOPICNAME_FIELD);
  public static final String KAFKA_TIMESTAMP_FIELD = "timestamp";
  public static final String KAFKA_PARTITION_FIELD = "partition";
  public static final String KAFKA_OFFSET_FIELD = "offset";
  private StructType providedSchema;
  private StructType expectingSchema;
  private ConfigurationsCache configCache;
  private ParserConfigurations parserConfigurations;
  private Map<String, String> topicToSensorMap = new HashMap<>();

  private String zookeeperQuorum;

  private ParserRunner<JSONObject> envelopeParserRunner;

  public MetronSparkEnvelopeAdapter() {

  }

  public ConfigurationsCache initCache(CuratorFramework client) {
    return new ZKConfigurationsCache( client, ZKConfigurationsCache.ConfiguredTypes.PARSER);
  }

  @Override
  public Set<InstantiatedComponent> getComponents(Config config, boolean b) throws Exception {
    return null;
  }

  @Override
  public String getAlias() {
    return null;
  }

  public Map<String, Object> extractMetadata(org.apache.spark.sql.Row row) {
    return ImmutableMap.of(
            "timestamp", (long)row.getAs(KAFKA_TIMESTAMP_FIELD),
            "topic", (String)row.getAs(KAFKA_TOPICNAME_FIELD),
            "partition", (int) row.getAs(KAFKA_PARTITION_FIELD),
            "offset", (long) row.getAs(KAFKA_OFFSET_FIELD));
  }

  @Override
  public Iterable<org.apache.spark.sql.Row> translate(org.apache.spark.sql.Row row) throws Exception {
    // from k,v bigger rows grow
    final byte[] originalMessage = row.getAs(Translator.VALUE_FIELD_NAME);
    Map<String, Object> metadata = extractMetadata(row);
    final String sensorType = topicToSensorMap.get(metadata.get("topic"));
    final SensorParserConfig parserDriverConfig = parserConfigurations.getSensorParserConfig(sensorType);
    final String metadataPrefix = MetadataUtil.INSTANCE.getMetadataPrefix(parserDriverConfig.getRawMessageStrategyConfig());
    if ((metadataPrefix != null) && (metadataPrefix.length() > 0)) {
      final Map<String, Object> prefixedMetadata = metadata.entrySet().stream()
            .collect(Collectors.toMap(k -> metadataPrefix + k, Function.identity()));
      metadata = prefixedMetadata;
    }

    final RawMessageStrategy rawMessageStrategy = parserDriverConfig.getRawMessageStrategy();
    final RawMessage rawMessage  = rawMessageStrategy.get(metadata,
            originalMessage,
            parserDriverConfig.getReadMetadata(),
            parserDriverConfig.getRawMessageStrategyConfig());

    ParserRunnerResults<JSONObject> parsedData = envelopeParserRunner.execute(sensorType, rawMessage, parserConfigurations);

    parsedData.getMessages();
    parsedData.getErrors();
    return null;
  }

  @Override
  public org.apache.spark.sql.types.StructType getExpectingSchema() {
    // binary string data being passed at the moment
    // TODO: investigate AVRO schema registry use
    return expectingSchema;
  }

  @Override
  public org.apache.spark.sql.types.StructType getProvidingSchema() {
    // What we provide to our callers - CBOR/binary encoded JSON data in a binary field called "binvalue"
    return providedSchema;
  }

  protected final ParserConfigurations configurations = new ParserConfigurations();

  protected SensorParserConfig getSensorParserConfig(String sensorType) {
    return configurations.getSensorParserConfig(sensorType);
  }

  protected Context initializeStellar(final List<String> sensorTypes) {
    Map<String, Object> cacheConfig = new HashMap<>();
    for (String sensorType: sensorTypes) {
      SensorParserConfig config = getSensorParserConfig(sensorType);

      if (config != null) {
        cacheConfig.putAll(config.getCacheConfig());
      }
    }
    Cache<CachingStellarProcessor.Key, Object> cache = CachingStellarProcessor.createCache(cacheConfig);

    Context.Builder builder = new Context.Builder()
            .with(Context.Capabilities.ZOOKEEPER_CLIENT, () -> ZookeeperClient.getZKInstance(zookeeperQuorum))
            .with(Context.Capabilities.GLOBAL_CONFIG, configurations::getGlobalConfig)
            .with(Context.Capabilities.STELLAR_CONFIG, configurations::getGlobalConfig)
            ;
    if(cache != null) {
      builder = builder.with(Context.Capabilities.CACHE, () -> cache);
    }
    Context stellarContext = builder.build();
    StellarFunctions.initialize(stellarContext);
    return stellarContext;
  }

  // envelope
  @Override
  public void configure(Config config) {
    // returned row type single string type for JSON Encoded data?
    // single binary field for Avro encoded data?
    // break up data into source specific schemas?
    // raw string
    // JSON string
    // Parsed? JSON data
    providedSchema = DataTypes.createStructType(new StructField[]{DataTypes.createStructField("cborvalue", DataTypes.BinaryType, false)});
    expectingSchema = SchemaUtils.binaryValueSchema();

    zookeeperQuorum = Objects.requireNonNull(config.getString(ZOOKEEPER), "Zookeeper quorum is required");
    initCache(ZookeeperClient.getZKInstance(zookeeperQuorum));
    // read parser type
    // read parser config
    // initialise parser
    // configure parser

    // sensorParserGroup(List<Sensor>)
    // Sensor->SensorParserGroup->ParserConfig
    // we should be able to get all these from zookeeper.

    parserConfigurations = configCache.get(ParserConfigurations.class);
    List<String> sensorTypes = parserConfigurations.getTypes();

    for (String sensorType : sensorTypes) {
      SensorParserConfig parserDriveConfig = getSensorParserConfig(sensorType);
      if (parserDriveConfig != null) {
        parserDriveConfig.init();
        topicToSensorMap.put(parserDriveConfig.getSensorTopic(), sensorType);
      } else {
        throw new IllegalStateException(
                "Unable to retrieve a parser config for " + sensorType);
      }
      // We need a list of sensors, so that we can configure multiple sensor support
      // does this mean one topic per sensor?  maybe???
      // how else do we know which sensor produced which record?
    }

    envelopeParserRunner = new ParserRunnerImpl(new HashSet<>(sensorTypes));
    envelopeParserRunner.init( () -> configCache.get(ParserConfigurations.class) , initializeStellar(sensorTypes) );

  }

  @Override
  public Validations getValidations() {
    return null;
  }


}
