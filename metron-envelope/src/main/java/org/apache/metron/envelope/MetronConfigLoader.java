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
package org.apache.metron.envelope;

import com.cloudera.labs.envelope.component.ProvidesAlias;
import com.cloudera.labs.envelope.configuration.ConfigLoader;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import envelope.shaded.com.google.common.base.Charsets;
import envelope.shaded.com.google.common.base.Preconditions;
import envelope.shaded.com.google.common.collect.ImmutableSet;
import org.apache.metron.common.configuration.SensorParserConfig;
import org.apache.metron.common.utils.LazyLogger;
import org.apache.metron.common.utils.LazyLoggerFactory;
import org.apache.metron.envelope.config.ParserConfigManager;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;


public class MetronConfigLoader implements ConfigLoader, ProvidesAlias {
  private final static LazyLogger LOG = LazyLoggerFactory.getLogger(MetronConfigLoader.class);

  private static final String CONFIG_LOADER_ALIAS = "metron-config-adaptor";
  private static final String SPARK_ROW_ENCODING_STRATEGY = "spark-row-encoding-strategy";
  private static final String ZOOKEEPER_QUORUM = "zookeeper-quorum";
  private static final String CONFIG_ADAPTOR_TYPE = "adaptor-type";
  private static final String CONFIG_ADAPTOR_TYPE_PARSER_ALIAS = "metron-parser";
  private static final String CONFIG_PARSER_NAME = "parser-name";

  private static final String KAFKA_SECTION_NAME = "kafka";
  private static final String KAFKA_BROKER = "brokers";
  private static final String KAFKA_PARAMETER_PREFIX = "parameter";
  public static final String KAFKA_SASL_MECHANISM = KAFKA_PARAMETER_PREFIX + ".sasl.mechanism";
  public static final String KAFKA_SASL_KERB_SERVICE_NAME = KAFKA_PARAMETER_PREFIX + ".sasl.kerberos.service.name";
  public static final String KAFKA_SSL_TRUSTSTORE_LOCATION = KAFKA_PARAMETER_PREFIX + ".ssl.truststore.location";
  public static final String KAFKA_SSL_TRUSTSTORE_PASSWORD = KAFKA_PARAMETER_PREFIX + ".ssl.truststore.password";
  public static final String KAFKA_SECURITY_PROTOCOL = KAFKA_PARAMETER_PREFIX + ".security.protocol";

  private static final Set<PosixFilePermission> tempfilePerms = PosixFilePermissions.fromString("rwx------");
  private static final FileAttribute<Set<PosixFilePermission>> tempfileAttributes = PosixFilePermissions
          .asFileAttribute(tempfilePerms);

  private static final String PARSER_TEMPLATE = "steps {\n"+
          "  metron-parser-%s {\n"+
          "    input {\n"+
          "      type = kafka\n"+
          "      topics = [%s]\n"+
          "      group.id = metron-parser-%s\n"+
          "      # kafka-brokers\n"+
          "      kafka-brokers = %s\n"+
          "      # Kafka parameters \n" +
          "      %s\n"+
          "      translator {\n"+
          "        type = raw\n"+
          "      }\n"+
          "    }\n"+
          "    deriver {\n"+
          "      type = metronParserAdapter\n"+
          "      parser-name = %s\n"+
          "      spark-row-encoding-strategy = %s\n"+
          "      # zookeeper-quorum\n"+
          "      zookeeper-quorum = %s\n"+
          "      # kafka-brokers (used for error output)\n"+
          "      kafka-brokers = %s\n"+
          "      # Kafka parameters (used for error output)\n" +
          "      %s\n"+
          "    }\n"+
          "    output {\n"+
          "      type = kafka\n"+
          "      # Kafka Broker"+
          "      brokers = %s \n"+
          "      # Kafka parameters\n" +
          "      %s\n"+
          "      # default topic is Enrichments\n" +
          "      topic = %s\n"+
          "      serializer {\n"+
          "        type = delimited\n"+
          "        field.delimiter = \",\"\n"+
          "      }\n"+
          "    }\n"+
          "  }  \n"+
          "}";
  // todo: separate error stream to error topic
  @NotNull
  private final Set<String> supportedAdaptorTypes = ImmutableSet.of(
          org.apache.metron.envelope.parsing.MetronSparkPartitionParser.class.getSimpleName());

  @Nullable private String adaptorType;
  @Nullable private String parserName;
  @Nullable private String parserInputTopic;
  @Nullable private String parserOutputTopic;
  @Nullable private String sparkRowEncodingStrategy;
  @Nullable private String zookeeperQuorum;
  @Nullable private String kafkaBrokers;
  @Nullable private Map<String,String> kafkaParams = null;

  @Override
  public String getAlias() {
    return CONFIG_LOADER_ALIAS;
  }

  @Override
  @Nullable public Config getConfig() {
    @NotNull final String kafkaParamString = kafkaParams == null ? "" : kafkaParams.entrySet().stream()
            .map(entry -> String.format("%s = %s", entry.getKey(), entry.getValue()))
            .collect(Collectors.joining("\n"));

    @NotNull final String envelopeConfig = String.format(PARSER_TEMPLATE,
            parserName,       // stage name is metron-parser-%s
            // input sub-stage
            parserInputTopic,
            parserName,        // kafka input group id is metron-parser-%s\n"+
            kafkaBrokers,
            kafkaParamString,
            // deriver sub-stage
            parserName,
            sparkRowEncodingStrategy,
            zookeeperQuorum,
            kafkaBrokers,
            kafkaParamString,
            // output sub-stage
            kafkaBrokers,
            kafkaParamString,
            parserOutputTopic
            );

    LOG.info(String.format("Writing out envelope configuration of %s",envelopeConfig ));

    @Nullable Config metronConfig = null;
    @Nullable Path tempFile = null;
    try {
      tempFile = Files.createTempFile("temp-envelope-config", ".tmp", tempfileAttributes);
      try (final OutputStream os = Files.newOutputStream(tempFile, StandardOpenOption.DELETE_ON_CLOSE)) {
        os.write(envelopeConfig.getBytes(Charsets.UTF_8));
        os.flush();
        metronConfig = ConfigFactory.parseFile(tempFile.toFile());
      }
    } catch (IOException e) {
      @NotNull final String errorPath = tempFile == null ? "Null Path" : tempFile.getFileName().toAbsolutePath().toString();
      LOG.error(String.format("Error writing out Metron Config to location %s",tempFile));
    }
    return metronConfig;
  }

  @Override
  public void configure(Config config) {
    adaptorType = config.getString(CONFIG_ADAPTOR_TYPE);
    sparkRowEncodingStrategy = config.getString(SPARK_ROW_ENCODING_STRATEGY);
    if (sparkRowEncodingStrategy == null) {
      // todo: change to actual default strategy
      sparkRowEncodingStrategy = "default";
    }
    zookeeperQuorum = Objects.requireNonNull(config.getString(ZOOKEEPER_QUORUM),
            String.format("%s is a required entry but not found", ZOOKEEPER_QUORUM));

    LOG.debug("Adaptor type detected as %s", adaptorType);
    if (CONFIG_ADAPTOR_TYPE_PARSER_ALIAS.equals(adaptorType)) {
      parserName = config.getString(CONFIG_PARSER_NAME);
      kafkaParams = config.atKey(KAFKA_SECTION_NAME)
              .entrySet()
              .stream()
              .collect(Collectors.toMap(Map.Entry::getKey, entry -> config.getString( entry.getKey())));
      // move kafka broker addresses to a separate variable if it exists
      // todo: check if broker is available in zookeeper?
      kafkaBrokers = kafkaParams.remove(KAFKA_BROKER);

      // Now get config info that is only available in the Metron config hosted in zookeeper
      try(final ParserConfigManager parserConfigManager = new ParserConfigManager(zookeeperQuorum)) {
        @Nullable final SensorParserConfig sensorParserConfiguration = parserConfigManager.getConfigurations().getSensorParserConfig(parserName);
        Objects.requireNonNull(sensorParserConfiguration,
                String.format("Sensor %s in not listed in the Metron zookeeper-based configuration", parserName));
        parserInputTopic = Objects.requireNonNull(sensorParserConfiguration.getSensorTopic(),
                "Parser input topic required but not found in Metron zookeeper configuration");
        parserOutputTopic = Objects.requireNonNull(sensorParserConfiguration.getOutputTopic(),
                "Parser output topic required but not found in Metron zookeeper configuration");
      }
    }

    validateConfiguation();
  }

  private void validateConfiguation() {
    Preconditions.checkArgument( supportedAdaptorTypes.contains(adaptorType),
            String.format("Unsupported adaptor type %s",adaptorType));
    // todo: validate row encoding value

    if (CONFIG_ADAPTOR_TYPE_PARSER_ALIAS.equals(adaptorType)) {
      validateParserConfiguration();
    }
  }

  private void validateParserConfiguration() {
  }
}
