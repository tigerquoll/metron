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

package org.apache.metron.envelope.encoding;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import envelope.shaded.com.google.common.collect.ImmutableList;
import kafka.Kafka;
import org.apache.metron.common.utils.LazyLogger;
import org.apache.metron.common.utils.LazyLoggerFactory;
import org.apache.metron.envelope.utils.SparkKafkaUtils;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.jetbrains.annotations.Nullable;

import java.util.List;
import java.util.Objects;

/**
 * Spark Encoding Strategy
 * Core schema fields broken out into separate spark fields
 * All other fields Json encoded into a single text field
 * Avro encoded with a static schema when passed over Kafka
 */
public abstract class AbstractSparkRowEncodingStrategy implements SparkRowEncodingStrategy {
  private static LazyLogger LOGGER = LazyLoggerFactory.getLogger(AbstractSparkRowEncodingStrategy.class);

  /**
   * What type of serialisation do we use for Kafka
   * @return either avro or text
   */
  protected enum KafkaSerializationType {
    avro,
    text
  }

  /**
   * Schema template for Parser Output in Spark SQL Schema
   */
  protected abstract List<StructField> getParserOutputSparkSchemaBaseTemplate();

  /**
   * The actual Spark SQL Schema for Parser Output
   */
  protected StructType parserOutputSparkSchema = null;

  /**
   * Used to encode multiple fields into a single field
   */
  private transient ObjectMapper mapper = null;

  protected ObjectMapper getMapper() {
    return mapper;
  }

  protected String parserOutputKafkaSchema = null;

  protected String avroSchemaName;

  @Override
  public String getParserOutputKafkaAvroSchema() {
    if (getKafkaSerializationType() != KafkaSerializationType.avro) {
      throw new UnsupportedOperationException("Cannot get Avro Schema of a Kafka text based encoding Strategy");
    } else {
      return parserOutputKafkaSchema;
    }
  }

  /**
   * What name to give to the generated Avro schema
   * @return name to give the generated avro schema
   */
  protected abstract String getAvroSchemaName();

  private JsonFactory jsonFactory;
  private DataFieldType dataFieldType;
  private KafkaSerializationType kafkaSerializationType;
  private String additionalConfig;

  protected void init(JsonFactory jsonFactory, DataFieldType dataFieldType, KafkaSerializationType kafkaSerializationType, @Nullable String additionalConfig) {
    this.jsonFactory = Objects.requireNonNull(jsonFactory);
    this.dataFieldType = Objects.requireNonNull(dataFieldType);
    this.kafkaSerializationType = Objects.requireNonNull(kafkaSerializationType);

    // construct our actual spark schema by combining the template plus our parameterised data field
    parserOutputSparkSchema = DataTypes.createStructType(new ImmutableList.Builder<StructField>()
            .addAll(getParserOutputSparkSchemaBaseTemplate())
            .add(DataTypes.createStructField(DATA_FIELD, getDataFieldType().getSparkFieldType(), false))
            .build()
            .toArray( new StructField[0] ));

    if (getKafkaSerializationType() == KafkaSerializationType.avro) {
      parserOutputKafkaSchema = SparkKafkaUtils.generateAvroSchema(parserOutputSparkSchema.fields(), getAvroSchemaName()).toString(true);
    }
    mapper = new ObjectMapper(getJsonFactory());

    Objects.requireNonNull(getParserSparkOutputSchema());
    Objects.requireNonNull(getMapper());
  }

  protected String getAdditionalConfig() {
    return additionalConfig;
  }

  public KafkaSerializationType getKafkaSerializationType() {
    return kafkaSerializationType;
  }

  @Override
  public StructType getParserSparkOutputSchema() {
    return parserOutputSparkSchema;
  }

  public JsonFactory getJsonFactory() {
    return jsonFactory;
  }

  public DataFieldType getDataFieldType() {
    return dataFieldType;
  }
}
