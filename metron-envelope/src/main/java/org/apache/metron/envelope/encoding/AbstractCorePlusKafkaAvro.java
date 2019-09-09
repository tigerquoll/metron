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

import com.cloudera.labs.envelope.spark.RowWithSchema;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonProcessingException;
import envelope.shaded.com.google.common.collect.ImmutableList;
import kafka.Kafka;
import org.apache.metron.common.Constants;
import org.apache.metron.common.error.MetronError;
import org.apache.metron.common.utils.LazyLogger;
import org.apache.metron.common.utils.LazyLoggerFactory;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.json.simple.JSONObject;


import java.util.List;
import java.util.Objects;

/**
 * Spark Encoding Strategy
 * Core schema fields broken out into separate spark fields
 * All other fields Json encoded into a single text field
 * Avro encoded with a static schema when passed over Kafka
 */
public abstract class AbstractCorePlusKafkaAvro extends AbstractSparkRowEncodingStrategy implements SparkRowEncodingStrategy  {
  private static LazyLogger LOGGER = LazyLoggerFactory.getLogger(AbstractCorePlusKafkaAvro.class);

  @Override
  public void init(JsonFactory jsonFactory, DataFieldType dataFieldType, KafkaSerializationType kafkaSerializationType, String additionalConfig) {
    if (kafkaSerializationType != KafkaSerializationType.avro) {
      throw new IllegalStateException("CorePlus Strategies must always be Avro encoded");
    }
    super.init(jsonFactory, dataFieldType,kafkaSerializationType, additionalConfig);
  }

  /**
   * "Core plus" Type schemas break out known fields into separate spark sql fields
   */
  protected List<StructField> getParserOutputSparkSchemaBaseTemplate() {
    return new ImmutableList.Builder<StructField>()
            .add(versionFieldSchema)
            .add(errorIndFieldSchema)
            .add(DataTypes.createStructField(Constants.Fields.GUID.getName(), DataTypes.StringType, true)) // Also kafka key
            .add(DataTypes.createStructField(Constants.Fields.SENSOR_TYPE.getName(), DataTypes.StringType, true))
            .add(DataTypes.createStructField(Constants.Fields.SRC_ADDR.getName(), DataTypes.StringType, true))
            .add(DataTypes.createStructField(Constants.Fields.DST_ADDR.getName(),  DataTypes.StringType, true))
            .add(DataTypes.createStructField(Constants.Fields.SRC_PORT.getName(),  DataTypes.IntegerType, true))
            .add(DataTypes.createStructField(Constants.Fields.DST_PORT.getName(), DataTypes.IntegerType, true))
            .add(DataTypes.createStructField(Constants.Fields.PROTOCOL.getName(),  DataTypes.StringType, true))
            .add(DataTypes.createStructField(Constants.Fields.TIMESTAMP.getName(),  DataTypes.LongType, true))
            .add(DataTypes.createStructField(Constants.Fields.ORIGINAL.getName(),  DataTypes.StringType, true))
            .add(DataTypes.createStructField(Constants.Fields.INCLUDES_REVERSE_TRAFFIC.getName(), DataTypes.BooleanType, true)) // Used in pcap encoders
            // a dataval field type will be added here during initialisation
            .build();
  }

  private RowWithSchema encodeCompositeField(JSONObject data, Object errorIndicator) throws JsonProcessingException {
    // check for reserved name usage
    EncodingUtils.warnIfReservedFieldsAreUsed(data, LOGGER);
    final Object compositeFieldValue = Objects.requireNonNull(EncodingUtils.encodeCombinedFields(getMapper(), getDataFieldType(), data));
    return new RowWithSchema(getParserSparkOutputSchema(), ImmutableList.of(VERSION_ONE, errorIndicator, compositeFieldValue));
  }

  @Override
  public RowWithSchema encodeParserErrorIntoSparkRow(MetronError metronError) throws JsonProcessingException {
    return encodeCompositeField(metronError.getJSONObject(), ERROR_INDICATOR_FALSE);
  }

  @Override
  public RowWithSchema encodeParserResultIntoSparkRow(JSONObject parsedMessage) throws JsonProcessingException {
    return encodeCompositeField(parsedMessage, ERROR_INDICATOR_TRUE);
  }

  @Override
  public JSONObject decodeParsedMessage(@NotNull Row row) {
    return null;
  }
}
