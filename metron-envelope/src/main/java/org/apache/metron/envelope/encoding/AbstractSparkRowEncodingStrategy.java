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
import com.cloudera.labs.envelope.utils.RowUtils;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import envelope.shaded.com.google.common.collect.ImmutableList;
import envelope.shaded.com.google.common.base.Preconditions;
import org.apache.metron.common.Constants;
import org.apache.metron.common.error.MetronError;
import org.apache.metron.common.utils.LazyLogger;
import org.apache.metron.common.utils.LazyLoggerFactory;
import org.apache.metron.envelope.utils.SparkKafkaUtils;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.json.simple.JSONObject;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.metron.envelope.encoding.EncodingUtils.DATA_FIELD_NAME;
import static org.apache.metron.envelope.encoding.EncodingUtils.ERROR_INDICATOR_FALSE;
import static org.apache.metron.envelope.encoding.EncodingUtils.ERROR_INDICATOR_TRUE;
import static org.apache.metron.envelope.encoding.EncodingUtils.VERSION_ONE;
import static org.apache.metron.envelope.encoding.EncodingUtils.errorIndFieldSchema;
import static org.apache.metron.envelope.encoding.EncodingUtils.reservedFieldNames;
import static org.apache.metron.envelope.encoding.EncodingUtils.versionFieldSchema;

/**
 * Spark Encoding Strategy
 * Different Options are
 * 1. CorePlus or Combined - break out well known fields into separate fields, or keep all fields together
 * 2. Json or Avro - Do we encode "all together" field values in Json (text) or Cbor (binary)
 * 3. Kafka encoding - Do we encode the et text or binary (related to #2).
 * Only certain combinations make sense, so these options have classes that contain the choices
 * No end-user should be using AbstractSparkRowEncodingStrategy
 */
public abstract class AbstractSparkRowEncodingStrategy implements SparkRowEncodingStrategy {
  private static final LazyLogger LOGGER = LazyLoggerFactory.getLogger(AbstractSparkRowEncodingStrategy.class);

  /**
   * What type of serialisation do we use for Kafka - avro or text
   * We are limited here by what envelope supports
   */
  protected enum KafkaSerializationType {
    avro,
    text
  }

  /**
   * Two ways we can deal with field representation in spark
   * corePlus: split out well known files into separate fields, plus a combined field for others
   * combined:  all fields are combined into a single field
   */
  protected enum FieldLayoutType {
    corePlus,
    combined
  }

  /**
   * Encapsulates the choice of String or Text Field for the Composite Field containing non-standard fields values.
   */
  enum DataFieldType {
    FieldType_String(DataTypes.StringType),
    FieldType_Binary(DataTypes.BinaryType);

    DataFieldType(DataType dataType) {
      this.dataType = dataType;
    }

    final DataType dataType;
    DataType getSparkFieldType() {
      return dataType;
    }
    public Object mapValue(ObjectMapper mapper, Object value) throws JsonProcessingException {
      return (this == FieldType_Binary) ?
              mapper.writeValueAsBytes(value) :
              mapper.writeValueAsString(value);
    }
  }

  /**
   * The actual Spark SQL Schema for Parser Output
   */
  private StructType parserOutputSparkSchema = null;

  /**
   * Used to encode multiple fields into a single field
   */
  private transient ObjectMapper mapper = null;

  /**
   * If avro used for Kafka, the avro schema to use
   */
  private String parserOutputKafkaSchema = null;

  private DataFieldType dataFieldType;
  private KafkaSerializationType kafkaSerializationType;
  private FieldLayoutType fieldLayoutType;
  private Map<String, StructField> schemaIndex;

  @Override
  public StructType getParserOutputSparkSchema() {
    return parserOutputSparkSchema;
  }

  @Override
  public String getParserOutputKafkaAvroSchema() {
    Preconditions.checkState(kafkaSerializationType == KafkaSerializationType.avro,
            "Cannot get Avro Schema of a Kafka text based encoding Strategy");
    return parserOutputKafkaSchema;
  }

  /**
   * "Core plus" Type schemas break out known fields into separate spark sql fields
   */
  private List<StructField> getParserOutputSparkSchemaBaseTemplate() {
    List<StructField> list;
    switch (fieldLayoutType) {
      case corePlus:
        // Core plus has a number of core fields that are broken out
        list = new ImmutableList.Builder<StructField>()
                .add(versionFieldSchema)
                .add(errorIndFieldSchema)
                .add(DataTypes.createStructField(Constants.Fields.GUID.getName(), DataTypes.StringType, true)) // Also kafka key
                .add(DataTypes.createStructField(Constants.Fields.SENSOR_TYPE.getName(), DataTypes.StringType, true))
                .add(DataTypes.createStructField(Constants.Fields.SRC_ADDR.getName(), DataTypes.StringType, true))
                .add(DataTypes.createStructField(Constants.Fields.DST_ADDR.getName(), DataTypes.StringType, true))
                .add(DataTypes.createStructField(Constants.Fields.SRC_PORT.getName(), DataTypes.IntegerType, true))
                .add(DataTypes.createStructField(Constants.Fields.DST_PORT.getName(), DataTypes.IntegerType, true))
                .add(DataTypes.createStructField(Constants.Fields.PROTOCOL.getName(), DataTypes.StringType, true))
                .add(DataTypes.createStructField(Constants.Fields.TIMESTAMP.getName(), DataTypes.LongType, true))
                .add(DataTypes.createStructField(Constants.Fields.ORIGINAL.getName(), DataTypes.StringType, true))
                .add(DataTypes.createStructField(Constants.Fields.INCLUDES_REVERSE_TRAFFIC.getName(), DataTypes.BooleanType, true)) // Used in pcap encoders
                // a dataval field type will be added here during initialisation
                .build();
        break;
      case combined:
        list = Collections.emptyList();
        // combined field layout has a single data field, which will be added here during initialisation
        break;
      default:
        throw new UnsupportedOperationException(
                String.format("Do not know how to encode fieldLayout type %s", fieldLayoutType.toString()));
    }
    return list;
  }

  void init(JsonFactory jsonFactory, DataFieldType dataFieldType,
            KafkaSerializationType kafkaSerializationType,
            FieldLayoutType fieldLayoutType,
            @Nullable String avroSchemaName) {
    this.dataFieldType = Objects.requireNonNull(dataFieldType);
    this.kafkaSerializationType = Objects.requireNonNull(kafkaSerializationType);
    this.fieldLayoutType = Objects.requireNonNull(fieldLayoutType);
    mapper = new ObjectMapper( Objects.requireNonNull(jsonFactory) );

    // construct our actual spark schema by combining the template plus our parameterised data field
    parserOutputSparkSchema = DataTypes.createStructType(new ImmutableList.Builder<StructField>()
            .addAll(getParserOutputSparkSchemaBaseTemplate())
            .add(DataTypes.createStructField(DATA_FIELD_NAME, dataFieldType.getSparkFieldType(), false))
            .build()
            .toArray( new StructField[0] ));

    if (kafkaSerializationType == KafkaSerializationType.avro) {
      // Generate our avro schema for use with Kafka
      parserOutputKafkaSchema = SparkKafkaUtils.generateAvroSchema(parserOutputSparkSchema.fields(), avroSchemaName).toString(true);
    }

    if ((fieldLayoutType == FieldLayoutType.corePlus) &&
            (kafkaSerializationType != KafkaSerializationType.avro)) {
      throw new IllegalStateException("CorePlus Strategies must always be Avro encoded");
    }

    // Generate an index of standard, non-reserved schema fields for ease of processing
    switch (fieldLayoutType) {
      case combined:
        // combined has no other fields other then the data field
        schemaIndex = Collections.emptyMap();
        break;
      case corePlus:
        // core pluss has a number of fields separate from the combined field
        schemaIndex = Arrays.stream(parserOutputSparkSchema.fields())
                .filter(x -> !reservedFieldNames.contains(x.name()))
                .collect(Collectors.toMap(StructField::name, Function.identity()));
        break;
      default:
        throw new UnsupportedOperationException(
                String.format("Do not know how to encode fieldLayout type %s", fieldLayoutType.toString()));
    }

    Objects.requireNonNull(parserOutputSparkSchema);
    Objects.requireNonNull(mapper);
  }

  @Override
  public RowWithSchema encodeParserErrorIntoSparkRow(@NotNull MetronError metronError) throws JsonProcessingException {
    RowWithSchema rowWithSchema;
    switch(fieldLayoutType) {
      case combined:
        // combined field layout has no core fields
        final List<Object> noCoreFields = new ArrayList<>();
        rowWithSchema = encodeParserCombined(metronError.getJSONObject(), noCoreFields);
        break;
      case corePlus:
        // check for reserved name usage
        final JSONObject jsonObject = metronError.getJSONObject();
        EncodingUtils.warnIfReservedFieldsAreUsed(jsonObject);
        rowWithSchema = encodeParserCorePlus(jsonObject, ERROR_INDICATOR_TRUE);
        break;
      default:
        throw new UnsupportedOperationException(
                String.format("Do not know how to encode fieldLayout type %s", fieldLayoutType.toString()));
    }
    return rowWithSchema;
  }

  @Override
  public RowWithSchema encodeParserResultIntoSparkRow(@Nullable JSONObject parsedMessage) throws JsonProcessingException {
    RowWithSchema rowWithSchema = null;
    if (parsedMessage != null) {
      switch (fieldLayoutType) {
        case combined:
          // combined field layout has no core fields
          final List<Object> noCoreFields = new ArrayList<>();
          rowWithSchema = encodeParserCombined(parsedMessage, noCoreFields);
          break;
        case corePlus:
          // check for reserved name usage
          EncodingUtils.warnIfReservedFieldsAreUsed(parsedMessage);
          rowWithSchema = encodeParserCorePlus(parsedMessage, ERROR_INDICATOR_FALSE);
          break;
        default:
          throw new UnsupportedOperationException(
                  String.format("Do not know how to encode fieldLayout type %s", fieldLayoutType.toString()));
      }
    }
    return rowWithSchema;
  }

  private RowWithSchema encodeParserCorePlus(@NotNull JSONObject parsedMessage, Object errorInd) throws JsonProcessingException {
    final List<Object> encodedRowValues = new ArrayList<>();
    encodedRowValues.add(VERSION_ONE);
    encodedRowValues.add(errorInd);
    // Add standard field values (add null if they are not present)
    for(Map.Entry<String, StructField> schemaEntry: schemaIndex.entrySet()) {
      final String fieldName = schemaEntry.getKey();
      final StructField fieldSchema = schemaEntry.getValue();
      // Standard fields are always added to a datasest row, even if they are missing (i.e. null)
      final Object rowVal = RowUtils.toRowValue(parsedMessage.get(fieldName), fieldSchema.dataType());
      encodedRowValues.add(rowVal);
      parsedMessage.remove(fieldName);
    }

    // Now that we have encoded the core values, combine encode the rest of the fields in the message
    return encodeParserCombined(parsedMessage, encodedRowValues);
  }

  @NotNull
  private RowWithSchema encodeParserCombined(@NotNull JSONObject parsedMessage,
                                             @NotNull List<Object> existingEncodedValues
  ) throws JsonProcessingException {
    // Any remaining fields are non-standard - encode them into a composite field
    existingEncodedValues.add(EncodingUtils.encodeCombinedFields(mapper, dataFieldType, parsedMessage));

    Preconditions.checkState(parsedMessage.size() == existingEncodedValues.size(),
            "existingEncodedValues.size (%d), does not match parserOutputSparkSchema.size (%d)",
            existingEncodedValues.size(), parsedMessage.size() );
    return new RowWithSchema(parserOutputSparkSchema, existingEncodedValues);
  }

  @Override
  public JSONObject decodeParsedMessage(@NotNull Row row) {
    return null;
  }
}
