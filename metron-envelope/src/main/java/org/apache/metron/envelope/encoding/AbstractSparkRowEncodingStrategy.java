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
import com.cloudera.labs.envelope.utils.AvroUtils;
import com.cloudera.labs.envelope.utils.RowUtils;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import envelope.shaded.com.google.common.collect.ImmutableList;
import envelope.shaded.com.google.common.base.Preconditions;
import com.cloudera.labs.envelope.shaded.org.apache.avro.Schema;
import org.apache.metron.common.Constants;
import org.apache.metron.common.error.MetronError;
import org.apache.metron.common.utils.LazyLogger;
import org.apache.metron.common.utils.LazyLoggerFactory;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.json.simple.JSONObject;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
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

  private StructType errorSchema = DataTypes.createStructType(
          Collections.singletonList(
                  DataTypes.createStructField("error", DataTypes.StringType, true)
          )
  );

  /**
   * The actual Spark SQL Schema for Parser Output
   */
  private StructType outputSparkSchema = null;

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
  public StructType getOutputSparkSchema() {
    return outputSparkSchema;
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

  /**
   * Generates a fieldname -> fieldDefintion index for ease of later processing
   * @param fieldLayoutType Which field type layout we are using
   * @param parserOutputSparkSchema The fields to index
   * @return Index of well known fields that we are using with this encoder, plus their schema definitions
   */
  static Map<String, StructField> generateFieldIndex(AbstractSparkRowEncodingStrategy.FieldLayoutType fieldLayoutType, StructType parserOutputSparkSchema) {
    Map<String, StructField> index = null;
    // Generate an index of standard, non-reserved schema fields for ease of processing
    switch (fieldLayoutType) {
      case combined:
        // combined has no other fields other then the data field
        index = Collections.emptyMap();
        break;
      case corePlus:
        // core plus has a number of fields separate from the combined field
        index = Arrays.stream(parserOutputSparkSchema.fields())
                .filter(x -> !reservedFieldNames.contains(x.name()))
                .collect(Collectors.toMap(StructField::name, Function.identity()));
        break;
      default:
        throw new UnsupportedOperationException(
                String.format("Do not know how to encode fieldLayout type %s", fieldLayoutType.toString()));
    }
    return index;
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
    outputSparkSchema = DataTypes.createStructType(new ImmutableList.Builder<StructField>()
            .addAll(getParserOutputSparkSchemaBaseTemplate())
            .add(DataTypes.createStructField(DATA_FIELD_NAME, dataFieldType.getSparkFieldType(), false))
            .build()
            .toArray( new StructField[0] ));

    if (kafkaSerializationType == KafkaSerializationType.avro) {
      // Generate our avro schema for use with Kafka, escape quotes so we can embed it cleanly into a envelope config file
      parserOutputKafkaSchema = AvroUtils.schemaFor(outputSparkSchema).toString(true).replaceAll("\"", "\\\"");
    }

    if ((fieldLayoutType == FieldLayoutType.corePlus) &&
            (kafkaSerializationType != KafkaSerializationType.avro)) {
      throw new IllegalStateException("CorePlus Strategies must always be Avro encoded");
    }

    schemaIndex = generateFieldIndex(fieldLayoutType, outputSparkSchema);

    Objects.requireNonNull(outputSparkSchema);
    Objects.requireNonNull(mapper);
  }

  @Override
  public RowWithSchema encodeParserErrorIntoSparkRow(@NotNull MetronError metronError) throws JsonProcessingException {
    return(encodeErrorIntoSparkRow(metronError));
  }

  @Override
  public RowWithSchema encodeParserResultIntoSparkRow(@Nullable JSONObject parsedMessage) throws JsonProcessingException {
    return encodeResultIntoSparkRow(parsedMessage);
  }

  @Override
  public RowWithSchema encodeEnrichedMessageIntoSparkRow(@Nullable JSONObject parsedMessage) throws JsonProcessingException {
    return encodeResultIntoSparkRow(parsedMessage);
  }

  @Override
  public RowWithSchema encodeEnrichErrorIntoSparkRow(@NotNull MetronError metronError) {
    return encodeErrorIntoSparkRow(metronError);
  }

  /**
   * Errors are all encoded as a single text field though that is
   * currently shielded from the user as an implementation detail
   * @param metronError Error to encode
   * @return Error encoded into a spark row
   */
  protected RowWithSchema encodeErrorIntoSparkRow(@NotNull MetronError metronError) {
    String error = metronError.getJSONObject().toJSONString();
    Object value = RowUtils.toRowValue(error, DataTypes.StringType);
    return new RowWithSchema(errorSchema, value);
  }

  /**
   * Encodes the provided JSONObject into a spark row
   * @param metronMessage Message to encode
   * @return Spark Row
   * @throws JsonProcessingException if an encoding error ocurrs
   */
  protected RowWithSchema encodeResultIntoSparkRow(@Nullable JSONObject metronMessage) throws JsonProcessingException {
    RowWithSchema rowWithSchema = null;
    if (metronMessage != null) {
      switch (fieldLayoutType) {
        case combined:
          // combined field layout has no core fields
          final List<Object> noCoreFields = new ArrayList<>();
          rowWithSchema = encodeCombined(metronMessage, noCoreFields);
          break;
        case corePlus:
          // check for reserved name usage
          EncodingUtils.warnIfReservedFieldsAreUsed(metronMessage);
          rowWithSchema = encodeCorePlus(metronMessage);
          break;
        default:
          throw new UnsupportedOperationException(
                  String.format("Do not know how to encode fieldLayout type %s", fieldLayoutType.toString()));
      }
    }
    return rowWithSchema;
  }

  private RowWithSchema encodeCorePlus(@NotNull JSONObject metronMessage) throws JsonProcessingException {
    final List<Object> encodedRowValues = new ArrayList<>();
    encodedRowValues.add(VERSION_ONE);
    // Add standard field values (add null if they are not present)
    for(Map.Entry<String, StructField> schemaEntry: schemaIndex.entrySet()) {
      final String fieldName = schemaEntry.getKey();
      final StructField fieldSchema = schemaEntry.getValue();
      // Standard fields are always added to a datasest row, even if they are missing (i.e. null)
      final Object rowVal = RowUtils.toRowValue(metronMessage.get(fieldName), fieldSchema.dataType());
      encodedRowValues.add(rowVal);
      metronMessage.remove(fieldName);
    }

    // Now that we have encoded the core values, combine encode the rest of the fields in the message
    return encodeCombined(metronMessage, encodedRowValues);
  }

  @NotNull
  private RowWithSchema encodeCombined(@NotNull JSONObject metronMessage,
                                       @NotNull List<Object> existingEncodedValues
  ) throws JsonProcessingException {
    // Any remaining fields are non-standard - encode them into a composite field
    existingEncodedValues.add(EncodingUtils.encodeCombinedFields(mapper, dataFieldType, metronMessage));

    Preconditions.checkState(metronMessage.size() == existingEncodedValues.size(),
            "existingEncodedValues.size (%d), does not match parserOutputSparkSchema.size (%d)",
            existingEncodedValues.size(), metronMessage.size() );
    return new RowWithSchema(outputSparkSchema, existingEncodedValues);
  }

  @Override
  public JSONObject decodeParsedMessageFromKafka(@NotNull Row row) {

    //@NotNull final Map<String,Object> metadata = SparkKafkaUtils.extractKafkaMetadata(row);
    //@NotNull final Map<String,Object> message = SparkKafkaUtils.extractKafkaMessage(row);

    return null;
  }

  private static final String KafkaBinaryInConfiguration =
          "    input {\n" +
          "      type = kafka\n" +
          "      brokers = \"%s\"\n" +
          "      topic = %s\n" +
         // "      encoding = bytearray\n" + no longer needed, pulled from expected schema if its binary or string
          "      translator {\n" +
          "        type = avro\n" +
          "        schema {\n" +
          "          type = avro\n" +
          "          literal = \"%s\"n" +
          "        }\n" +
          "      }\n" +
          "    }";

  private static final String KafkaTextInConfiguration =
          "  input {\n" +
          "    type = kafka\n" +
          "    brokers = \"%s\"\n" +
          "    topic = %s\n" +
          "    translator {\n" +
          "      type = raw\n" +
          "      delimiter = \",\"\n" +
          "      schema {\n" +
          "        type = flat\n" +
          "        field.names = [value]\n" +
          "        field.types = [string]\n" +
          "      }\n" +
          "    }\n" +
          "  }";

  private static final String KafkaTextOutConfiguration =
          "      serializer {\n" +
          "        type = delimited\n" +
          "        field.delimiter = \",\"\n" +
          "      } ";

  private static final String KafkaAvroOutConfiguration =
          "      serializer {\n" +
          "        type = avro\n" +
          "        schema.path = %s" +
          "      } ";

  private static final String kafkaAvroSerialisationConf =
          "";


  public String getEnvelopeKafkaOuputSerialisationSection() throws IOException {
    String serializerSection = "";

    switch (kafkaSerializationType) {
      case text:
        serializerSection = KafkaTextOutConfiguration;
        break;
      case avro:
        // write schema out to text file, get path
        // inject path into serializer section
        Path schemaFilePath = writeOutAvroSchemaFile(AvroUtils.schemaFor(outputSparkSchema));
        serializerSection = String.format(KafkaAvroOutConfiguration, schemaFilePath.toAbsolutePath().toString());
        break;
      default:
    }
    return serializerSection;
  }

  String getEnvelopeKafkaInputTranslatorSection() {
    switch (kafkaSerializationType) {
      case text:
        break;
      case avro:
        break;
      default:
    }
    return null;
  }

  /**
   * Writes out an Avro schema to a temporary file and returns the path
   * @param avroSchema
   * @return Path to temp file containing avro schema file
   */
  static Path writeOutAvroSchemaFile(final Schema avroSchema) throws IOException {
    Objects.requireNonNull(avroSchema);
    final Path tempfile = Files.createTempFile(null,".avsc");
    Files.write(tempfile, avroSchema.toString().getBytes(StandardCharsets.UTF_8));
    return tempfile;
  }

}
