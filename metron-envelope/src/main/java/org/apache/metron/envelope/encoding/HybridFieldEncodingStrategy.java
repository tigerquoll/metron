package org.apache.metron.envelope.encoding;

import com.cloudera.labs.envelope.spark.RowWithSchema;
import com.cloudera.labs.envelope.utils.RowUtils;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Sets;
import envelope.shaded.com.google.common.collect.ImmutableList;
import org.apache.metron.common.error.MetronError;
import org.apache.metron.common.utils.LazyLogger;
import org.apache.metron.common.utils.LazyLoggerFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.json.simple.JSONObject;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.metron.envelope.encoding.EncodingUtils.encodeJsonFields;

/**
 * Encapsulate various logic on how Metron encodes values in Spark Dataframes
 * This strategy encodes well known fields as separate spark dataframe fields
 * Non-standard fields are all encoded as a Map<FieldName,FieldValue) which is then encoded by Jackson (serialisation is parameterised)
 */
public class HybridFieldEncodingStrategy implements SparkRowEncodingStrategy {
  private static LazyLogger LOGGER = LazyLoggerFactory.getLogger(HybridFieldEncodingStrategy.class);
  private DataFieldType datafieldType;

  /**
   * Schema of processed data
   */
  private static List<StructField> parserOutputSchemaTemplate = new ImmutableList.Builder<StructField>()
          .add(versionFieldSchema)
          .add(errorIndFieldSchema)
          .add(DataTypes.createStructField("ip_src_addr", DataTypes.StringType, true))
          .add(DataTypes.createStructField("ip_dst_addr",  DataTypes.StringType, true))
          .add(DataTypes.createStructField("ip_src_port",  DataTypes.IntegerType, true))
          .add(DataTypes.createStructField("ip_dst_port",  DataTypes.IntegerType, true))
          .add(DataTypes.createStructField("ip_dst_port",  DataTypes.IntegerType, true))
          .add(DataTypes.createStructField("protocol",  DataTypes.StringType, true))
          .add(DataTypes.createStructField("timestamp",  DataTypes.LongType, true))
          .add(DataTypes.createStructField("original_string",  DataTypes.StringType, true))
          // a dataval field type will be added here during initialisation
          .build();

  /**
   * Index of field name to field type
   */
  private Map<String, StructField> schemaIndex;

  private static StructType parserOutputSchema;
  private transient ObjectMapper mapper = null;

  @Override
  public StructType getParserOutputSchema() {
    return parserOutputSchema;
  }

  @Override
  public void init(JsonFactory encodingFactory, DataFieldType datafieldType) {
    this.datafieldType = datafieldType;
    // init serialisation
    mapper = new ObjectMapper(encodingFactory);

    // Create the output schema based on the passed in parameters
    final StructField dataFieldSchema = DataTypes.createStructField(DATAVALUE,
            datafieldType.getSparkFieldType(),
            false);

    final List<StructField> parserOutputFields = new ImmutableList.Builder<StructField>()
            .addAll(parserOutputSchemaTemplate)
            .add(dataFieldSchema)
            .build();
    parserOutputSchema = DataTypes.createStructType(parserOutputFields);

    // Generate an index of standard, non reserved schema fields for ease of processing
    schemaIndex = parserOutputFields.stream()
            .filter( x -> !reservedFieldNames.contains(x.name()))
            .collect(Collectors.toMap(StructField::name, Function.identity()));
  }

  /**
   * Encode a metron parse error into a Spark row
   * For errors, no standard fields are used, all fields are non-standard
   * @param metronError The error to encode
   * @return Spark row encoded to our output schema, null if serialisation error occurred
   */
  @Override
  public RowWithSchema encodeParserErrorIntoSparkRow(MetronError metronError) throws JsonProcessingException {
    final List<Object> encodedRowValues = new ArrayList<>();
    encodedRowValues.add(VERSION_ONE);
    encodedRowValues.add(ERROR_INDICATOR_FALSE);
    for(Map.Entry<String, StructField> schemaEntry: schemaIndex.entrySet()) {
      final StructField fieldSchema = schemaEntry.getValue();
      // Standard fields are always added to a datasest row, even if they are missing (i.e. null)
      encodedRowValues.add(RowUtils.toRowValue(null, fieldSchema.dataType()));
    }

    // All errors values are encoded into one composite field
    encodedRowValues.add( datafieldType.mapValue(mapper, metronError.getJSONObject()));

    return new RowWithSchema(parserOutputSchema, encodedRowValues);
  }

  /**
   * Encode a metron parse result into a Spark row
   *
   * @param parsedMessage The parsed message
   * @return Spark row encoded to our output schema, null if serialisation error occurred
   */
  @Override
  public RowWithSchema encodeParserResultIntoSparkRow(JSONObject parsedMessage) throws JsonProcessingException {
    // check for reserved name usage
    EncodingUtils.warnIfReservedFieldsAreUsed(parsedMessage, LOGGER);

    final List<Object> encodedRowValues = new ArrayList<>();
    encodedRowValues.add(VERSION_ONE);
    encodedRowValues.add(ERROR_INDICATOR_FALSE);

    // Add standard field values (add null if they are not present)
    for(Map.Entry<String, StructField> schemaEntry: schemaIndex.entrySet()) {
      final String fieldName = schemaEntry.getKey();
      final StructField fieldSchema = schemaEntry.getValue();
      // Standard fields are always added to a datasest row, even if they are missing (i.e. null)
      final Object rowVal = RowUtils.toRowValue(parsedMessage.get(fieldName), fieldSchema.dataType());
      encodedRowValues.add(rowVal);
      parsedMessage.remove(fieldName);
    }

    // Any remaining fields are non-standard - encode them into a composite field
    encodedRowValues.add(encodeJsonFields(mapper, datafieldType, parsedMessage));

    return new RowWithSchema(parserOutputSchema, encodedRowValues);
  }




}
