package org.apache.metron.envelope.encoding;

import com.cloudera.labs.envelope.spark.RowWithSchema;
import com.cloudera.labs.envelope.utils.RowUtils;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import envelope.shaded.com.google.common.collect.ImmutableSet;
import org.apache.metron.common.error.MetronError;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.json.simple.JSONObject;

import java.io.Serializable;
import java.util.Set;

/**
 * Interface that encapsulates the way we encode metron data into spark SQL dataframes,
 * and how those dataframes are sent / received over Kafka
 */
public interface SparkRowEncodingStrategy extends Serializable {
  /**
   * Reserved fields used in all row encoding strategies
   */
  String ERROR_INDICATOR_FIELD = "_isErrorValue";
  Object ERROR_INDICATOR_FALSE = RowUtils.toRowValue(false, DataTypes.BooleanType);
  Object ERROR_INDICATOR_TRUE = RowUtils.toRowValue(true, DataTypes.BooleanType);

  String SCHEMA_VERSION_FIELD = "_schemaVersion";
  Object VERSION_ONE = RowUtils.toRowValue(1, DataTypes.ShortType);
  String DATA_FIELD = "_dataValue";

  StructField errorIndFieldSchema = DataTypes.createStructField(ERROR_INDICATOR_FIELD, DataTypes.BooleanType, false);
  StructField versionFieldSchema = DataTypes.createStructField(SCHEMA_VERSION_FIELD, DataTypes.ShortType, false);
  Set<String> reservedFieldNames = ImmutableSet.of(SCHEMA_VERSION_FIELD, DATA_FIELD, ERROR_INDICATOR_FIELD);

  /**
   * Encapsulates the choice of String or Text Field for the Composite Field containing non-standard fields values.
   */
  enum DataFieldType {
    FieldType_String(DataTypes.StringType),
    FieldType_Binary(DataTypes.BinaryType);

    DataFieldType(DataType dataType) {
      this.dataType = dataType;
    }
    protected DataType dataType;
    public DataType getSparkFieldType() {
      return dataType;
    }
    public Object mapValue(ObjectMapper mapper, Object value) throws JsonProcessingException {
      return (this == FieldType_Binary) ?
              mapper.writeValueAsBytes(value) :
              mapper.writeValueAsString(value);
    }
  }

  /**
   * Initialise any serialization libraries
   */
  void init(@Nullable String additionalConfig)

  /**
   * Spark schema of the data after it has been parsed by Metron
   *
   * @return spark schema struct
   */
  StructType getParserOutputSchema();

  /**
   * Encode a metron parse error into a Spark row
   *
   * @param metronError The error to encode
   * @return Spark row encoded to our output schema, null if serialisation error occurred
   */
  RowWithSchema encodeParserErrorIntoSparkRow(@NotNull MetronError metronError) throws JsonProcessingException;

  /**
   * Encode a metron parse result into a Spark row
   *
   * @param parsedMessage The parsed message
   * @return Spark row encoded to our output schema, null if serialisation error occurred
   */
  RowWithSchema encodeParserResultIntoSparkRow(@NotNull JSONObject parsedMessage) throws JsonProcessingException;

  /**
   * Decode a parsed message that has been read from Kafka
   * @param row Message to decode
   * @return JSONObject containing the message
   */
  JSONObject decodeParsedMessage(@NotNull Row row);

}
