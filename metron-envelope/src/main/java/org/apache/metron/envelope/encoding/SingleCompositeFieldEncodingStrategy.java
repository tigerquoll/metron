package org.apache.metron.envelope.encoding;

import com.cloudera.labs.envelope.spark.RowWithSchema;
import com.cloudera.labs.envelope.utils.RowUtils;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import envelope.shaded.com.google.common.collect.ImmutableList;
import org.apache.metron.common.error.MetronError;
import org.apache.metron.common.utils.LazyLogger;
import org.apache.metron.common.utils.LazyLoggerFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.json.simple.JSONObject;

/**
 * Encapsulate various logic on how Metron encodes values in Spark Dataframes
 * This strategy encodes all data fields into a Map<FieldName,FieldValue>
 * which is then encoded by Jackson (the serialisation format is parameterised)
 */
public class SingleCompositeFieldEncodingStrategy implements SparkRowEncodingStrategy {
  private static LazyLogger LOGGER = LazyLoggerFactory.getLogger(SingleCompositeFieldEncodingStrategy.class);
  private transient ObjectMapper mapper = null;
  private DataFieldType datafieldType;

  /**
   * Schema of processed data
   */
  private StructType outputSchema = null;

  @Override
  public void init(JsonFactory encodingFactory, DataFieldType datafieldType) {
    this.datafieldType = datafieldType;
    outputSchema = DataTypes.createStructType(new StructField[]{
            versionFieldSchema,
            errorIndFieldSchema,
            DataTypes.createStructField(DATAVALUE, datafieldType.getSparkFieldType(), false),
    });
    mapper = new ObjectMapper(encodingFactory);
  }

  @Override
  public StructType getParserOutputSchema() {
    return outputSchema;
  }

  private RowWithSchema encodeCompositeField(JSONObject data, Object errorIndicator) throws JsonProcessingException {
    // check for reserved name usage
    EncodingUtils.warnIfReservedFieldsAreUsed(data, LOGGER);
    final Object compositeFieldValue = EncodingUtils.encodeJsonFields(mapper,datafieldType, data);
    return new RowWithSchema(outputSchema, ImmutableList.of(VERSION_ONE, errorIndicator, compositeFieldValue));
  }

  @Override
  public RowWithSchema encodeParserErrorIntoSparkRow(MetronError metronError) throws JsonProcessingException {
    return encodeCompositeField(metronError.getJSONObject(), ERROR_INDICATOR_FALSE);
  }

  @Override
  public RowWithSchema encodeParserResultIntoSparkRow(JSONObject parsedMessage) throws JsonProcessingException {
    return encodeCompositeField(parsedMessage, ERROR_INDICATOR_TRUE);
  }
}
