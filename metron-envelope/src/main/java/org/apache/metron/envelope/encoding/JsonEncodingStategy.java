package org.apache.metron.envelope.encoding;

import com.cloudera.labs.envelope.spark.RowWithSchema;
import com.cloudera.labs.envelope.utils.RowUtils;
import envelope.shaded.com.google.common.collect.ImmutableList;
import org.apache.metron.common.error.MetronError;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.json.simple.JSONObject;

public class JsonEncodingStategy implements SparkRowEncodingStrategy {

  /**
   * Schema of processed data
   */
  private static StructType outputSchema = DataTypes.createStructType(new StructField[]{
          // Initially just 0x1 - used to enable backward compatibility while we are not using a schema registry
          DataTypes.createStructField("json", DataTypes.StringType, false),
  });

  @Override
  public void init() {

  }

  @Override
  public StructType getOutputSchema() {
    return outputSchema;
  }

  private RowWithSchema encodeJsonString(String jsonData) {
    Object rowValue = RowUtils.toRowValue(jsonData, DataTypes.StringType);
    return new RowWithSchema(outputSchema, ImmutableList.of(rowValue));
  }

  @Override
  public RowWithSchema encodeErrorIntoSparkRow(MetronError metronError) {
    return encodeJsonString(metronError.getJSONObject().toJSONString());
  }

  @Override
  public RowWithSchema encodeResultIntoSparkRow(JSONObject parsedMessage) {
    return encodeJsonString(parsedMessage.toJSONString());
  }
}
