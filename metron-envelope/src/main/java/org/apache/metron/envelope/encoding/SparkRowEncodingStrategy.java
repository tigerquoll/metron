package org.apache.metron.envelope.encoding;

import com.cloudera.labs.envelope.spark.RowWithSchema;
import org.apache.metron.common.error.MetronError;
import org.apache.spark.sql.types.StructType;
import org.json.simple.JSONObject;

public interface SparkRowEncodingStrategy {
  /**
   * Initialise any serialization libraries
   */
  void init();

  /**
   * Spark schema of the data after it has been parsed by Metron
   * @return
   */
  StructType getOutputSchema();

  /**
   * Encode a metron parse error into a Spark row
   * @param metronError The error to encode
   * @return Spark row encoded to our output schema, null if serialisation error occurred
   */
  RowWithSchema encodeErrorIntoSparkRow(MetronError metronError);

  /**
   * Encode a metron parse result into a Spark row
   * @param parsedMessage The parsed message
   * @return Spark row encoded to our output schema, null if serialisation error occurred
   */
  RowWithSchema encodeResultIntoSparkRow(JSONObject parsedMessage);
}
