package org.apache.metron.envelope.encoding;

import com.cloudera.labs.envelope.spark.RowWithSchema;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.metron.common.error.MetronError;
import org.apache.spark.sql.types.StructType;
import org.json.simple.JSONObject;

import java.io.Serializable;

/**
 * Interface that encapsulates the way we encode metron data into spark SQL
 */
public interface SparkRowEncodingStrategy extends Serializable {
  /**
   * Initialise any serialization libraries
   */
  void init();

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
  RowWithSchema encodeParserErrorIntoSparkRow(MetronError metronError);

  /**
   * Encode a metron parse result into a Spark row
   *
   * @param parsedMessage The parsed message
   * @return Spark row encoded to our output schema, null if serialisation error occurred
   */
  RowWithSchema encodeParserResultIntoSparkRow(JSONObject parsedMessage) throws JsonProcessingException;


}
