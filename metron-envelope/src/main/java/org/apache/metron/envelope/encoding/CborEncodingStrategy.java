package org.apache.metron.envelope.encoding;

import com.cloudera.labs.envelope.spark.RowWithSchema;
import com.cloudera.labs.envelope.utils.RowUtils;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.cbor.CBORFactory;
import envelope.shaded.com.google.common.collect.ImmutableList;
import org.apache.metron.common.error.MetronError;
import org.apache.metron.common.utils.LazyLogger;
import org.apache.metron.common.utils.LazyLoggerFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.json.simple.JSONObject;

public class CborEncodingStrategy implements SparkRowEncodingStrategy {
  private static final Object versionTag = RowUtils.toRowValue(1, DataTypes.ShortType);
  private static LazyLogger LOGGER = LazyLoggerFactory.getLogger(CborEncodingStrategy.class);
  /**
   * Schema of processed data
   */
  private static StructType outputSchema = DataTypes.createStructType(new StructField[]{
          // Initially just 0x1 - used to enable backward compatibility while we are not using a schema registry
          DataTypes.createStructField("version", DataTypes.ShortType, false),
          // JSON data encoded into Concise Binary Object Representation (CBOR) see RFC 7049 for specific details
          DataTypes.createStructField("cborvalue", DataTypes.BinaryType, false),
          // Does cbordata contain a Metron Parse output or a Metron Error object
          DataTypes.createStructField("isErrorValue", DataTypes.BooleanType, false)
  });
  private transient ObjectMapper mapper = null;

  @Override
  public StructType getOutputSchema() {
    return outputSchema;
  }

  @Override
  public synchronized void init() {
    final CBORFactory f = new CBORFactory();
    mapper = new ObjectMapper(f);
  }

  /**
   * Encode a metron parse error into a Spark row
   *
   * @param metronError The error to encode
   * @return Spark row encoded to our output schema, null if serialisation error occurred
   */
  @Override
  public RowWithSchema encodeErrorIntoSparkRow(MetronError metronError) {
    return encodeSparkRow(metronError, true);
  }

  /**
   * Encode a metron parse result into a Spark row
   *
   * @param parsedMessage The parsed message
   * @return Spark row encoded to our output schema, null if serialisation error occurred
   */
  @Override
  public RowWithSchema encodeResultIntoSparkRow(JSONObject parsedMessage) {
    return encodeSparkRow(parsedMessage, false);
  }

  /**
   * Encodes a metron parse result into a Spark row
   *
   * @param obj            The object to encode
   * @param errorIndicator If this object represents an error at all
   * @return Spar row encoded to out outut schema, null if a serialisation error occurred
   */
  private RowWithSchema encodeSparkRow(Object obj, boolean errorIndicator) {
    byte[] data;
    try {
      data = mapper.writeValueAsBytes(obj);
    } catch (JsonProcessingException e) {
      LOGGER.error("Error serialising metron parse error", e);
      return null;
    }
    Object sparkValue = RowUtils.toRowValue(data, DataTypes.BinaryType);
    final Object errorInd = RowUtils.toRowValue(errorIndicator, DataTypes.BooleanType);
    return new RowWithSchema(outputSchema, ImmutableList.of(versionTag, sparkValue, errorInd));
  }
}
