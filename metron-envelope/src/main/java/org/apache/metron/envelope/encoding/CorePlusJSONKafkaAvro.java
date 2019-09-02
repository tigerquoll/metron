package org.apache.metron.envelope.encoding;

import com.cloudera.labs.envelope.spark.RowWithSchema;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.metron.common.error.MetronError;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.json.simple.JSONObject;

public class CorePlusJSONKafkaAvro implements SparkRowEncodingStrategy {

  @NotNull final Serializer<Row> avroEncoder = new JsonFactory();
  @NotNull final JsonFactory jsonEncoder

  @Override
  public void init(@Nullable String additionalConfig) {

  }

  @Override
  public StructType getParserOutputSchema() {
    return null;
  }

  @Override
  public RowWithSchema encodeParserErrorIntoSparkRow(@NotNull MetronError metronError) throws JsonProcessingException {
    return null;
  }

  @Override
  public RowWithSchema encodeParserResultIntoSparkRow(@NotNull JSONObject parsedMessage) throws JsonProcessingException {
    return null;
  }

  @Override
  public JSONObject decodeParsedMessage(@NotNull Row row) {
    return null;
  }
}
