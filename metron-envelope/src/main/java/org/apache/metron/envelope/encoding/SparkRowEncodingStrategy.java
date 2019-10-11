package org.apache.metron.envelope.encoding;

import com.cloudera.labs.envelope.spark.RowWithSchema;
import com.cloudera.labs.envelope.utils.RowUtils;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonProcessingException;
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

import java.io.IOException;
import java.io.Serializable;
import java.util.Set;

/**
 * Interface that encapsulates the way we encode metron data into spark SQL dataframes,
 * and how those dataframes are sent / received over Kafka
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
  StructType getOutputSparkSchema();

  /**
   * The Avro schema of the parsed message as it is stored in Kafka
   * @return Kafka Avro schema
   */
  String getParserOutputKafkaAvroSchema();

  String getEnvelopeKafkaOuputSerialisationSection() throws IOException;
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
  JSONObject decodeParsedMessageFromKafka(@NotNull Row row) throws JsonProcessingException;

  /**
   * Encode a metron enrichmented message into a Spark row
   * @param enrichedMessage  The message to encode
   * @return  The enriched message, encoded into our chosen spark encoding mechanism
   */
  RowWithSchema encodeEnrichedMessageIntoSparkRow(@NotNull JSONObject enrichedMessage) throws JsonProcessingException ;

  RowWithSchema encodeEnrichErrorIntoSparkRow(@NotNull MetronError metronError);

}
