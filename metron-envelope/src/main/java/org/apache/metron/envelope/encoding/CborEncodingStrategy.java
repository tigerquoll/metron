package org.apache.metron.envelope.encoding;

import com.cloudera.labs.envelope.spark.RowWithSchema;
import com.cloudera.labs.envelope.utils.RowUtils;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.cbor.CBORFactory;
import envelope.shaded.com.google.common.collect.ImmutableList;
import envelope.shaded.com.google.common.collect.ImmutableSet;
import org.apache.metron.common.error.MetronError;
import org.apache.metron.common.utils.LazyLogger;
import org.apache.metron.common.utils.LazyLoggerFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.json.simple.JSONObject;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class CborEncodingStrategy implements SparkRowEncodingStrategy {
  private static final Object versionTag = RowUtils.toRowValue(1, DataTypes.ShortType);
  private static LazyLogger LOGGER = LazyLoggerFactory.getLogger(CborEncodingStrategy.class);
  public static final String VERSION = "_version";
  public static final String CBORVALUE = "_cborvalue";
  public static final String IS_ERROR_VALUE = "_isErrorValue";

  private final static StructField cborFieldSchema = DataTypes.createStructField(CBORVALUE, DataTypes.BinaryType, false);
  private final static StructField errorIndFieldSchema = DataTypes.createStructField(IS_ERROR_VALUE, DataTypes.BooleanType, false);
  private final static StructField versionFieldSchema = DataTypes.createStructField(VERSION, DataTypes.ShortType, false);

  /**
   * Schema of processed data
   */
  private static List<StructField> parserOutputFields = new ImmutableList.Builder<StructField>()
          .add(versionFieldSchema)
          .add(DataTypes.createStructField("ip_src_addr", DataTypes.StringType, true))
          .add(DataTypes.createStructField("ip_dst_addr",  DataTypes.StringType, true))
          .add(DataTypes.createStructField("ip_src_port",  DataTypes.IntegerType, true))
          .add(DataTypes.createStructField("ip_dst_port",  DataTypes.IntegerType, true))
          .add(DataTypes.createStructField("ip_dst_port",  DataTypes.IntegerType, true))
          .add(DataTypes.createStructField("protocol",  DataTypes.StringType, true))
          .add(DataTypes.createStructField("timestamp",  DataTypes.LongType, true))
          .add(DataTypes.createStructField("original_string",  DataTypes.StringType, true))
          // JSON data encoded into Concise Binary Object Representation (CBOR) see RFC 7049 for specific details
          .add(cborFieldSchema)
          // Does cbordata contain a Metron Parse output or a Metron Error object
          .add(errorIndFieldSchema)
          .build();
  private static Set<String> reservedFieldNames = ImmutableSet.of(VERSION,CBORVALUE,IS_ERROR_VALUE);

  /**
   * Index of field name to field type
   */
  private Map<String, StructField> schemaIndex = parserOutputFields.stream()
          .filter( x -> !reservedFieldNames.contains(x.name()))
          .collect(Collectors.toMap(StructField::name, x->x));

  private static StructType parserOutputSchema = DataTypes.createStructType(parserOutputFields);
  private transient ObjectMapper mapper = null;

  @Override
  public StructType getParserOutputSchema() {
    return parserOutputSchema;
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
  public RowWithSchema encodeParserErrorIntoSparkRow(MetronError metronError) {
    return encodeSparkRow(metronError, true);
  }

  // Creates a row schema with nullable values set to null
  public Map<StructField, Object> createDefaultRow() {
    return parserOutputFields.stream().filter(StructField::nullable).collect(Collectors.toMap( x->x, null));
  }

  /**
   * Encode a metron parse result into a Spark row
   *
   * @param parsedMessage The parsed message
   * @return Spark row encoded to our output schema, null if serialisation error occurred
   */
  @Override
  public RowWithSchema encodeParserResultIntoSparkRow(JSONObject parsedMessage) throws JsonProcessingException {
    // new plan
    // iterate throw row schema
    // grab value (or null) and add to separate lists (or List <EntrySet>)
    // all other fields get added to separate map for encoding into CBOR

    final Map<StructField,Object> encodedRow = new HashMap<>();
    final Map<String, Object> nonStandardFields = new HashMap<>();

    for(Map.Entry entry : (Set<Map.Entry>) parsedMessage.entrySet()) {
      final String key = (String)entry.getKey();
      // todo: Verify that binary objects are passed as objects in JSONObject
      final Object value = (Object) entry.getValue();
      if (schemaIndex.containsKey(key)) {
        // This is a well known field, it will be stored explicitly
        final Object fieldValue = RowUtils.toRowValue(value, schemaIndex.get(key).dataType());
        encodedRow.put(schemaIndex.get(key), fieldValue);
      } else {
        nonStandardFields.put(key,value);
      }
    }

    // todo: What happens if we have no nonStandardFields? null or empty list?
    final Object nonStandardFieldValues = RowUtils.toRowValue(nonStandardFields, DataTypes.BinaryType);
    encodedRow.put(nonStandardFieldValues, cborFieldSchema);

    final Object errorInd = RowUtils.toRowValue(false, DataTypes.BooleanType);
    encodedRow.put(errorInd, errorIndFieldSchema);

    final Object schemaVersion =  RowUtils.toRowValue(1, DataTypes.ShortType);
    encodedRow.put(schemaVersion, versionFieldSchema);

    List<Object> fieldValues = null;
    List<StructField> fieldSchema = null;


    return new RowWithSchema(fieldValues, fieldSchema);
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
    return new RowWithSchema(parserOutputSchema, ImmutableList.of(versionTag, sparkValue, errorInd));
  }
}
