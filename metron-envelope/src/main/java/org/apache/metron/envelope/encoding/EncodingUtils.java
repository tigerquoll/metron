package org.apache.metron.envelope.encoding;

import com.cloudera.labs.envelope.utils.RowUtils;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Sets;
import envelope.shaded.com.google.common.collect.ImmutableSet;
import org.apache.metron.common.utils.LazyLogger;
import org.apache.metron.common.utils.LazyLoggerFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.json.simple.JSONObject;

import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

final class EncodingUtils {
  private static final LazyLogger LOGGER = LazyLoggerFactory.getLogger(EncodingUtils.class);

  /**
   * Reserved fields used in all row encoding strategies
   */
  private static final String ERROR_INDICATOR_FIELD = "_isErrorValue";
  static final Object ERROR_INDICATOR_FALSE = RowUtils.toRowValue(false, DataTypes.BooleanType);
  static final Object ERROR_INDICATOR_TRUE = RowUtils.toRowValue(true, DataTypes.BooleanType);

  private static final String SCHEMA_VERSION_FIELD = "_schemaVersion";
  static final Object VERSION_ONE = RowUtils.toRowValue(1, DataTypes.ShortType);
  static final String DATA_FIELD_NAME = "_dataValue";

  static final StructField errorIndFieldSchema = DataTypes.createStructField(ERROR_INDICATOR_FIELD, DataTypes.BooleanType, false);
  static final StructField versionFieldSchema = DataTypes.createStructField(SCHEMA_VERSION_FIELD, DataTypes.ShortType, false);
  static final Set<String> reservedFieldNames = ImmutableSet.of(SCHEMA_VERSION_FIELD, DATA_FIELD_NAME, ERROR_INDICATOR_FIELD);

  private EncodingUtils() {}

  /**
   * Encodes all fields in the JSONObject into a HashMap and encodes as per the passed serialisation settings
   * @param mapper serialisation engine
   * @param dataFieldType result spark filed type
   * @param parsedMessage message to encode
   * @return Encoded data (either String or Byte[] depending on configuration)
   * @throws JsonProcessingException if serialisation error occurs
   */
  static Object encodeCombinedFields(ObjectMapper mapper, AbstractSparkRowEncodingStrategy.DataFieldType dataFieldType, JSONObject parsedMessage) throws JsonProcessingException {
    // Copy remaining fields into a standard HashMap to make sure it encodes cleanly.
    final Set<Map.Entry> reminaingFields = parsedMessage.entrySet();
    final Map<String,Object> remainingFieldsCopy = reminaingFields.stream()
            .collect(Collectors.toMap(Object::toString, Function.identity()));

    // serialise the Map into a single field of the type dataFieldType
    return dataFieldType.mapValue(mapper, remainingFieldsCopy);
  }

  static void warnIfReservedFieldsAreUsed(JSONObject parsedMessage) {
    final Set<String> fieldsInError = Sets.intersection(reservedFieldNames, parsedMessage.keySet());
    if (!fieldsInError.isEmpty()) {
      LOGGER.warn(String.format("The following reserved fields will be overwriten"
              + " by internal Metron Processing %s", fieldsInError.stream().collect(Collectors.joining(",", "{", "}"))));
    }
  }
}
