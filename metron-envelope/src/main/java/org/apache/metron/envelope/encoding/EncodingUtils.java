package org.apache.metron.envelope.encoding;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Sets;
import org.apache.metron.common.utils.LazyLogger;
import org.json.simple.JSONObject;

import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

public final class EncodingUtils {

  private EncodingUtils() {}

  /**
   * Encodes all fields in the JSONObject into a HashMap and encodes as per the passed serialisation settings
   * @param mapper serialisation engine
   * @param dataFieldType result spark filed type
   * @param parsedMessage message to encode
   * @return Encoded data (either String or Byte[] depending on configuration)
   * @throws JsonProcessingException if serialisation error occurs
   */
  static Object encodeJsonFields(ObjectMapper mapper, SparkRowEncodingStrategy.DataFieldType dataFieldType, JSONObject parsedMessage) throws JsonProcessingException {
    // Copy remaining fields into a standard HashMap to make sure it encodes cleanly.
    final Set<Map.Entry> reminaingFields = parsedMessage.entrySet();
    final Map<String,Object> remainingFieldsCopy = reminaingFields.stream()
            .collect(Collectors.toMap(Object::toString, Function.identity()));

    // serialise the Map into a single field of the type dataFieldType
    return dataFieldType.mapValue(mapper, remainingFieldsCopy);
  }

  static void warnIfReservedFieldsAreUsed(JSONObject parsedMessage, LazyLogger logger) {
    final Set<String> fieldsInError = Sets.intersection(SparkRowEncodingStrategy.reservedFieldNames, parsedMessage.keySet());
    if (!fieldsInError.isEmpty()) {
      logger.warn(String.format("The following reserved fields will be overwriten"
              + " by internal Metron Processing %s", fieldsInError.stream().collect(Collectors.joining(",", "{", "}"))));
    }
  }
}
