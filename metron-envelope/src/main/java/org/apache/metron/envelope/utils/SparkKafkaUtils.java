package org.apache.metron.envelope.utils;

import envelope.shaded.com.google.common.collect.ImmutableMap;
import org.apache.spark.sql.Row;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Map;
import java.util.Objects;

public final class SparkKafkaUtils {
  @NotNull public static final String KAFKA_TOPICNAME_FIELD = "topic";
  @NotNull public static final String KAFKA_TIMESTAMP_FIELD = "timestamp";
  @NotNull public static final String KAFKA_PARTITION_FIELD = "partition";
  @NotNull public static final String KAFKA_OFFSET_FIELD = "offset";
  @NotNull public static final String KAFKA_KEY_FIELD = "key";
  @NotNull public static final String KAFKA_VALUE_FIELD = "value";

  @NotNull public static final String ENVELOPE_KAFKA_TOPICNAME_FIELD = KAFKA_TOPICNAME_FIELD;
  @NotNull public static final String ENVELOPE_KAFKA_TIMESTAMP_FIELD = KAFKA_TIMESTAMP_FIELD;
  @NotNull public static final String ENVELOPE_KAFKA_PARTITION_FIELD = KAFKA_PARTITION_FIELD;
  @NotNull public static final String ENVELOPE_KAFKA_OFFSET_FIELD = KAFKA_OFFSET_FIELD;
  @NotNull public static final String ENVELOPE_KAFKA_KEY_FIELD = KAFKA_KEY_FIELD;
  @NotNull public static final String ENVELOPE_KAFKA_VALUE_FIELD = KAFKA_VALUE_FIELD;

  private SparkKafkaUtils() {}
  /**
   * Encodes assumptions about input data format
   * @param row Row of data extracted from Kafka
   * @return extracted metadata
   */
  @NotNull
  public static Map<String, Object> extractKafkaMetadata(@NotNull Row row) {
    return ImmutableMap.of(
            ENVELOPE_KAFKA_TIMESTAMP_FIELD, row.getAs(KAFKA_TIMESTAMP_FIELD),
            ENVELOPE_KAFKA_TOPICNAME_FIELD, row.getAs(KAFKA_TOPICNAME_FIELD),
            ENVELOPE_KAFKA_PARTITION_FIELD, row.getAs(KAFKA_PARTITION_FIELD),
            ENVELOPE_KAFKA_OFFSET_FIELD, row.getAs(KAFKA_OFFSET_FIELD));
  }

  public static Map<String, Object> extractKafkaMessage(@NotNull Row row) {
    return ImmutableMap.of(
            ENVELOPE_KAFKA_KEY_FIELD, row.getAs(KAFKA_KEY_FIELD),
            ENVELOPE_KAFKA_VALUE_FIELD, row.getAs(KAFKA_VALUE_FIELD)
    );
  }

  
  /**
   * Specialised Pair class to contain retrieved Kafka Messages
   * @param <K> Type of the Key field
   * @param <V> Type of the Value field
   */
  public static class KafkaMessage<K,V> {
    @Nullable private final K key;
    @Nullable private final V value;

    private KafkaMessage(@Nullable K key, @Nullable V value) {
      this.key = key;
      this.value = value;
    }

    @NotNull
    public static <K,V> KafkaMessage<K,V> of(@Nullable K key, @Nullable V value) {
      return new KafkaMessage<>(key,value);
    }

    @Nullable
    public K getKey() {
      return key;
    }

    @Nullable
    public V getValue() {
      return value;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      KafkaMessage<?, ?> that = (KafkaMessage<?, ?>) o;
      return Objects.equals(getKey(), that.getKey()) &&
              Objects.equals(getValue(), that.getValue());
    }

    @Override
    public int hashCode() {
      return Objects.hash(getKey(), getValue());
    }

    @Override
    public String toString() {
      return "KafkaMessage{" +
              "key=" + key +
              ", value=" + value +
              '}';
    }
  }

  /*
  Fields to be written out include:
    public enum Fields implements Field {
     SRC_ADDR("ip_src_addr")
    ,SRC_PORT("ip_src_port")
    ,DST_ADDR("ip_dst_addr")
    ,DST_PORT("ip_dst_port")
    ,PROTOCOL("protocol")
    ,TIMESTAMP("timestamp")
    ,ORIGINAL("original_string")
    ,GUID("guid")
    ,SENSOR_TYPE("source.type")
    ,INCLUDES_REVERSE_TRAFFIC("includes_reverse_traffic")
   */
}
