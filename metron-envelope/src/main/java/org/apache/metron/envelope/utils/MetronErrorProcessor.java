package org.apache.metron.envelope.utils;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.metron.common.Constants;
import org.apache.metron.common.error.MetronError;
import org.apache.metron.common.utils.LazyLogger;
import org.apache.metron.common.utils.LazyLoggerFactory;
import org.apache.metron.envelope.config.ParserConfigManager;

import org.apache.metron.envelope.parsing.MetronSparkParser;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Spark Metron error processing
 */
public class MetronErrorProcessor {
  private static LazyLogger LOG = LazyLoggerFactory.getLogger(MetronErrorProcessor.class);
  // Kafka Producer for handling errors
  private static volatile KafkaProducer<String,String> metronErrorKafkaProducer;

  // Map between SensorType and the relevant errorHandling logic
  private Map<String, MetronErrorHandler> metronErrorHandlers = Collections.emptyMap();
  private MetronErrorHandler defaultErrorHandler;

  /**
   * Used to create Kafka processor for error reporting
   * @param kafkaBrokers
   */
  static void initMetronErrorKafkaProducer(String kafkaBrokers) {
    synchronized (MetronSparkParser.class) {
      if (metronErrorKafkaProducer != null) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBrokers);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "Metron Spark Error Reporter");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        metronErrorKafkaProducer = new KafkaProducer<>(props);
      }
    }
  }

  /**
   * Constructor
   * @param kafkaBrokers Kafka broker to send errors to.
   */
  public MetronErrorProcessor(String kafkaBrokers) {
    if (metronErrorKafkaProducer == null) {
      initMetronErrorKafkaProducer(kafkaBrokers);
    }
    defaultErrorHandler = new MetronErrorHandlerImpl(metronErrorKafkaProducer, Constants.ERROR_STREAM);
  }

  /**
   * Constructor
   * @param kafkaBrokers Kafka broker to send errors to.
   * @param parserConfigManager parsers config as aprser can have multiple error topics, null for default only
   */
  public MetronErrorProcessor(String kafkaBrokers, ParserConfigManager parserConfigManager) {
    if (metronErrorKafkaProducer == null) {
      initMetronErrorKafkaProducer(kafkaBrokers);
    }

    // No parse config means nothing but the default error handler
    metronErrorHandlers = parserConfigManager != null ?
            createSensorTypeToErrorHandler(parserConfigManager) :
            Collections.emptyMap();

    defaultErrorHandler = new MetronErrorHandlerImpl(metronErrorKafkaProducer, Constants.ERROR_STREAM);
  }

  /**
   * Creates the various MetronErrorHandlers for each snesor
   * @param parserConfigManager configManager for sensor->topic mappings
   * @return Map of sensorTypes -> ErrorTopics
   */
  private Map<String, MetronErrorHandler> createSensorTypeToErrorHandler(ParserConfigManager parserConfigManager) {
    return parserConfigManager.getConfigurations().getTypes().stream()
            .collect(Collectors.toMap(
                    sensor -> sensor,
                    sensor -> new MetronErrorHandlerImpl(metronErrorKafkaProducer,
                            parserConfigManager.getConfigurations().getSensorParserConfig(sensor).getErrorTopic())));
  }

  /**
   * Given an set of sensors, picks the first error topic that is configured
   * @param sensors Set of sensors to check
   * @return Optional: true if error handler is configured, present if not
   */
  private Optional<MetronErrorHandler> pickErrorHandler(Set<String> sensors) {
    for( String sensor: sensors) {
      if (metronErrorHandlers.containsKey(sensor)) {
        return Optional.of(metronErrorHandlers.get(sensor));
      }
    }
    return Optional.empty();
  }

  public void handleMetronError(final MetronError metronError) {
    if (metronError != null) {
      pickErrorHandler(metronError.getSensorTypes()).orElse(defaultErrorHandler).handleError(metronError);
    } else {
      LOG.warn("Error Processor parsed a null MetronError to process");
    }
  }

  public void close() {
    metronErrorHandlers.clear();
    defaultErrorHandler = null;

    // We don't delete the producer, as others could be using it
    metronErrorKafkaProducer.flush();
  }
}
