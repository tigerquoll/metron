package org.apache.metron.envelope.utils;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.metron.common.error.MetronError;
import org.apache.metron.common.utils.LazyLogger;
import org.apache.metron.common.utils.LazyLoggerFactory;

/**
 * Handles metron processing errors by sending them to a error topic
 * encoded as straight text/json
 */
public class MetronErrorHandlerImpl implements MetronErrorHandler {
  private static final LazyLogger LOGGER = LazyLoggerFactory.getLogger(MetronErrorHandlerImpl.class);
  private KafkaProducer<String, String> kafkaProducer;
  private String errorTopic;

  public MetronErrorHandlerImpl(KafkaProducer<String,String> kafkaProducer, String errorTopic) {
    this.kafkaProducer = kafkaProducer;
    this.errorTopic = errorTopic;
  }

  @Override
  public void handleError(MetronError error) {
    kafkaProducer.send( new ProducerRecord<>(errorTopic,error.getJSONObject().toJSONString()));
  }
}
