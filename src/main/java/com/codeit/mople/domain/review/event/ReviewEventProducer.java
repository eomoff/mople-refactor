package com.codeit.mople.domain.review.event;

import com.codeit.mople.global.config.KafkaProperties;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.event.TransactionPhase;
import org.springframework.transaction.event.TransactionalEventListener;

@Component
@ConditionalOnProperty(prefix = KafkaProperties.PREFIX, name = "enabled", havingValue = "true")
public class ReviewEventProducer {

  private final KafkaTemplate<String, Object> kafkaTemplate;

  private final String reviewWrittenTopic;

  public ReviewEventProducer(
      KafkaTemplate<String, Object> kafkaTemplate,
      KafkaProperties properties
  ) {
    this.kafkaTemplate = kafkaTemplate;
    this.reviewWrittenTopic = properties.topics().reviewWritten();
  }

  @TransactionalEventListener(phase = TransactionPhase.AFTER_COMMIT)
  public void on(ReviewWrittenEvent event) {
    kafkaTemplate.send(reviewWrittenTopic, event.eventId().toString(), event);
  }

}
