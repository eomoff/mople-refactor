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

  private final String reviewCreatedTopic;
  private final String reviewUpdatedTopic;
  private final String reviewDeletedTopic;

  public ReviewEventProducer(
      KafkaTemplate<String, Object> kafkaTemplate,
      KafkaProperties properties
  ) {
    this.kafkaTemplate = kafkaTemplate;
    this.reviewCreatedTopic = properties.topics().reviewCreated();
    this.reviewUpdatedTopic = properties.topics().reviewUpdated();
    this.reviewDeletedTopic = properties.topics().reviewDeleted();
  }

  @TransactionalEventListener(phase = TransactionPhase.AFTER_COMMIT)
  public void on(ReviewCreatedEvent event) {
    kafkaTemplate.send(reviewCreatedTopic, event.eventId().toString(), event);
  }

  @TransactionalEventListener(phase = TransactionPhase.AFTER_COMMIT)
  public void on(ReviewUpdatedEvent event) {
    kafkaTemplate.send(reviewUpdatedTopic, event.eventId().toString(), event);
  }

  @TransactionalEventListener(phase = TransactionPhase.AFTER_COMMIT)
  public void on(ReviewDeletedEvent event) {
    kafkaTemplate.send(reviewDeletedTopic, event.eventId().toString(), event);
  }

}
