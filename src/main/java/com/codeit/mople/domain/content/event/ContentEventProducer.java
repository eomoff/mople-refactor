package com.codeit.mople.domain.content.event;

import com.codeit.mople.global.config.KafkaProperties;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.event.TransactionPhase;
import org.springframework.transaction.event.TransactionalEventListener;

@Component
@ConditionalOnProperty(prefix = KafkaProperties.PREFIX, name = "enabled", havingValue = "true")
public class ContentEventProducer {

  private final KafkaTemplate<String, Object> kafkaTemplate;

  private final String contentSearchIndexTopic;

  public ContentEventProducer(
      KafkaTemplate<String, Object> kafkaTemplate,
      KafkaProperties properties
  ) {
    this.kafkaTemplate = kafkaTemplate;
    this.contentSearchIndexTopic = properties.topics().contentSearchIndex();
  }

  @TransactionalEventListener(phase = TransactionPhase.AFTER_COMMIT)
  public void on(ContentSearchIndexEvent event) {
    kafkaTemplate.send(
        contentSearchIndexTopic,
        event.contentId().toString(),
        event
    );
  }

  @TransactionalEventListener(phase = TransactionPhase.AFTER_COMMIT)
  public void on(ContentSearchIndexDeleteEvent event) {
    kafkaTemplate.send(
        contentSearchIndexTopic,
        event.contentId().toString(),
        event
    );
  }
}