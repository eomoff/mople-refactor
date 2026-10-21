package com.codeit.mople.domain.user.event;

import com.codeit.mople.global.config.KafkaProperties;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.event.TransactionPhase;
import org.springframework.transaction.event.TransactionalEventListener;

@Component
@ConditionalOnProperty(prefix = KafkaProperties.PREFIX, name = "enabled", havingValue = "true")
public class UserEventProducer {

  private final KafkaTemplate<String, Object> kafkaTemplate;

  private final String userSearchIndexTopic;

  public UserEventProducer(
      KafkaTemplate<String, Object> kafkaTemplate,
      KafkaProperties properties
  ) {
    this.kafkaTemplate = kafkaTemplate;
    this.userSearchIndexTopic = properties.topics().userSearchIndex();
  }

  @TransactionalEventListener(phase = TransactionPhase.AFTER_COMMIT)
  public void on(UserSearchIndexEvent event) {
    kafkaTemplate.send(
        userSearchIndexTopic,
        event.userId().toString(),
        event
    );
  }

}