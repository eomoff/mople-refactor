package com.codeit.mople.domain.user.event;

import com.codeit.mople.domain.user.admin.event.UserAccountStatusChangedEvent;
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
  private final String userAccountStatusChangedTopic;

  public UserEventProducer(
      KafkaTemplate<String, Object> kafkaTemplate,
      KafkaProperties properties
  ) {
    this.kafkaTemplate = kafkaTemplate;
    this.userSearchIndexTopic = properties.topics().userSearchIndex();
    this.userAccountStatusChangedTopic = properties.topics().userAccountStatusChanged();
  }

  @TransactionalEventListener(phase = TransactionPhase.AFTER_COMMIT)
  public void on(UserSearchIndexEvent event) {
    kafkaTemplate.send(
        userSearchIndexTopic,
        event.userId().toString(),
        event
    );
  }

  @TransactionalEventListener(phase = TransactionPhase.AFTER_COMMIT)
  public void on(UserAccountStatusChangedEvent event) {
    kafkaTemplate.send(
        userAccountStatusChangedTopic,
        event.userId().toString(),
        event
    );
  }

}