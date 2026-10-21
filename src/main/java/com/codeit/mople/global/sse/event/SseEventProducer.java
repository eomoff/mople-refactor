package com.codeit.mople.global.sse.event;

import com.codeit.mople.domain.directmessage.event.DirectMessageCreatedEvent;
import com.codeit.mople.domain.notification.event.NotificationCreatedEvent;
import com.codeit.mople.global.config.KafkaProperties;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.event.TransactionPhase;
import org.springframework.transaction.event.TransactionalEventListener;

@Component
@ConditionalOnProperty(prefix = KafkaProperties.PREFIX, name = "enabled", havingValue = "true")
public class SseEventProducer {

  private final KafkaTemplate<String, Object> kafkaTemplate;

  private final String directMessageTopic;
  private final String notificationTopic;

  public SseEventProducer(
      KafkaTemplate<String, Object> kafkaTemplate,
      KafkaProperties properties
  ) {
    this.kafkaTemplate = kafkaTemplate;
    this.directMessageTopic = properties.topics().directMessageCreated();
    this.notificationTopic = properties.topics().notificationCreated();
  }

  @TransactionalEventListener(phase = TransactionPhase.AFTER_COMMIT)
  public void on(DirectMessageCreatedEvent event) {
    kafkaTemplate.send(directMessageTopic, event);
  }

  @TransactionalEventListener(phase = TransactionPhase.AFTER_COMMIT)
  public void on(NotificationCreatedEvent event) {
    kafkaTemplate.send(notificationTopic, event);
  }

}
