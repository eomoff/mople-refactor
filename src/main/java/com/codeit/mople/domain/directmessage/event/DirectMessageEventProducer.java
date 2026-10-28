package com.codeit.mople.domain.directmessage.event;

import com.codeit.mople.global.config.KafkaProperties;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.event.TransactionPhase;
import org.springframework.transaction.event.TransactionalEventListener;

@Slf4j
@Component
@ConditionalOnProperty(prefix = KafkaProperties.PREFIX, name = "enabled", havingValue = "true")
public class DirectMessageEventProducer {

  private final KafkaTemplate<String, Object> kafkaTemplate;
  private final String topic;

  public DirectMessageEventProducer(
      KafkaTemplate<String, Object> kafkaTemplate,
      KafkaProperties kafkaProperties
  ) {
    this.kafkaTemplate = kafkaTemplate;
    this.topic = kafkaProperties.topics().directMessageReceived();
  }

  @TransactionalEventListener(phase = TransactionPhase.AFTER_COMMIT)
  public void on(DirectMessageReceivedEvent event) {
    String key = event.receiverId().toString();

    log.debug("DM 수신 이벤트 발행: directMessageId={}, eventId={}",
        event.directMessageId(), event.eventId());
    kafkaTemplate.send(topic, key, event);
  }
}
