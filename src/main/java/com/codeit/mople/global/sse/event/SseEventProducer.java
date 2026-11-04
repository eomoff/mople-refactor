package com.codeit.mople.global.sse.event;

import com.codeit.mople.domain.directmessage.event.DirectMessageCreatedEvent;
import com.codeit.mople.domain.notification.event.NotificationCreatedEvent;
import com.codeit.mople.global.config.KafkaProperties;
import com.codeit.mople.global.sse.service.SseEventService;
import com.codeit.mople.global.sse.service.SseService;
import java.util.UUID;
import java.util.function.Supplier;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.event.TransactionPhase;
import org.springframework.transaction.event.TransactionalEventListener;

@Slf4j
@Component
@ConditionalOnProperty(prefix = KafkaProperties.PREFIX, name = "enabled", havingValue = "true")
public class SseEventProducer {

  private final KafkaTemplate<String, Object> kafkaTemplate;
  private final SseService sseService;
  private final SseEventService sseEventService;

  private final String directMessageTopic;

  public SseEventProducer(
      KafkaTemplate<String, Object> kafkaTemplate,
      SseService sseService,
      SseEventService sseEventService,
      KafkaProperties properties
  ) {
    this.kafkaTemplate = kafkaTemplate;
    this.sseService = sseService;
    this.sseEventService = sseEventService;
    this.directMessageTopic = properties.topics().directMessageCreated();
  }

  @TransactionalEventListener(phase = TransactionPhase.AFTER_COMMIT)
  public void on(DirectMessageCreatedEvent event) {
    saveForResend(event.eventId(), event.receiverId(), "direct-messages",
        () -> sseEventService.getDirectMessageDto(event.directMessageId()));

    kafkaTemplate.send(directMessageTopic, event.receiverId().toString(), event);
  }

  @TransactionalEventListener(phase = TransactionPhase.AFTER_COMMIT)
  public void on(NotificationCreatedEvent event) {
    saveForResend(event.notificationId(), event.receiverId(), "notifications",
        () -> sseEventService.getNotificationResponse(event.notificationId()));
  }

  private void saveForResend(
      UUID eventId,
      UUID receiverId,
      String eventName,
      Supplier<Object> data
  ) {
    try {
      sseService.saveForResend(eventId, receiverId, eventName, data.get());
    } catch (RuntimeException e) {
      log.warn("재전송용 SSE 이벤트 저장 실패: eventId={}, receiverId={}", eventId, receiverId, e);
    }
  }

}
