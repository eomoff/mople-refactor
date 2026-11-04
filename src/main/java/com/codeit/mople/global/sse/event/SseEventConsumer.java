package com.codeit.mople.global.sse.event;

import com.codeit.mople.domain.directmessage.dto.response.DirectMessageDto;
import com.codeit.mople.domain.directmessage.event.DirectMessageCreatedEvent;
import com.codeit.mople.domain.notification.dto.response.NotificationResponse;
import com.codeit.mople.domain.notification.event.NotificationCreatedEvent;
import com.codeit.mople.global.config.KafkaProperties;
import com.codeit.mople.global.sse.service.SseEventService;
import com.codeit.mople.global.sse.service.SseService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
@ConditionalOnProperty(prefix = KafkaProperties.PREFIX, name = "enabled", havingValue = "true")
public class SseEventConsumer {

  private final SseService sseService;
  private final SseEventService sseEventService;

  @KafkaListener(
      topics = "${spring.kafka.topics.direct-message-created}",
      groupId = "sse-${sse.server-id}",
      properties = "auto.offset.reset=latest"
  )
  public void handle(DirectMessageCreatedEvent event) {
    if (!sseService.isConnected(event.receiverId())) {
      return;
    }

    log.debug("SSE 이벤트 전송 시도: receiverId={}, directMessageId={}",
        event.receiverId(), event.directMessageId());

    DirectMessageDto response =
        sseEventService.getDirectMessageDto(event.directMessageId());

    // SSE 이벤트 전송
    sseService.send(
        event.eventId(),
        event.receiverId(),
        "direct-messages",
        response
    );

    log.info("SSE 전송 완료: receiverId={}, directMessageId={}",
        event.receiverId(), event.directMessageId());
  }

  @KafkaListener(
      topics = "${spring.kafka.topics.notification-created}",
      groupId = "sse-${sse.server-id}",
      properties = "auto.offset.reset=latest"
  )
  public void handle(NotificationCreatedEvent event) {
    if (!sseService.isConnected(event.receiverId())) {
      return;
    }

    log.debug("SSE 이벤트 전송 시도: receiverId={}, notificationId={}",
        event.receiverId(), event.notificationId());

    NotificationResponse response =
        sseEventService.getNotificationResponse(event.notificationId());

    sseService.send(
        event.notificationId(),
        event.receiverId(),
        "notifications",
        response
    );

    log.info("알림 SSE 전송 완료: receiverId={}, notificationId={}",
        event.receiverId(), event.notificationId());
  }

}
