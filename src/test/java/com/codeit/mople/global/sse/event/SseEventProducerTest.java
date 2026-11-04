package com.codeit.mople.global.sse.event;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.willThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

import com.codeit.mople.domain.directmessage.dto.response.DirectMessageDto;
import com.codeit.mople.domain.directmessage.event.DirectMessageCreatedEvent;
import com.codeit.mople.domain.notification.dto.response.NotificationResponse;
import com.codeit.mople.domain.notification.event.NotificationCreatedEvent;
import com.codeit.mople.global.config.KafkaProperties;
import com.codeit.mople.global.sse.service.SseEventService;
import com.codeit.mople.global.sse.service.SseService;
import java.time.Instant;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.redis.RedisConnectionFailureException;
import org.springframework.kafka.core.KafkaTemplate;

@ExtendWith(MockitoExtension.class)
@DisplayName("SSE 전송 이벤트 Kafka 발행 테스트")
class SseEventProducerTest {

  @Mock
  KafkaTemplate<String, Object> kafkaTemplate;

  @Mock
  SseService sseService;

  @Mock
  SseEventService sseEventService;

  SseEventProducer producer;

  UUID receiverId;

  @BeforeEach
  void setUp() {
    KafkaProperties properties = new KafkaProperties(
        true,
        "localhost:9092",
        new KafkaProperties.Topics(
            "mople.follow.created.v1",
            "mople.playlist.content-added.v1",
            "mople.direct-message.created.v1",
            "mople.notification.created.v1",
            "mople.content-search-index-events.v1",
            "mople.user-search-index-events.v1",
            "mople.playlist-search-index-events.v1",
            "mople.user.account-status-changed.v1",
            "mople.playlist.subscribed.v1",
            "mople.playlist.created.v1",
            "mople.direct-message.received.v1",
            "mople.review.written.v1"
        )
    );

    producer = new SseEventProducer(kafkaTemplate, sseService, sseEventService, properties);

    receiverId = UUID.randomUUID();
  }

  @Test
  @DisplayName("DM 생성 이벤트를 재전송용으로 저장한 뒤 receiverId 키로 발행하는지")
  void savesDirectMessageForResendBeforePublishing() {
    // given
    UUID directMessageId = UUID.randomUUID();
    DirectMessageCreatedEvent event = new DirectMessageCreatedEvent(
        UUID.randomUUID(), Instant.now(), receiverId, directMessageId);
    DirectMessageDto directMessageDto = mock(DirectMessageDto.class);

    given(sseEventService.getDirectMessageDto(directMessageId)).willReturn(directMessageDto);

    // when
    producer.on(event);

    // then
    InOrder inOrder = inOrder(sseService, kafkaTemplate);
    inOrder.verify(sseService).saveForResend(
        event.eventId(), receiverId, "direct-messages", directMessageDto);
    inOrder.verify(kafkaTemplate)
        .send("mople.direct-message.created.v1", receiverId.toString(), event);
  }

  @Test
  @DisplayName("알림 생성 이벤트는 알림 ID로 재전송용 저장만 하고 Kafka로 보내지 않는지")
  void savesNotificationForResendWithoutPublishing() {
    // given
    UUID notificationId = UUID.randomUUID();
    NotificationCreatedEvent event = new NotificationCreatedEvent(
        UUID.randomUUID(), Instant.now(), receiverId, notificationId);
    NotificationResponse notificationResponse = mock(NotificationResponse.class);

    given(sseEventService.getNotificationResponse(notificationId))
        .willReturn(notificationResponse);

    // when
    producer.on(event);

    // then
    verify(sseService).saveForResend(
        notificationId, receiverId, "notifications", notificationResponse);
    verifyNoInteractions(kafkaTemplate);
  }

  @Test
  @DisplayName("재전송용 저장이 실패해도 발행하는지")
  void publishesEvenIfSaveForResendFails() {
    // given
    DirectMessageCreatedEvent event = new DirectMessageCreatedEvent(
        UUID.randomUUID(), Instant.now(), receiverId, UUID.randomUUID());

    willThrow(new RedisConnectionFailureException("redis down"))
        .given(sseService).saveForResend(any(), any(), any(), any());

    // when
    producer.on(event);

    // then
    verify(kafkaTemplate)
        .send("mople.direct-message.created.v1", receiverId.toString(), event);
  }
}
