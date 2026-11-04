package com.codeit.mople.global.sse.event;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.willThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import com.codeit.mople.domain.directmessage.dto.response.DirectMessageDto;
import com.codeit.mople.domain.directmessage.event.DirectMessageCreatedEvent;
import com.codeit.mople.domain.notification.dto.response.NotificationResponse;
import com.codeit.mople.domain.notification.event.NotificationCreatedEvent;
import com.codeit.mople.global.sse.service.SseEventService;
import com.codeit.mople.global.sse.service.SseService;
import java.time.Instant;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.redis.RedisConnectionFailureException;

@ExtendWith(MockitoExtension.class)
@DisplayName("SSE 재전송용 이벤트 저장 테스트")
class SseEventProducerTest {

  @Mock
  SseService sseService;

  @Mock
  SseEventService sseEventService;

  SseEventProducer producer;

  UUID receiverId;

  @BeforeEach
  void setUp() {
    producer = new SseEventProducer(sseService, sseEventService);

    receiverId = UUID.randomUUID();
  }

  @Test
  @DisplayName("DM 생성 이벤트를 이벤트 ID로 재전송용 저장하는지")
  void savesDirectMessageForResend() {
    // given
    UUID directMessageId = UUID.randomUUID();
    DirectMessageCreatedEvent event = new DirectMessageCreatedEvent(
        UUID.randomUUID(), Instant.now(), receiverId, directMessageId);
    DirectMessageDto directMessageDto = mock(DirectMessageDto.class);

    given(sseEventService.getDirectMessageDto(directMessageId)).willReturn(directMessageDto);

    // when
    producer.on(event);

    // then
    verify(sseService).saveForResend(
        event.eventId(), receiverId, "direct-messages", directMessageDto);
  }

  @Test
  @DisplayName("알림 생성 이벤트를 알림 ID로 재전송용 저장하는지")
  void savesNotificationForResend() {
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
  }

  @Test
  @DisplayName("재전송용 저장이 실패해도 예외를 던지지 않는지")
  void doesNotThrowWhenSaveForResendFails() {
    // given
    DirectMessageCreatedEvent event = new DirectMessageCreatedEvent(
        UUID.randomUUID(), Instant.now(), receiverId, UUID.randomUUID());

    willThrow(new RedisConnectionFailureException("redis down"))
        .given(sseService).saveForResend(any(), any(), any(), any());

    // when & then
    assertThatCode(() -> producer.on(event)).doesNotThrowAnyException();
  }
}
