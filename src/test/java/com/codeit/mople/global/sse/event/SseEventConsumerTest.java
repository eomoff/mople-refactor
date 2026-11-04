package com.codeit.mople.global.sse.event;

import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;

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
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class SseEventConsumerTest {

  @Mock
  private SseService sseService;

  @Mock
  private SseEventService sseEventService;

  @InjectMocks
  private SseEventConsumer eventConsumer;

  private UUID receiverId;
  private UUID directMessageId;
  private UUID notificationId;

  @BeforeEach
  void setUp() {
    // 공통
    receiverId = UUID.randomUUID();
    directMessageId = UUID.randomUUID();
    notificationId = UUID.randomUUID();
  }

  @Nested
  @DisplayName("DM 생성 이벤트")
  class DirectMessageCreated {

    @Test
    @DisplayName("DM 생성 이벤트 성공 - 이 서버에 연결된 사용자에게 이벤트 ID로 전송")
    void handle_success() {
      // given
      UUID eventId = UUID.randomUUID();

      DirectMessageCreatedEvent event =
          new DirectMessageCreatedEvent(eventId, Instant.now(), receiverId, directMessageId);

      DirectMessageDto directMessageDto = mock(DirectMessageDto.class);

      given(sseService.isConnected(receiverId)).willReturn(true);
      given(sseEventService.getDirectMessageDto(directMessageId))
          .willReturn(directMessageDto);

      // when
      eventConsumer.handle(event);

      // then
      verify(sseService).send(eventId, receiverId, "direct-messages", directMessageDto);
    }

    @Test
    @DisplayName("DM 생성 이벤트 무시 - 이 서버에 연결이 없으면 DM을 조회하지 않음")
    void handle_ignore_notConnected() {
      // given
      DirectMessageCreatedEvent event = new DirectMessageCreatedEvent(
          UUID.randomUUID(), Instant.now(), receiverId, directMessageId);

      given(sseService.isConnected(receiverId)).willReturn(false);

      // when
      eventConsumer.handle(event);

      // then
      verify(sseService).isConnected(receiverId);
      verifyNoMoreInteractions(sseService);
      verifyNoInteractions(sseEventService);
    }

  }

  @Nested
  @DisplayName("알림 생성 이벤트")
  class NotificationCreated {

    @Test
    @DisplayName("알림 생성 이벤트 성공 - 이 서버에 연결된 사용자에게 알림 ID로 전송")
    void handle_success() {
      // given
      NotificationCreatedEvent event = new NotificationCreatedEvent(
          UUID.randomUUID(), Instant.now(), receiverId, notificationId);

      NotificationResponse notificationResponse = mock(NotificationResponse.class);

      given(sseService.isConnected(receiverId)).willReturn(true);
      given(sseEventService.getNotificationResponse(notificationId))
          .willReturn(notificationResponse);

      // when
      eventConsumer.handle(event);

      // then
      verify(sseService).send(notificationId, receiverId, "notifications", notificationResponse);
    }

    @Test
    @DisplayName("알림 생성 이벤트 무시 - 이 서버에 연결이 없으면 알림을 조회하지 않음")
    void handle_ignore_notConnected() {
      // given
      NotificationCreatedEvent event = new NotificationCreatedEvent(
          UUID.randomUUID(), Instant.now(), receiverId, notificationId);

      given(sseService.isConnected(receiverId)).willReturn(false);

      // when
      eventConsumer.handle(event);

      // then
      verify(sseService).isConnected(receiverId);
      verifyNoMoreInteractions(sseService);
      verifyNoInteractions(sseEventService);
    }

  }

}
