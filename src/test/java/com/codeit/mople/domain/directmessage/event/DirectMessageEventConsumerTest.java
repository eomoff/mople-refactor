package com.codeit.mople.domain.directmessage.event;

import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

import com.codeit.mople.domain.notification.entity.NotificationTemplate;
import com.codeit.mople.domain.notification.entity.NotificationType;
import com.codeit.mople.domain.notification.service.NotificationCreator;
import com.codeit.mople.global.event.processed.ProcessedEventRepository;
import java.time.Instant;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class DirectMessageEventConsumerTest {

  @Mock
  private NotificationCreator notificationCreator;

  @Mock
  private ProcessedEventRepository processedEventRepository;

  @InjectMocks
  private DirectMessageEventConsumer eventConsumer;

  private UUID directMessageId;
  private UUID receiverId;

  @BeforeEach
  void setUp() {
    directMessageId = UUID.randomUUID();
    receiverId = UUID.randomUUID();
  }

  @Test
  @DisplayName("DM 수신 이벤트 수신 시 DIRECT_MESSAGE_RECEIVED 알림을 생성한다")
  void handle_direct_message_received() {
    // given
    UUID eventId = UUID.randomUUID();
    DirectMessageReceivedEvent event = new DirectMessageReceivedEvent(
        eventId, Instant.now(), directMessageId, receiverId, "발신자", "안녕하세요!"
    );

    given(processedEventRepository.insertIfAbsent(eventId)).willReturn(1);

    // when
    eventConsumer.handle(event);

    // then
    verify(processedEventRepository).insertIfAbsent(eventId);
    verify(notificationCreator).createNotification(
        receiverId,
        new NotificationTemplate(
            "새로운 메시지가 도착했습니다.",
            "발신자: 안녕하세요!"
        ),
        NotificationType.DIRECT_MESSAGE_RECEIVED,
        directMessageId.toString()
    );
  }

  @Test
  @DisplayName("이미 처리된 이벤트는 알림을 생성하지 않고 스킵한다")
  void handle_success_already_processed_skip() {
    // given
    UUID eventId = UUID.randomUUID();
    DirectMessageReceivedEvent event = new DirectMessageReceivedEvent(
        eventId, Instant.now(), directMessageId, receiverId, "발신자", "안녕하세요!"
    );

    given(processedEventRepository.insertIfAbsent(eventId)).willReturn(0);

    // when
    eventConsumer.handle(event);

    // then
    verify(processedEventRepository).insertIfAbsent(eventId);
    verifyNoInteractions(notificationCreator);
  }
}
