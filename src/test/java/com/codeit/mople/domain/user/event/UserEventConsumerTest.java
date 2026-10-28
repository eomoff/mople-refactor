package com.codeit.mople.domain.user.event;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

import com.codeit.mople.domain.notification.entity.NotificationTemplate;
import com.codeit.mople.domain.notification.entity.NotificationType;
import com.codeit.mople.domain.notification.service.NotificationCreator;
import com.codeit.mople.domain.user.admin.enums.ForceLogoutReason;
import com.codeit.mople.domain.user.admin.event.UserAccountStatusChangedEvent;
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
class UserEventConsumerTest {

  @Mock
  private NotificationCreator notificationCreator;

  @Mock
  private ProcessedEventRepository processedEventRepository;

  @InjectMocks
  private UserEventConsumer eventConsumer;

  private UUID userId;

  @BeforeEach
  void setUp() {
    userId = UUID.randomUUID();
  }

  @Test
  @DisplayName("권한 변경 이벤트 수신 시 ROLE_CHANGED 알림을 생성한다")
  void handle_role_change() {
    // given
    UUID eventId = UUID.randomUUID();
    UserAccountStatusChangedEvent event = new UserAccountStatusChangedEvent(
        eventId, Instant.now(), userId, ForceLogoutReason.ROLE_CHANGE, true
    );

    given(processedEventRepository.insertIfAbsent(eventId)).willReturn(1);

    // when
    eventConsumer.handle(event);

    // then
    verify(processedEventRepository).insertIfAbsent(eventId);
    verify(notificationCreator).createNotification(
        eq(userId),
        eq(new NotificationTemplate(
            "권한이 변경되었습니다.",
            "관리자에 의해 권한이 변경되었습니다. 다시 로그인해주세요."
        )),
        eq(NotificationType.ROLE_CHANGED),
        anyString()
    );
  }

  @Test
  @DisplayName("계정 잠금 이벤트 수신 시 알림을 생성하지 않는다")
  void handle_account_locked() {
    // given
    UserAccountStatusChangedEvent event = new UserAccountStatusChangedEvent(
        UUID.randomUUID(), Instant.now(), userId, ForceLogoutReason.ACCOUNT_LOCKED, true
    );

    // when
    eventConsumer.handle(event);

    // then
    verifyNoInteractions(processedEventRepository, notificationCreator);
  }

  @Test
  @DisplayName("계정 잠금 해제 이벤트 수신 시 알림을 생성하지 않는다")
  void handle_account_unlocked() {
    // given
    UserAccountStatusChangedEvent event = new UserAccountStatusChangedEvent(
        UUID.randomUUID(), Instant.now(), userId, ForceLogoutReason.ACCOUNT_UNLOCKED, false
    );

    // when
    eventConsumer.handle(event);

    // then
    verifyNoInteractions(processedEventRepository, notificationCreator);
  }

  @Test
  @DisplayName("이미 처리된 이벤트는 알림을 생성하지 않고 스킵한다")
  void handle_success_already_processed_skip() {
    // given
    UUID eventId = UUID.randomUUID();
    UserAccountStatusChangedEvent event = new UserAccountStatusChangedEvent(
        eventId, Instant.now(), userId, ForceLogoutReason.ROLE_CHANGE, true
    );

    given(processedEventRepository.insertIfAbsent(eventId)).willReturn(0);

    // when
    eventConsumer.handle(event);

    // then
    verify(processedEventRepository).insertIfAbsent(eventId);
    verifyNoInteractions(notificationCreator);
  }
}
