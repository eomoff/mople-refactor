package com.codeit.mople.domain.user.event;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

import com.codeit.mople.domain.notification.entity.NotificationTemplate;
import com.codeit.mople.domain.notification.entity.NotificationType;
import com.codeit.mople.domain.notification.service.NotificationCreator;
import com.codeit.mople.domain.user.admin.enums.ForceLogoutReason;
import com.codeit.mople.domain.user.admin.event.UserAccountStatusChangedEvent;
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

  @InjectMocks
  private UserEventConsumer eventConsumer;

  private UUID userId;

  @BeforeEach
  void setUp() {
    userId = UUID.randomUUID();
  }

  @Test
  @DisplayName("권한 변경 이벤트 수신 시 이벤트 ID를 대상 키로 ROLE_CHANGED 알림을 생성한다")
  void handle_role_change() {
    // given
    UUID eventId = UUID.randomUUID();
    UserAccountStatusChangedEvent event = new UserAccountStatusChangedEvent(
        eventId, Instant.now(), userId, ForceLogoutReason.ROLE_CHANGE, true
    );

    // when
    eventConsumer.handle(event);

    // then
    verify(notificationCreator).createNotification(
        userId,
        new NotificationTemplate(
            "권한이 변경되었습니다.",
            "관리자에 의해 권한이 변경되었습니다. 다시 로그인해주세요."
        ),
        NotificationType.ROLE_CHANGED,
        eventId.toString()
    );
  }

  @Test
  @DisplayName("같은 권한 변경 이벤트가 다시 와도 같은 대상 키로 알림을 요청한다")
  void handle_role_change_redelivered() {
    // given
    UUID eventId = UUID.randomUUID();
    UserAccountStatusChangedEvent event = new UserAccountStatusChangedEvent(
        eventId, Instant.now(), userId, ForceLogoutReason.ROLE_CHANGE, true
    );

    // when
    eventConsumer.handle(event);
    eventConsumer.handle(event);

    // then
    verify(notificationCreator, times(2)).createNotification(
        userId,
        new NotificationTemplate(
            "권한이 변경되었습니다.",
            "관리자에 의해 권한이 변경되었습니다. 다시 로그인해주세요."
        ),
        NotificationType.ROLE_CHANGED,
        eventId.toString()
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
    verifyNoInteractions(notificationCreator);
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
    verifyNoInteractions(notificationCreator);
  }
}
