package com.codeit.mople.domain.notification.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import com.codeit.mople.domain.notification.entity.Notification;
import com.codeit.mople.domain.notification.entity.NotificationTemplate;
import com.codeit.mople.domain.notification.entity.NotificationType;
import com.codeit.mople.domain.notification.event.NotificationCreatedEvent;
import com.codeit.mople.domain.notification.exception.NotificationErrorCode;
import com.codeit.mople.domain.notification.exception.NotificationException;
import com.codeit.mople.domain.notification.repository.NotificationRepository;
import com.codeit.mople.domain.user.repository.UserRepository;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.context.ApplicationEventPublisher;

@ExtendWith(MockitoExtension.class)
class NotificationCreatorTest {

  @Mock
  private NotificationRepository notificationRepository;

  @Mock
  private UserRepository userRepository;

  @Mock
  private ApplicationEventPublisher eventPublisher;

  @InjectMocks
  private NotificationCreator notificationCreator;

  private UUID receiverId;
  private NotificationTemplate template;
  private String hashId;

  @BeforeEach
  void setUp() {
    receiverId = UUID.randomUUID();
    template = new NotificationTemplate("새로운 메시지가 도착했습니다.", "발신자: 안녕하세요!");
    hashId = Notification.hashIdOf(receiverId, NotificationType.DIRECT_MESSAGE_RECEIVED, "dm-id");
  }

  @Nested
  @DisplayName("알림 생성")
  class CreateNotification {

    @Test
    @DisplayName("알림 생성 성공")
    void createNotification_success() {
      // given
      UUID notificationId = UUID.randomUUID();
      Notification saved = mock(Notification.class);

      given(userRepository.existsById(receiverId))
          .willReturn(true);

      given(notificationRepository.existsByHashId(hashId))
          .willReturn(false);

      given(notificationRepository.save(any(Notification.class)))
          .willReturn(saved);

      given(saved.getId())
          .willReturn(notificationId);

      // when
      notificationCreator.createNotification(
          receiverId,
          template,
          NotificationType.DIRECT_MESSAGE_RECEIVED,
          "dm-id"
      );

      // then
      ArgumentCaptor<Notification> notificationCaptor = ArgumentCaptor.forClass(Notification.class);
      verify(notificationRepository)
          .save(notificationCaptor.capture());

      Notification notification = notificationCaptor.getValue();
      assertThat(notification.getReceiverId()).isEqualTo(receiverId);
      assertThat(notification.getTemplate()).isEqualTo(template);
      assertThat(notification.getNotificationType())
          .isEqualTo(NotificationType.DIRECT_MESSAGE_RECEIVED);
      assertThat(notification.getHashId()).isEqualTo(hashId);

      ArgumentCaptor<NotificationCreatedEvent> eventCaptor =
          ArgumentCaptor.forClass(NotificationCreatedEvent.class);
      verify(eventPublisher)
          .publishEvent(eventCaptor.capture());

      assertThat(eventCaptor.getValue().receiverId()).isEqualTo(receiverId);
      assertThat(eventCaptor.getValue().notificationId()).isEqualTo(notificationId);
    }

    @Test
    @DisplayName("같은 hashId의 알림이 이미 있으면 저장하지 않고 이벤트도 발행하지 않는다")
    void createNotification_skipsSave_whenHashIdExists() {
      // given
      given(userRepository.existsById(receiverId))
          .willReturn(true);

      given(notificationRepository.existsByHashId(hashId))
          .willReturn(true);

      // when
      notificationCreator.createNotification(
          receiverId,
          template,
          NotificationType.DIRECT_MESSAGE_RECEIVED,
          "dm-id"
      );

      // then
      verify(notificationRepository, never())
          .save(any(Notification.class));

      verify(eventPublisher, never())
          .publishEvent(any(Object.class));
    }

    @Test
    @DisplayName("수신자가 존재하지 않으면 NOTIFICATION_USER_NOT_FOUND 예외가 발생하고 저장하지 않는다")
    void createNotification_throwsException_whenReceiverNotFound() {
      // given
      given(userRepository.existsById(receiverId))
          .willReturn(false);

      // when & then
      assertThatThrownBy(() -> notificationCreator.createNotification(
          receiverId,
          template,
          NotificationType.DIRECT_MESSAGE_RECEIVED,
          "dm-id"
      ))
          .isInstanceOf(NotificationException.class)
          .satisfies(e -> assertThat(((NotificationException) e).getErrorCode())
              .isEqualTo(NotificationErrorCode.NOTIFICATION_USER_NOT_FOUND));

      verify(notificationRepository, never())
          .save(any(Notification.class));

      verify(eventPublisher, never())
          .publishEvent(any(Object.class));
    }
  }
}
