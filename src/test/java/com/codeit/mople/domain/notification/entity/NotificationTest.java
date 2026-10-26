package com.codeit.mople.domain.notification.entity;

import java.time.Instant;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class NotificationTest {

  private UUID receiverId;
  private NotificationTemplate template;

  @BeforeEach
  void setUp() {
    receiverId = UUID.randomUUID();
    template = new NotificationTemplate("제목", "내용");
  }

  @Test
  @DisplayName("Notification 생성 시 전달한 필드값이 저장되고 읽지 않은 상태로 시작한다")
  void create_success() {
    Notification notification = Notification.create(receiverId, template,
        NotificationType.FOLLOW_CREATED, "hashId");

    assertThat(notification.getReceiverId()).isEqualTo(receiverId);
    assertThat(notification.getTemplate()).isEqualTo(template);
    assertThat(notification.getNotificationType()).isEqualTo(NotificationType.FOLLOW_CREATED);
    assertThat(notification.getHashId()).isEqualTo("hashId");
    assertThat(notification.getReadAt()).isNull();
  }

  @Test
  @DisplayName("템플릿의 content가 null이어도 Notification 생성에 성공한다")
  void create_success_withNullContent() {
    Notification notification = Notification.create(receiverId,
        new NotificationTemplate("제목", null), NotificationType.FOLLOW_CREATED, "hashId");

    assertThat(notification.getTemplate().content()).isNull();
  }

  @Test
  @DisplayName("ROLE_CHANGED 타입은 level이 WARNING이다")
  void create_levelIsWarning_whenWarningType() {
    Notification notification = Notification.create(receiverId, template,
        NotificationType.ROLE_CHANGED, "hashId");

    assertThat(notification.getLevel()).isEqualTo(NotificationLevel.WARNING);
  }

  @Test
  @DisplayName("ROLE_CHANGED 외 타입은 level이 INFO이다")
  void create_levelIsInfo_whenInfoType() {
    for (NotificationType type : NotificationType.values()) {
      if (type == NotificationType.ROLE_CHANGED) {
        continue;
      }

      Notification notification = Notification.create(receiverId, template, type, "hashId");

      assertThat(notification.getLevel())
          .as("NotificationType=%s 일 때 level은 INFO여야 한다", type)
          .isEqualTo(NotificationLevel.INFO);
    }
  }

  @Test
  @DisplayName("receiverId가 null이면 예외가 발생한다")
  void create_throwsException_whenReceiverIdIsNull() {
    assertThatThrownBy(
        () -> Notification.create(null, template, NotificationType.FOLLOW_CREATED, "hashId"))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("receiverId");
  }

  @Test
  @DisplayName("template이 null이면 예외가 발생한다")
  void create_throwsException_whenTemplateIsNull() {
    assertThatThrownBy(
        () -> Notification.create(receiverId, null, NotificationType.FOLLOW_CREATED, "hashId"))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("template");
  }

  @Test
  @DisplayName("템플릿의 title이 null이면 예외가 발생한다")
  void createTemplate_throwsException_whenTitleIsNull() {
    assertThatThrownBy(() -> new NotificationTemplate(null, "내용"))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("title");
  }

  @Test
  @DisplayName("notificationType이 null이면 예외가 발생한다")
  void create_throwsException_whenNotificationTypeIsNull() {
    assertThatThrownBy(() -> Notification.create(receiverId, template, null, "hashId"))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("notificationType");
  }

  @Test
  @DisplayName("hashId가 null이면 예외가 발생한다")
  void create_throwsException_whenHashIdIsNull() {
    assertThatThrownBy(
        () -> Notification.create(receiverId, template, NotificationType.FOLLOW_CREATED, null))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("hashId");
  }

  @Test
  @DisplayName("읽음 처리하면 readAt이 기록된다")
  void markAsRead_success() {
    Notification notification = Notification.create(receiverId, template,
        NotificationType.FOLLOW_CREATED, "hashId");

    notification.markAsRead();

    assertThat(notification.getReadAt()).isNotNull();
  }

  @Test
  @DisplayName("이미 읽은 알림을 다시 읽음 처리해도 처음 기록한 readAt이 유지된다")
  void markAsRead_keepsFirstReadAt_whenAlreadyRead() {
    Notification notification = Notification.create(receiverId, template,
        NotificationType.FOLLOW_CREATED, "hashId");
    notification.markAsRead();
    Instant firstReadAt = notification.getReadAt();

    notification.markAsRead();

    assertThat(notification.getReadAt()).isSameAs(firstReadAt);
  }

  @Test
  @DisplayName("수신자, 타입, 대상이 같으면 hashId가 같다")
  void hashIdOf_returnsSameHash_whenSameInputs() {
    String first = Notification.hashIdOf(receiverId, NotificationType.FOLLOW_CREATED, "target");
    String second = Notification.hashIdOf(receiverId, NotificationType.FOLLOW_CREATED, "target");

    assertThat(first).isEqualTo(second);
  }

  @Test
  @DisplayName("수신자, 타입, 대상 중 하나라도 다르면 hashId가 다르다")
  void hashIdOf_returnsDifferentHash_whenAnyInputDiffers() {
    String hashId = Notification.hashIdOf(receiverId, NotificationType.FOLLOW_CREATED, "target");

    String otherReceiver =
        Notification.hashIdOf(UUID.randomUUID(), NotificationType.FOLLOW_CREATED, "target");
    String otherType =
        Notification.hashIdOf(receiverId, NotificationType.DIRECT_MESSAGE_RECEIVED, "target");
    String otherTarget =
        Notification.hashIdOf(receiverId, NotificationType.FOLLOW_CREATED, "other");

    assertThat(hashId).isNotEqualTo(otherReceiver);
    assertThat(hashId).isNotEqualTo(otherType);
    assertThat(hashId).isNotEqualTo(otherTarget);
  }
}
