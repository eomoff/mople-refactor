package com.codeit.mople.domain.notification.entity;

import com.codeit.mople.global.entity.BaseEntity;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Table;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.HexFormat;
import java.util.Objects;
import java.util.UUID;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

@Entity
@Table(name = "notifications")
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class Notification extends BaseEntity {

  @Column(nullable = false)
  private UUID receiverId;

  @JdbcTypeCode(SqlTypes.JSON)
  @Column(nullable = false)
  private NotificationTemplate template;

  @Enumerated(EnumType.STRING)
  @Column(nullable = false)
  private NotificationType notificationType;

  @Enumerated(EnumType.STRING)
  @Column(nullable = false)
  private NotificationLevel level;

  @Column(nullable = false, unique = true)
  private String hashId;

  @Column
  private Instant readAt;

  private Notification(
      UUID receiverId,
      NotificationTemplate template,
      NotificationType notificationType,
      String hashId
  ) {
    this.receiverId = Objects.requireNonNull(receiverId, "receiverId");
    this.template = Objects.requireNonNull(template, "template");
    this.notificationType = Objects.requireNonNull(notificationType, "notificationType");
    this.level = notificationType.getLevel();
    this.hashId = Objects.requireNonNull(hashId, "hashId");
  }

  public static Notification create(
      UUID receiverId,
      NotificationTemplate template,
      NotificationType notificationType,
      String hashId
  ) {
    return new Notification(receiverId, template, notificationType, hashId);
  }

  public void markAsRead() {
    if (readAt == null) {
      readAt = Instant.now();
    }
  }

  public static String hashIdOf(
      UUID receiverId, NotificationType notificationType, String targetKey) {
    try {
      byte[] digest = MessageDigest.getInstance("SHA-256")
          .digest((receiverId + ":" + notificationType + ":" + targetKey).getBytes(StandardCharsets.UTF_8));
      return HexFormat.of().formatHex(digest);
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException(e);
    }
  }
}