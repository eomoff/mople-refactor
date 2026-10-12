package com.codeit.mople.domain.notification.entity;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

@Getter
@RequiredArgsConstructor
public enum NotificationType {
  ROLE_CHANGED(NotificationLevel.WARNING),
  PLAYLIST_SUBSCRIBED(NotificationLevel.INFO),
  PLAYLIST_CONTENT_ADDED(NotificationLevel.INFO),
  FOLLOWEE_ACTIVITY(NotificationLevel.INFO),
  FOLLOW_CREATED(NotificationLevel.INFO),
  DIRECT_MESSAGE_RECEIVED(NotificationLevel.INFO);

  private final NotificationLevel level;
}
