package com.codeit.mople.domain.notification.event;

import java.time.Instant;
import java.util.UUID;

public record NotificationCreatedEvent(
    UUID eventId,
    Instant occurredAt,
    UUID receiverId,
    UUID notificationId
) {

}
