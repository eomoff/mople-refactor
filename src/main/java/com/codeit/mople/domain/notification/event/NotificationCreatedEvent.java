package com.codeit.mople.domain.notification.event;

import com.codeit.mople.global.event.PublishableEvent;
import java.time.Instant;
import java.util.UUID;

public record NotificationCreatedEvent(
    UUID eventId,
    Instant occurredAt,
    UUID receiverId,
    UUID notificationId
) implements PublishableEvent {

}
