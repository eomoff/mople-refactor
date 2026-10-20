package com.codeit.mople.domain.directmessage.event;

import com.codeit.mople.global.event.PublishableEvent;
import java.time.Instant;
import java.util.UUID;

public record DirectMessageCreatedEvent(
    UUID eventId,
    Instant occurredAt,
    UUID receiverId,
    UUID directMessageId
) implements PublishableEvent {

}
