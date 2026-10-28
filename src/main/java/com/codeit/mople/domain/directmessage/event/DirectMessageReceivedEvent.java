package com.codeit.mople.domain.directmessage.event;

import java.time.Instant;
import java.util.UUID;

public record DirectMessageReceivedEvent(
    UUID eventId,
    Instant occurredAt,
    UUID directMessageId,
    UUID receiverId,
    String senderName,
    String messageContent
) {

}
