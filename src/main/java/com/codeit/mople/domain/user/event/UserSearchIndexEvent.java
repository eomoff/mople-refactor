package com.codeit.mople.domain.user.event;

import com.codeit.mople.global.event.PublishableEvent;
import java.time.Instant;
import java.util.UUID;

public record UserSearchIndexEvent(
    UUID eventId,
    Instant occurredAt,
    UUID userId,
    String email,
    String name,
    Instant createdAt,
    boolean locked,
    String role
) implements PublishableEvent {

}