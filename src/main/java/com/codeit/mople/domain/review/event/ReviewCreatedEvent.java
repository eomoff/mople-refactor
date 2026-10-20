package com.codeit.mople.domain.review.event;

import com.codeit.mople.global.event.PublishableEvent;
import java.time.Instant;
import java.util.UUID;

public record ReviewCreatedEvent(
    UUID eventId,
    Instant occurredAt,
    UUID contentId,
    double rating
) implements PublishableEvent {

}
