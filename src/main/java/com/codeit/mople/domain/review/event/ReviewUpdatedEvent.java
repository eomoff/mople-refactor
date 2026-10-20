package com.codeit.mople.domain.review.event;

import com.codeit.mople.global.event.PublishableEvent;
import java.time.Instant;
import java.util.UUID;

public record ReviewUpdatedEvent(
    UUID eventId,
    Instant occurredAt,
    UUID contentId,
    double oldRating,
    double newRating
) implements PublishableEvent {

}
