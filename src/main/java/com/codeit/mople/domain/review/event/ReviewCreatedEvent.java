package com.codeit.mople.domain.review.event;

import java.time.Instant;
import java.util.UUID;

public record ReviewCreatedEvent(
    UUID eventId,
    Instant occurredAt,
    UUID contentId,
    double rating
) {

}
