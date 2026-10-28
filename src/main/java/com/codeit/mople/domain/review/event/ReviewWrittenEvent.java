package com.codeit.mople.domain.review.event;

import java.time.Instant;
import java.util.UUID;

public record ReviewWrittenEvent(
    UUID eventId,
    Instant occurredAt,
    UUID reviewId,
    UUID authorId,
    String authorName
) {

}
