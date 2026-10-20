package com.codeit.mople.domain.content.event;

import com.codeit.mople.domain.content.entity.ContentType;
import com.codeit.mople.global.event.PublishableEvent;
import java.time.Instant;
import java.util.UUID;

public record ContentSearchIndexEvent(
    UUID eventId,
    Instant occurredAt,
    UUID contentId,
    String title,
    ContentType type,
    double rating,
    long watcherCount,
    Instant createdAt
) implements PublishableEvent {

}