package com.codeit.mople.domain.content.event;

import java.time.Instant;
import java.util.UUID;

public record ContentSearchIndexDeleteEvent(
    UUID eventId,
    Instant occurredAt,
    UUID contentId
) {

}