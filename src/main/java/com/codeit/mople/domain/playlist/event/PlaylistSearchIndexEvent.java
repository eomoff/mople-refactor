package com.codeit.mople.domain.playlist.event;

import java.time.Instant;
import java.util.UUID;

public record PlaylistSearchIndexEvent(
    UUID eventId,
    Instant occurredAt,
    UUID playlistId,
    String title,
    Instant updatedAt,
    long subscribeCount
) {

}