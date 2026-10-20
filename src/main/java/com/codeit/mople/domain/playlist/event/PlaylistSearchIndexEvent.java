package com.codeit.mople.domain.playlist.event;

import com.codeit.mople.global.event.PublishableEvent;
import java.time.Instant;
import java.util.UUID;

public record PlaylistSearchIndexEvent(
    UUID eventId,
    Instant occurredAt,
    UUID playlistId,
    String title,
    Instant updatedAt,
    long subscribeCount
) implements PublishableEvent {

}