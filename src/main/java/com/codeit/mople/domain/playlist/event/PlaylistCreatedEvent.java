package com.codeit.mople.domain.playlist.event;

import java.time.Instant;
import java.util.UUID;

public record PlaylistCreatedEvent(
    UUID eventId,
    Instant occurredAt,
    UUID playlistId,
    UUID ownerId,
    String ownerName,
    String playlistTitle
) {

}
