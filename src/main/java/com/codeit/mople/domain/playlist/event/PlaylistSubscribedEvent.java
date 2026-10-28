package com.codeit.mople.domain.playlist.event;

import java.time.Instant;
import java.util.UUID;

public record PlaylistSubscribedEvent(
    UUID eventId,
    Instant occurredAt,
    UUID ownerId,
    UUID playlistId,
    UUID subscriberId,
    String subscriberName,
    String playlistTitle
) {

}
