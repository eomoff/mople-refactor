package com.codeit.mople.domain.playlist.event;

import java.time.Instant;
import java.util.UUID;

public record PlaylistSearchIndexDeleteEvent(
    UUID eventId,
    Instant occurredAt,
    UUID playlistId
) {

}
