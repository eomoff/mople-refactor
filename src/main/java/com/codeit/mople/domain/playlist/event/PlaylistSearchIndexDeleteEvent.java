package com.codeit.mople.domain.playlist.event;

import com.codeit.mople.global.event.PublishableEvent;
import java.time.Instant;
import java.util.UUID;

public record PlaylistSearchIndexDeleteEvent(
    UUID eventId,
    Instant occurredAt,
    UUID playlistId
) implements PublishableEvent {

}
