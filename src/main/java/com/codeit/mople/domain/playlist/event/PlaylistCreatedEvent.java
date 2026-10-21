package com.codeit.mople.domain.playlist.event;

import java.util.UUID;

public record PlaylistCreatedEvent(
    UUID playlistId,
    UUID ownerId,
    String ownerName,
    String playlistTitle
) {

}
