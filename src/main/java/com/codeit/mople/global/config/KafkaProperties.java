package com.codeit.mople.global.config;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

@Validated
@ConfigurationProperties(prefix = KafkaProperties.PREFIX)
public record KafkaProperties(
    boolean enabled,
    String bootstrapServers,
    @NotNull @Valid Topics topics
) {

  public static final String PREFIX = "spring.kafka";

  public record Topics(
      @NotBlank String followCreated,
      @NotBlank String playlistContentAdded,
      @NotBlank String directMessageCreated,
      @NotBlank String notificationCreated,
      @NotBlank String contentSearchIndex,
      @NotBlank String userSearchIndex,
      @NotBlank String playlistSearchIndex,
      @NotBlank String userAccountStatusChanged,
      @NotBlank String playlistSubscribed,
      @NotBlank String playlistCreated,
      @NotBlank String directMessageReceived,
      @NotBlank String reviewWritten
  ) {

  }
}
