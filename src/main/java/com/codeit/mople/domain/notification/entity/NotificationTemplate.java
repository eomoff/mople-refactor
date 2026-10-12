package com.codeit.mople.domain.notification.entity;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.Objects;

@JsonIgnoreProperties(ignoreUnknown = true)
public record NotificationTemplate(
    String title,
    String content
) {
  public NotificationTemplate {
    Objects.requireNonNull(title, "title");
  }

}
