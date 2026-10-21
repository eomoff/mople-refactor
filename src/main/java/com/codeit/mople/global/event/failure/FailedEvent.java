package com.codeit.mople.global.event.failure;

public record FailedEvent(
    String topic,
    String key,
    String eventType,
    String data,
    String error
) {

  public static FailedEvent of(
      String topic,
      String key,
      Object event,
      String data,
      Throwable cause
  ) {
    return new FailedEvent(
        topic,
        key == null ? "" : key,
        event.getClass().getName(),
        data == null ? "" : data,
        cause == null ? "" : String.valueOf(cause.getMessage())
    );
  }
}