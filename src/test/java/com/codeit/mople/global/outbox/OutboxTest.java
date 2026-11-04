package com.codeit.mople.global.outbox;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class OutboxTest {

  private static final String AGGREGATE_TYPE = "follow";
  private static final String AGGREGATE_ID = "follow-id";
  private static final String EVENT_TYPE = "FollowCreatedEvent";
  private static final String PAYLOAD = "{}";

  @Test
  @DisplayName("Outbox 생성 시 전달한 필드값이 저장되고 미발행 상태로 시작한다")
  void create_success() {
    Outbox outbox = Outbox.create(AGGREGATE_TYPE, AGGREGATE_ID, EVENT_TYPE, PAYLOAD);

    assertThat(outbox.getAggregateType()).isEqualTo(AGGREGATE_TYPE);
    assertThat(outbox.getAggregateId()).isEqualTo(AGGREGATE_ID);
    assertThat(outbox.getEventType()).isEqualTo(EVENT_TYPE);
    assertThat(outbox.getPayload()).isEqualTo(PAYLOAD);
    assertThat(outbox.getStatus()).isEqualTo(OutboxStatus.PENDING);
    assertThat(outbox.getRetryCount()).isZero();
    assertThat(outbox.getSentAt()).isNull();
  }

  @Test
  @DisplayName("aggregateType이 null이면 예외가 발생한다")
  void create_throwsException_whenAggregateTypeIsNull() {
    assertThatThrownBy(() -> Outbox.create(null, AGGREGATE_ID, EVENT_TYPE, PAYLOAD))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("aggregateType");
  }

  @Test
  @DisplayName("aggregateId가 null이면 예외가 발생한다")
  void create_throwsException_whenAggregateIdIsNull() {
    assertThatThrownBy(() -> Outbox.create(AGGREGATE_TYPE, null, EVENT_TYPE, PAYLOAD))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("aggregateId");
  }

  @Test
  @DisplayName("eventType이 null이면 예외가 발생한다")
  void create_throwsException_whenEventTypeIsNull() {
    assertThatThrownBy(() -> Outbox.create(AGGREGATE_TYPE, AGGREGATE_ID, null, PAYLOAD))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("eventType");
  }

  @Test
  @DisplayName("payload가 null이면 예외가 발생한다")
  void create_throwsException_whenPayloadIsNull() {
    assertThatThrownBy(() -> Outbox.create(AGGREGATE_TYPE, AGGREGATE_ID, EVENT_TYPE, null))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("payload");
  }
}
