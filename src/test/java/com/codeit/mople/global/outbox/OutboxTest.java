package com.codeit.mople.global.outbox;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.UUID;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class OutboxTest {

  private static final UUID EVENT_ID = UUID.randomUUID();
  private static final String AGGREGATE_TYPE = "follow";
  private static final String AGGREGATE_ID = "follow-id";
  private static final String EVENT_TYPE = "FollowCreatedEvent";
  private static final String PAYLOAD = "{}";

  private Outbox create() {
    return Outbox.create(EVENT_ID, AGGREGATE_TYPE, AGGREGATE_ID, EVENT_TYPE, PAYLOAD);
  }

  @Test
  @DisplayName("Outbox 생성 시 전달한 필드값이 저장되고 미발행 상태로 시작한다")
  void create_success() {
    Outbox outbox = create();

    assertThat(outbox.getEventId()).isEqualTo(EVENT_ID);
    assertThat(outbox.getAggregateType()).isEqualTo(AGGREGATE_TYPE);
    assertThat(outbox.getAggregateId()).isEqualTo(AGGREGATE_ID);
    assertThat(outbox.getEventType()).isEqualTo(EVENT_TYPE);
    assertThat(outbox.getPayload()).isEqualTo(PAYLOAD);
    assertThat(outbox.getStatus()).isEqualTo(OutboxStatus.PENDING);
    assertThat(outbox.getRetryCount()).isZero();
    assertThat(outbox.getSentAt()).isNull();
  }

  @Test
  @DisplayName("eventId가 null이면 예외가 발생한다")
  void create_throwsException_whenEventIdIsNull() {
    assertThatThrownBy(
        () -> Outbox.create(null, AGGREGATE_TYPE, AGGREGATE_ID, EVENT_TYPE, PAYLOAD))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("eventId");
  }

  @Test
  @DisplayName("aggregateType이 null이면 예외가 발생한다")
  void create_throwsException_whenAggregateTypeIsNull() {
    assertThatThrownBy(() -> Outbox.create(EVENT_ID, null, AGGREGATE_ID, EVENT_TYPE, PAYLOAD))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("aggregateType");
  }

  @Test
  @DisplayName("aggregateId가 null이면 예외가 발생한다")
  void create_throwsException_whenAggregateIdIsNull() {
    assertThatThrownBy(() -> Outbox.create(EVENT_ID, AGGREGATE_TYPE, null, EVENT_TYPE, PAYLOAD))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("aggregateId");
  }

  @Test
  @DisplayName("eventType이 null이면 예외가 발생한다")
  void create_throwsException_whenEventTypeIsNull() {
    assertThatThrownBy(
        () -> Outbox.create(EVENT_ID, AGGREGATE_TYPE, AGGREGATE_ID, null, PAYLOAD))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("eventType");
  }

  @Test
  @DisplayName("payload가 null이면 예외가 발생한다")
  void create_throwsException_whenPayloadIsNull() {
    assertThatThrownBy(
        () -> Outbox.create(EVENT_ID, AGGREGATE_TYPE, AGGREGATE_ID, EVENT_TYPE, null))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("payload");
  }

  @Test
  @DisplayName("발행 완료 처리하면 SENT 상태가 되고 발행 시각이 기록된다")
  void markSent_success() {
    Outbox outbox = create();

    outbox.markSent();

    assertThat(outbox.getStatus()).isEqualTo(OutboxStatus.SENT);
    assertThat(outbox.getSentAt()).isNotNull();
  }

  @Test
  @DisplayName("재시도 횟수를 올리면 1씩 늘고 미발행 상태는 그대로다")
  void increaseRetryCount_success() {
    Outbox outbox = create();

    outbox.increaseRetryCount();
    outbox.increaseRetryCount();

    assertThat(outbox.getRetryCount()).isEqualTo(2);
    assertThat(outbox.getStatus()).isEqualTo(OutboxStatus.PENDING);
  }
}
