package com.codeit.mople.global.event.failure;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
@DisplayName("KafkaPublishFailureListener 테스트")
class KafkaPublishFailureListenerTest {

  static final String TOPIC = "mople.follow.created.v1";
  static final String KEY = "followee-key";

  record TestEvent(UUID eventId, Instant occurredAt) {

  }

  @Mock
  FailedEventStore failedEventStore;
  @Mock
  ObjectMapper objectMapper;

  @Captor
  ArgumentCaptor<FailedEvent> failedEventCaptor;

  TestEvent event;
  ProducerRecord<Object, Object> record;
  MeterRegistry meterRegistry;
  KafkaPublishFailureListener listener;

  final Executor sameThreadExecutor = Runnable::run;
  final Executor neverRunsExecutor = command -> {
  };

  @BeforeEach
  void setUp() {
    event = new TestEvent(UUID.randomUUID(), Instant.now());
    record = new ProducerRecord<>(TOPIC, KEY, event);
    meterRegistry = new SimpleMeterRegistry();
    listener = new KafkaPublishFailureListener(
        failedEventStore, objectMapper, meterRegistry, sameThreadExecutor);
  }

  @Nested
  @DisplayName("발행 실패 기록")
  class RecordFailure {

    @Test
    @DisplayName("발행에 실패하면 토픽과 키와 이벤트 정보를 담아서 잘 쌓는지")
    void recordsFailure() throws Exception {
      // given
      given(objectMapper.writeValueAsString(event)).willReturn("{\"eventId\":\"test\"}");

      // when
      listener.onError(record, null, new RuntimeException("broker down"));

      // then
      verify(failedEventStore).save(failedEventCaptor.capture());

      FailedEvent saved = failedEventCaptor.getValue();
      assertThat(saved.topic()).isEqualTo(TOPIC);
      assertThat(saved.key()).isEqualTo(KEY);
      assertThat(saved.eventType()).isEqualTo(TestEvent.class.getName());
      assertThat(saved.data()).isEqualTo("{\"eventId\":\"test\"}");
      assertThat(saved.error()).isEqualTo("broker down");
    }

    @Test
    @DisplayName("본문 직렬화가 깨져도 본문만 비운 채로 실패 이벤트를 쌓는지")
    void recordsFailureWhenSerializeFails() throws Exception {
      // given
      given(objectMapper.writeValueAsString(event))
          .willThrow(new JsonProcessingException("직렬화 실패") {
          });

      // when
      listener.onError(record, null, new RuntimeException("broker down"));

      // then
      verify(failedEventStore).save(failedEventCaptor.capture());

      FailedEvent saved = failedEventCaptor.getValue();
      assertThat(saved.data()).isEmpty();
      assertThat(saved.eventType()).isEqualTo(TestEvent.class.getName());
    }

    @Test
    @DisplayName("키 없이 보낸 이벤트가 실패하면 키를 빈 값으로 쌓는지")
    void recordsFailureWithoutKey() throws Exception {
      // given
      given(objectMapper.writeValueAsString(event)).willReturn("{}");

      // when
      listener.onError(new ProducerRecord<>(TOPIC, event), null,
          new RuntimeException("broker down"));

      // then
      verify(failedEventStore).save(failedEventCaptor.capture());
      assertThat(failedEventCaptor.getValue().key()).isEmpty();
    }

    @Test
    @DisplayName("실패 처리를 executor 에 한 번만 제출하는지")
    void submitsFailureOnce() throws Exception {
      // given
      AtomicInteger submitted = new AtomicInteger();
      KafkaPublishFailureListener counting = new KafkaPublishFailureListener(
          failedEventStore, objectMapper, meterRegistry,
          command -> {
            submitted.incrementAndGet();
            command.run();
          });
      given(objectMapper.writeValueAsString(event)).willReturn("{}");

      // when
      counting.onError(record, null, new RuntimeException("broker down"));

      // then
      assertThat(submitted).hasValue(1);
      verify(failedEventStore).save(any());
    }

    @Test
    @DisplayName("실패 처리가 호출 스레드가 아니라 전용 executor를 거쳐서 도는지")
    void handlesFailureThroughExecutor() {
      // given
      KafkaPublishFailureListener deferred = new KafkaPublishFailureListener(
          failedEventStore, objectMapper, meterRegistry, neverRunsExecutor);

      // when
      deferred.onError(record, null, new RuntimeException("broker down"));

      // then
      verify(failedEventStore, never()).save(any());
      assertThat(meterRegistry.find("kafka.event.publish.failure").counter()).isNull();
    }
  }

  @Nested
  @DisplayName("발행 실패 집계")
  class FailureMetrics {

    @Test
    @DisplayName("발행에 실패하면 토픽과 실패 원인을 태그로 달아 세는지")
    void countsFailureByTopicAndReason() throws Exception {
      // given
      given(objectMapper.writeValueAsString(event)).willReturn("{}");

      // when
      listener.onError(record, null, new IllegalStateException("broker down"));

      // then
      assertThat(meterRegistry.get("kafka.event.publish.failure")
          .tag("topic", TOPIC)
          .tag("reason", "IllegalStateException")
          .counter().count()).isEqualTo(1.0);
    }
  }
}
