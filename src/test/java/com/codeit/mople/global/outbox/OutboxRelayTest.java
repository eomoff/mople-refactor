package com.codeit.mople.global.outbox;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.codeit.mople.global.outbox.repository.OutboxRepository;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.KafkaException;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

@ExtendWith(MockitoExtension.class)
@DisplayName("outbox relay 테스트")
class OutboxRelayTest {

  static final String TOPIC = "mople.follow.created.v1";

  record SampleEvent(UUID eventId, String name) {

  }

  final ObjectMapper objectMapper = new ObjectMapper();

  @Mock
  OutboxRepository outboxRepository;

  @Mock
  KafkaTemplate<Object, Object> kafkaTemplate;

  @Captor
  ArgumentCaptor<ProducerRecord<Object, Object>> recordCaptor;

  OutboxRelay relay;

  @BeforeEach
  void setUp() {
    relay = new OutboxRelay(outboxRepository, kafkaTemplate, objectMapper);
  }

  private Outbox pendingOutbox(String key) throws JsonProcessingException {
    SampleEvent event = new SampleEvent(UUID.randomUUID(), "이름");
    return Outbox.create(event.eventId(), TOPIC, key, SampleEvent.class.getName(),
        objectMapper.writeValueAsString(event));
  }

  private void givenPending(Outbox... outboxes) {
    given(outboxRepository.findPendingForUpdate(OutboxRelay.BATCH_SIZE))
        .willReturn(List.of(outboxes));
  }

  @SuppressWarnings("unchecked")
  private void givenSendResult(CompletableFuture<SendResult<Object, Object>> result) {
    given(kafkaTemplate.send(any(ProducerRecord.class))).willReturn(result);
  }

  @Nested
  @DisplayName("발행 성공")
  class Success {

    @Test
    @DisplayName("aggregate_type을 토픽, aggregate_id를 키로 원래 이벤트를 보내는지")
    void sendsEventToTopicWithKey() throws Exception {
      // given
      Outbox outbox = pendingOutbox("followee-id");
      givenPending(outbox);
      givenSendResult(CompletableFuture.completedFuture(null));

      // when
      relay.relay();

      // then
      verify(kafkaTemplate).send(recordCaptor.capture());

      ProducerRecord<Object, Object> record = recordCaptor.getValue();
      assertThat(record.topic()).isEqualTo(TOPIC);
      assertThat(record.key()).isEqualTo("followee-id");
      assertThat(record.value())
          .isEqualTo(objectMapper.readValue(outbox.getPayload(), SampleEvent.class));
    }

    @Test
    @DisplayName("헤더에 eventId를 싣는지")
    void carriesEventIdHeader() throws Exception {
      // given
      Outbox outbox = pendingOutbox("followee-id");
      givenPending(outbox);
      givenSendResult(CompletableFuture.completedFuture(null));

      // when
      relay.relay();

      // then
      verify(kafkaTemplate).send(recordCaptor.capture());

      Header header = recordCaptor.getValue().headers().lastHeader(OutboxRelay.EVENT_ID_HEADER);
      assertThat(new String(header.value(), StandardCharsets.UTF_8))
          .isEqualTo(outbox.getEventId().toString());
    }

    @Test
    @DisplayName("ack를 받으면 SENT로 바꾸고 발행 시각을 남기는지")
    void marksSentAfterAck() throws Exception {
      // given
      Outbox outbox = pendingOutbox("followee-id");
      givenPending(outbox);
      givenSendResult(CompletableFuture.completedFuture(null));

      // when
      relay.relay();

      // then
      assertThat(outbox.getStatus()).isEqualTo(OutboxStatus.SENT);
      assertThat(outbox.getSentAt()).isNotNull();
      assertThat(outbox.getRetryCount()).isZero();
    }
  }

  @Nested
  @DisplayName("발행 실패")
  class Failure {

    @Test
    @DisplayName("ack가 실패하면 PENDING으로 두고 retry_count를 올린 뒤 남은 행은 보내지 않는지")
    void keepsPendingAndStopsWhenAckFails() throws Exception {
      // given
      Outbox first = pendingOutbox("first");
      Outbox second = pendingOutbox("second");
      givenPending(first, second);
      givenSendResult(CompletableFuture.failedFuture(new RuntimeException("broker down")));

      // when
      relay.relay();

      // then
      verify(kafkaTemplate, times(1)).send(any(ProducerRecord.class));

      assertThat(first.getStatus()).isEqualTo(OutboxStatus.PENDING);
      assertThat(first.getRetryCount()).isEqualTo(1);
      assertThat(second.getStatus()).isEqualTo(OutboxStatus.PENDING);
      assertThat(second.getRetryCount()).isZero();
    }

    @Test
    @DisplayName("send 호출이 바로 예외를 던져도 PENDING으로 두고 retry_count를 올리는지")
    void keepsPendingWhenSendThrows() throws Exception {
      // given
      Outbox outbox = pendingOutbox("followee-id");
      givenPending(outbox);
      given(kafkaTemplate.send(any(ProducerRecord.class)))
          .willThrow(new KafkaException("Send failed"));

      // when
      relay.relay();

      // then
      assertThat(outbox.getStatus()).isEqualTo(OutboxStatus.PENDING);
      assertThat(outbox.getRetryCount()).isEqualTo(1);
    }

    @Test
    @DisplayName("payload를 읽을 수 없는 행은 retry_count만 올리고 다음 행을 보내는지")
    void skipsUnreadableRowAndSendsNext() throws Exception {
      // given
      Outbox unreadable = Outbox.create(
          UUID.randomUUID(), TOPIC, "unreadable", "com.codeit.mople.NoSuchEvent", "{}");
      Outbox readable = pendingOutbox("readable");
      givenPending(unreadable, readable);
      givenSendResult(CompletableFuture.completedFuture(null));

      // when
      relay.relay();

      // then
      verify(kafkaTemplate, times(1)).send(recordCaptor.capture());
      assertThat(recordCaptor.getValue().key()).isEqualTo("readable");

      assertThat(unreadable.getStatus()).isEqualTo(OutboxStatus.PENDING);
      assertThat(unreadable.getRetryCount()).isEqualTo(1);
      assertThat(readable.getStatus()).isEqualTo(OutboxStatus.SENT);
    }
  }
}
