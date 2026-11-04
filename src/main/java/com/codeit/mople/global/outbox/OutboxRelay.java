package com.codeit.mople.global.outbox;

import com.codeit.mople.global.outbox.repository.OutboxRepository;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutionException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.transaction.annotation.Transactional;

@Slf4j
@RequiredArgsConstructor
public class OutboxRelay {

  public static final String EVENT_ID_HEADER = "eventId";
  static final int BATCH_SIZE = 100;

  private final OutboxRepository outboxRepository;
  private final KafkaTemplate<Object, Object> kafkaTemplate;
  private final ObjectMapper objectMapper;

  @Scheduled(fixedDelay = 1000)
  @Transactional
  public void relay() {
    for (Outbox outbox : outboxRepository.findPendingForUpdate(BATCH_SIZE)) {
      ProducerRecord<Object, Object> record;
      try {
        record = toRecord(outbox);
      } catch (ClassNotFoundException | JsonProcessingException e) {
        outbox.increaseRetryCount();
        log.error("outbox 이벤트를 읽지 못함: outboxId={}, eventType={}",
            outbox.getId(), outbox.getEventType(), e);
        continue;
      }

      try {
        kafkaTemplate.send(record).get();
        outbox.markSent();
      } catch (ExecutionException | RuntimeException e) {
        outbox.increaseRetryCount();
        log.warn("outbox 이벤트 발행 실패, 다음 폴링에 다시 보냄: outboxId={}, topic={}",
            outbox.getId(), outbox.getAggregateType(), e);
        return;
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return;
      }
    }
  }

  private ProducerRecord<Object, Object> toRecord(Outbox outbox)
      throws ClassNotFoundException, JsonProcessingException {
    Object event =
        objectMapper.readValue(outbox.getPayload(), Class.forName(outbox.getEventType()));
    ProducerRecord<Object, Object> record =
        new ProducerRecord<>(outbox.getAggregateType(), outbox.getAggregateId(), event);
    record.headers()
        .add(EVENT_ID_HEADER, outbox.getEventId().toString().getBytes(StandardCharsets.UTF_8));
    return record;
  }
}
