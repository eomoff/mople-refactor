package com.codeit.mople.global.event.failure;

import com.codeit.mople.global.config.KafkaProperties;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.concurrent.Executor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.core.NestedExceptionUtils;
import org.springframework.kafka.support.ProducerListener;
import org.springframework.stereotype.Component;

// KafkaTemplate 전송이 최종 실패하면 spring-kafka가 onError를 호출함
// Boot가 이 빈을 자동 설정 KafkaTemplate에 붙여 주므로, 보내는 쪽은 kafkaTemplate.send만 호출하면 됨
@Slf4j
@Component
@ConditionalOnProperty(prefix = KafkaProperties.PREFIX, name = "enabled", havingValue = "true")
public class KafkaPublishFailureListener implements ProducerListener<Object, Object> {

  private static final String FAILURE_COUNTER = "kafka.event.publish.failure";
  private static final String DESCRIPTION = "발행 최종 실패로 Redis 에 적재된 이벤트 수";

  private static final String TAG_TOPIC = "topic";
  private static final String TAG_REASON = "reason";
  private static final String UNKNOWN_REASON = "Unknown";

  private final FailedEventStore failedEventStore;
  private final ObjectMapper objectMapper;
  private final MeterRegistry meterRegistry;
  private final Executor failureExecutor;

  public KafkaPublishFailureListener(
      FailedEventStore failedEventStore,
      ObjectMapper objectMapper,
      MeterRegistry meterRegistry,
      @Qualifier("kafkaPublishFailureExecutor") Executor failureExecutor
  ) {
    this.failedEventStore = failedEventStore;
    this.objectMapper = objectMapper;
    this.meterRegistry = meterRegistry;
    this.failureExecutor = failureExecutor;
  }

  // 프로듀서 네트워크 스레드에서 호출되므로 Redis 저장 같은 후처리는 전용 executor로 넘김
  @Override
  public void onError(
      ProducerRecord<Object, Object> record,
      RecordMetadata metadata,
      Exception exception
  ) {
    String topic = record.topic();
    String key = (String) record.key();
    Object event = record.value();

    failureExecutor.execute(() -> handleFailure(topic, key, event, exception));
  }

  // 프로듀서 최종 실패를 하나의 객채로 생성 및 로그 기록(여기서 만들어진 객체는 redis에 저장할 객체임)
  private void handleFailure(String topic, String key, Object event, Throwable cause) {
    failedEventStore.save(FailedEvent.of(topic, key, event, serialize(topic, key, event), cause));
    countFailure(topic, cause);

    log.error("Kafka 이벤트 발행 최종 실패: topic={}, key={}, eventType={}",
        topic, key, event.getClass().getSimpleName(), cause);
  }

  // 예외 메시지를 태그로 쓰면 값이 무한히 늘어나므로 클래스 이름만 쓰는 집계
  private void countFailure(String topic, Throwable cause) {
    Counter.builder(FAILURE_COUNTER)
        .description(DESCRIPTION)
        .tag(TAG_TOPIC, topic)
        .tag(TAG_REASON, reasonTagOf(cause))
        .register(meterRegistry)
        .increment();
  }

  // 소비 쪽 ConsumeFailureMetricsListener 와 같은 규칙으로 원인 태그를 뽑음
  private String reasonTagOf(Throwable cause) {
    if (cause == null) {
      return UNKNOWN_REASON;
    }

    return NestedExceptionUtils.getMostSpecificCause(cause).getClass().getSimpleName();
  }

  // event를 JSON 문자열로 직렬화 Redis Stream은 문자열 값만 저장 가능해서 변환 필요
  // 변환 도중 문제가 생기면 로그 기록
  private String serialize(String topic, String key, Object event) {
    try {
      return objectMapper.writeValueAsString(event);
    } catch (JsonProcessingException e) {
      log.error("Kafka Producer 실패 이벤트 본문 직렬화 실패: topic={}, key={}, eventType={}",
          topic, key, event.getClass().getSimpleName(), e);
      return "";
    }
  }
}
