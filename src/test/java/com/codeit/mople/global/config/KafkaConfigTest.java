package com.codeit.mople.global.config;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@DisplayName("KafkaConfig 테스트")
class KafkaConfigTest {

  static final String TOPIC = "mople.follow.created.v1";

  ConsumerRecord<String, Object> record;
  RuntimeException exception;

  @BeforeEach
  void setUp() {
    record = new ConsumerRecord<>(TOPIC, 2, 100L, "followee-key", new Object());
    exception = new RuntimeException("알림 생성 실패");
  }

  @Nested
  @DisplayName("DLT 목적지 해석")
  class DeadLetterDestination {

    @Test
    @DisplayName("원 토픽 이름 뒤에 .dlt를 붙인 토픽으로 보내는지")
    void resolveDltTopic() {
      // when
      TopicPartition destination =
          KafkaConfig.DLT_DESTINATION_RESOLVER.apply(record, exception);

      // then
      assertThat(destination.topic()).isEqualTo("mople.follow.created.v1.dlt");
    }

    @Test
    @DisplayName("파티션을 지정하지 않아서 DLT 파티션 수가 더 적어도 발행이 안 깨지는지")
    void resolveAnyPartition() {
      // when
      TopicPartition destination =
          KafkaConfig.DLT_DESTINATION_RESOLVER.apply(record, exception);

      // then
      assertThat(destination.partition()).isEqualTo(-1);
    }
  }
}
