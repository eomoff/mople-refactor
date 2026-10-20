package com.codeit.mople.global.config;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

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

  @Nested
  @DisplayName("발행 실패 처리 executor")
  class FailureExecutor {

    ThreadPoolTaskExecutor executor;

    @BeforeEach
    void setUp() {
      executor = new KafkaConfig(KafkaConfigContextTest.kafkaProperties())
          .kafkaPublishFailureExecutor();
    }

    @AfterEach
    void tearDown() {
      executor.shutdown();
    }

    @Test
    @DisplayName("큐가 가득 차도 실패 기록이 호출 스레드에서 실행되는지")
    void runsOnCallerThreadWhenSaturated() {
      // given
      CountDownLatch blocked = new CountDownLatch(1);
      executor.execute(() -> await(blocked));
      for (int i = 0; i < KafkaConfig.FAILURE_QUEUE_CAPACITY; i++) {
        executor.execute(() -> await(blocked));
      }

      // when
      AtomicReference<String> ranOn = new AtomicReference<>();
      executor.execute(() -> ranOn.set(Thread.currentThread().getName()));

      // then
      assertThat(ranOn.get()).isEqualTo(Thread.currentThread().getName());
      blocked.countDown();
    }

    @Test
    @DisplayName("executor 가 이미 종료됐어도 실패 기록이 실행되는지")
    void runsAfterShutdown() {
      // given
      executor.shutdown();

      // when
      AtomicBoolean ran = new AtomicBoolean();
      executor.execute(() -> ran.set(true));

      // then
      assertThat(ran).isTrue();
    }

    void await(CountDownLatch latch) {
      try {
        latch.await(5, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
  }
}