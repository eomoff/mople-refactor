package com.codeit.mople.global.config;

import com.codeit.mople.global.error.CustomException;
import com.codeit.mople.global.event.failure.ConsumeFailureMetricsListener;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.function.BiFunction;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.kafka.DefaultKafkaConsumerFactoryCustomizer;
import org.springframework.boot.autoconfigure.kafka.DefaultKafkaProducerFactoryCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

@Slf4j
@Configuration
@ConditionalOnProperty(prefix = KafkaProperties.PREFIX, name = "enabled", havingValue = "true")
public class KafkaConfig {

  private static final String DLT_SUFFIX = ".dlt";
  private static final int ANY_PARTITION = -1;

  static final int FAILURE_QUEUE_CAPACITY = 1000;

  static final BiFunction<ConsumerRecord<?, ?>, Exception, TopicPartition> DLT_DESTINATION_RESOLVER =
      (record, exception) -> new TopicPartition(record.topic() + DLT_SUFFIX, ANY_PARTITION);

  // 큐 포화·종료 이후에도 실패 기록을 버리지 않고 호출 스레드에서 처리
  static final RejectedExecutionHandler CALLER_RUNS_ALWAYS = (task, executor) -> {
    log.warn("Kafka 발행 실패 처리 큐 포화 호출 스레드에서 동기 처리: queueSize={}, shutdown={}",
        executor.getQueue().size(), executor.isShutdown());
    task.run();
  };

  public KafkaConfig(KafkaProperties kafkaProperties) {
    Assert.state(StringUtils.hasText(kafkaProperties.bootstrapServers()),
        "spring.kafka.enabled=true 이지만 spring.kafka.bootstrap-servers 가 비어있습니다. KAFKA_BOOTSTRAP_SERVERS 를 설정하세요.");
    log.info("Kafka 이벤트 발행 활성화: bootstrapServers={}", kafkaProperties.bootstrapServers());
  }

  // send 실패에 대한 후처리를 비동기로
  @Bean
  public ThreadPoolTaskExecutor kafkaPublishFailureExecutor() {
    ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();

    executor.setCorePoolSize(1);  // 스레드 1개 고정
    executor.setMaxPoolSize(1);   // 스레드 1개 고정
    executor.setQueueCapacity(FAILURE_QUEUE_CAPACITY);  // 대기 큐 상한
    executor.setThreadNamePrefix("kafka-publish-failure-"); // 로그 스레드 덤프에서 구분용
    executor.setWaitForTasksToCompleteOnShutdown(true);     // 종료시 큐에 남은 작업 버리지 않음
    executor.setAwaitTerminationSeconds(10);                // 버리지 않는대신 최대 10초 기다림
    // 큐가 꽉 차거나 종류 후 들어온 작업은 호출 스레드에서 즉시 실행(유실 방지)
    executor.setRejectedExecutionHandler(CALLER_RUNS_ALWAYS);
    executor.initialize();

    return executor;
  }

  @Bean
  public DefaultErrorHandler kafkaErrorHandler(
      KafkaTemplate<String, Object> kafkaTemplate,
      ConsumeFailureMetricsListener consumeFailureMetricsListener
  ) {

    // DeadLetterPublishingRecoverer -> 실패 전용 토픽에 던져넣는 기능(DLT로 메세지 보내줌)
    // Consumer가 실패했을 때 그 메세지를 (원래토픽명 + .dlt)로 재발행
    DeadLetterPublishingRecoverer recoverer =
        new DeadLetterPublishingRecoverer(kafkaTemplate, DLT_DESTINATION_RESOLVER);

    // Exponential : 지수
    // 지수백오프 구현을 위한 객체 생성
    ExponentialBackOffWithMaxRetries backOff = new ExponentialBackOffWithMaxRetries(3);

    backOff.setInitialInterval(1000L); // 초기 재시도 대기 시간(1초)
    backOff.setMultiplier(2.0); // 다음 재시도에 곱해지는 시간(2배)
    backOff.setMaxInterval(4000L); // 최대 재시도 대기 시간(4초)

    // 재시도 recoverer 이것을 재시도
    DefaultErrorHandler errorHandler = new DefaultErrorHandler(recoverer, backOff);

    // 비즈니스 예외(not found, 권한 등)는 재시도해도 결과가 같으므로 재시도하지 않음
    errorHandler.addNotRetryableExceptions(CustomException.class);

    errorHandler.setRetryListeners(consumeFailureMetricsListener);

    return errorHandler;
  }

  @Bean
  @SuppressWarnings("unchecked")
  public DefaultKafkaProducerFactoryCustomizer jsonSerializerCustomizer(ObjectMapper objectMapper) {
    return factory -> ((DefaultKafkaProducerFactory<String, Object>)factory)
        .setValueSerializerSupplier(() -> new JsonSerializer<>(objectMapper));
  }

  @Bean
  @SuppressWarnings("unchecked")
  public DefaultKafkaConsumerFactoryCustomizer jsonDeserializerCustomizer(ObjectMapper objectMapper) {
    return factory -> ((DefaultKafkaConsumerFactory<String, Object>)factory)
        .setValueDeserializerSupplier(() -> new ErrorHandlingDeserializer<>(new JsonDeserializer<>(objectMapper)));
  }
}