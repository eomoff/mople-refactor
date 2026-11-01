package com.codeit.mople.global.config;

import static org.assertj.core.api.Assertions.assertThat;

import com.codeit.mople.global.event.failure.ConsumeFailureMetricsListener;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.autoconfigure.jackson.JacksonAutoConfiguration;
import org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.DefaultErrorHandler;

@DisplayName("KafkaConfig 컨텍스트 테스트")
class KafkaConfigContextTest {

  static final String BOOTSTRAP_SERVERS = "localhost:9092";

  static KafkaProperties kafkaProperties() {
    return new KafkaProperties(true, BOOTSTRAP_SERVERS, new KafkaProperties.Topics(
        "mople.follow.created.v1",
        "mople.playlist.content-added.v1",
        "mople.direct-message.created.v1",
        "mople.notification.created.v1",
        "mople.content-search-index-events.v1",
        "mople.user-search-index-events.v1",
        "mople.playlist-search-index-events.v1",
        "mople.user.account-status-changed.v1",
        "mople.playlist.subscribed.v1",
        "mople.playlist.created.v1",
        "mople.direct-message.received.v1",
        "mople.review.written.v1"
    ));
  }

  final ApplicationContextRunner runner = new ApplicationContextRunner()
      .withConfiguration(
          AutoConfigurations.of(JacksonAutoConfiguration.class, KafkaAutoConfiguration.class))
      .withUserConfiguration(KafkaConfig.class)
      .withBean(KafkaProperties.class, KafkaConfigContextTest::kafkaProperties)
      .withBean(ConsumeFailureMetricsListener.class,
          () -> new ConsumeFailureMetricsListener(new SimpleMeterRegistry()))
      .withPropertyValues(
          "spring.kafka.enabled=true",
          "spring.kafka.bootstrap-servers=" + BOOTSTRAP_SERVERS,
          "spring.kafka.admin.auto-create=false");

  @Nested
  @DisplayName("템플릿 빈 구성")
  class Templates {

    @Test
    @DisplayName("자동 설정 템플릿 하나와 우리 에러 핸들러가 올라오는지")
    void registersSingleTemplateAndErrorHandler() {
      runner.run(context -> assertThat(context)
          .hasSingleBean(KafkaTemplate.class)
          .hasSingleBean(ProducerFactory.class)
          .hasSingleBean(DefaultErrorHandler.class));
    }

    @Test
    @DisplayName("kafka.enabled 가 꺼져 있으면 우리 에러 핸들러가 안 올라오는지")
    void skipsErrorHandlerWhenDisabled() {
      runner.withPropertyValues("spring.kafka.enabled=false")
          .run(context -> assertThat(context)
              .doesNotHaveBean(DefaultErrorHandler.class));
    }
  }
}
