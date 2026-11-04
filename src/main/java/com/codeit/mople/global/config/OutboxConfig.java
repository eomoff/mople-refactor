package com.codeit.mople.global.config;

import com.codeit.mople.global.outbox.OutboxRelay;
import com.codeit.mople.global.outbox.repository.OutboxRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

@Configuration
@ConditionalOnProperty(prefix = KafkaProperties.PREFIX, name = "enabled", havingValue = "true")
public class OutboxConfig {

  @Bean
  public OutboxRelay outboxRelay(
      OutboxRepository outboxRepository,
      ProducerFactory<Object, Object> producerFactory,
      ObjectMapper objectMapper
  ) {
    return new OutboxRelay(outboxRepository, new KafkaTemplate<>(producerFactory), objectMapper);
  }
}
