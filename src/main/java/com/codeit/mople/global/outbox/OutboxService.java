package com.codeit.mople.global.outbox;

import com.codeit.mople.global.outbox.repository.OutboxRepository;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

@Service
@RequiredArgsConstructor
public class OutboxService {

  private final OutboxRepository outboxRepository;
  private final ObjectMapper objectMapper;

  @Transactional(propagation = Propagation.MANDATORY)
  public void save(UUID eventId, String topic, String key, Object event) {
    outboxRepository.save(
        Outbox.create(eventId, topic, key, event.getClass().getName(), toJson(event)));
  }

  private String toJson(Object event) {
    try {
      return objectMapper.writeValueAsString(event);
    } catch (JsonProcessingException e) {
      throw new IllegalStateException("outbox 이벤트 직렬화에 실패했습니다.", e);
    }
  }
}
