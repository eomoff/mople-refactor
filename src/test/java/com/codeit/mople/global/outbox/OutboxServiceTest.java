package com.codeit.mople.global.outbox;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;

import com.codeit.mople.global.outbox.repository.OutboxRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
@DisplayName("outbox 저장 테스트")
class OutboxServiceTest {

  record SampleEvent(UUID eventId, String name) {

  }

  final ObjectMapper objectMapper = new ObjectMapper();

  @Mock
  OutboxRepository outboxRepository;

  @Captor
  ArgumentCaptor<Outbox> outboxCaptor;

  OutboxService outboxService;

  @BeforeEach
  void setUp() {
    outboxService = new OutboxService(outboxRepository, objectMapper);
  }

  @Test
  @DisplayName("토픽은 aggregate_type, 키는 aggregate_id에 넣고 이벤트 클래스 이름과 JSON을 미발행으로 저장하는지")
  void savesPendingOutbox() throws Exception {
    // given
    SampleEvent event = new SampleEvent(UUID.randomUUID(), "이름");

    // when
    outboxService.save(event.eventId(), "mople.follow.created.v1", "followee-id", event);

    // then
    verify(outboxRepository).save(outboxCaptor.capture());

    Outbox saved = outboxCaptor.getValue();
    assertThat(saved.getEventId()).isEqualTo(event.eventId());
    assertThat(saved.getAggregateType()).isEqualTo("mople.follow.created.v1");
    assertThat(saved.getAggregateId()).isEqualTo("followee-id");
    assertThat(saved.getEventType()).isEqualTo(SampleEvent.class.getName());
    assertThat(saved.getPayload()).isEqualTo(objectMapper.writeValueAsString(event));
    assertThat(saved.getStatus()).isEqualTo(OutboxStatus.PENDING);
  }
}
