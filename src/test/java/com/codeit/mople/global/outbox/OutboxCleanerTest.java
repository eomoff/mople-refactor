package com.codeit.mople.global.outbox;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;

import com.codeit.mople.global.outbox.repository.OutboxRepository;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
@DisplayName("outbox 정리 스케줄러 테스트")
class OutboxCleanerTest {

  @Mock
  OutboxRepository outboxRepository;

  @InjectMocks
  OutboxCleaner outboxCleaner;

  @Captor
  ArgumentCaptor<Instant> beforeCaptor;

  @Test
  @DisplayName("SENT이고 보관 기간보다 오래된 행만 지우도록 요청하는지")
  void deletesSentOlderThanRetention() {
    // when
    outboxCleaner.deleteSent();

    // then
    verify(outboxRepository)
        .deleteByStatusAndCreatedAtBefore(eq(OutboxStatus.SENT), beforeCaptor.capture());

    assertThat(beforeCaptor.getValue())
        .isCloseTo(Instant.now().minus(OutboxCleaner.RETENTION), within(5, ChronoUnit.SECONDS));
  }
}
