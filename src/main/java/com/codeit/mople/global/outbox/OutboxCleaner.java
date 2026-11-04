package com.codeit.mople.global.outbox;

import com.codeit.mople.global.outbox.repository.OutboxRepository;
import java.time.Duration;
import java.time.Instant;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.javacrumbs.shedlock.spring.annotation.SchedulerLock;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Slf4j
@Component
@RequiredArgsConstructor
public class OutboxCleaner {

  static final Duration RETENTION = Duration.ofDays(7);

  private final OutboxRepository outboxRepository;

  @Scheduled(cron = "0 30 5 * * *", zone = "Asia/Seoul")
  @SchedulerLock(name = "outbox-sent-cleanup", lockAtMostFor = "PT10M", lockAtLeastFor = "PT1M")
  @Transactional
  public void deleteSent() {
    Instant before = Instant.now().minus(RETENTION);
    int deleted = outboxRepository.deleteByStatusAndCreatedAtBefore(OutboxStatus.SENT, before);
    log.info("발행 완료 outbox 정리: deleted={}, before={}", deleted, before);
  }
}
