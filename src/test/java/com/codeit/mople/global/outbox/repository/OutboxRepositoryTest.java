package com.codeit.mople.global.outbox.repository;

import static org.assertj.core.api.Assertions.assertThat;

import com.codeit.mople.global.config.JpaAuditingConfig;
import com.codeit.mople.global.config.QueryDslConfig;
import com.codeit.mople.global.outbox.Outbox;
import com.codeit.mople.global.outbox.OutboxStatus;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.boot.test.autoconfigure.orm.jpa.TestEntityManager;
import org.springframework.context.annotation.Import;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionTemplate;

@DataJpaTest
@Import({JpaAuditingConfig.class, QueryDslConfig.class})
@DisplayName("OutboxRepository 테스트")
class OutboxRepositoryTest {

  @Autowired
  OutboxRepository outboxRepository;

  @Autowired
  TestEntityManager entityManager;

  @Autowired
  PlatformTransactionManager transactionManager;

  private static Outbox pending() {
    return Outbox.create(UUID.randomUUID(), "mople.follow.created.v1", "key", "type", "{}");
  }

  private static Outbox sent() {
    Outbox outbox = pending();
    outbox.markSent();
    return outbox;
  }

  private Outbox persist(Outbox outbox) {
    return entityManager.persist(outbox);
  }

  private List<Long> idsOf(List<Outbox> outboxes) {
    return outboxes.stream().map(Outbox::getId).toList();
  }

  @Nested
  @DisplayName("미발행 행 조회 [findPendingForUpdate]")
  class FindPendingForUpdate {

    @Test
    @DisplayName("PENDING 행만 id 순서로 limit 개수만큼 가져오는지")
    void returnsPendingInIdOrderUpToLimit() {
      // given
      Outbox first = persist(pending());
      persist(sent());
      Outbox second = persist(pending());
      persist(pending());
      entityManager.flush();
      entityManager.clear();

      // when
      List<Outbox> found = outboxRepository.findPendingForUpdate(2);

      // then
      assertThat(idsOf(found)).containsExactly(first.getId(), second.getId());
    }
  }

  @Nested
  @DisplayName("다른 relay와 동시 조회")
  class ConcurrentRelays {

    @AfterEach
    void tearDown() {
      outboxRepository.deleteAll();
    }

    @Test
    @Transactional(propagation = Propagation.NOT_SUPPORTED)
    @DisplayName("다른 트랜잭션이 잠근 행은 건너뛰고 그 뒤에 쌓인 행만 가져오는지")
    void skipsRowsLockedByAnotherTransaction() throws Exception {
      // given
      List<Outbox> lockedRows = outboxRepository.saveAll(List.of(pending(), pending()));
      TransactionTemplate tx = new TransactionTemplate(transactionManager);
      CountDownLatch locked = new CountDownLatch(1);
      CountDownLatch release = new CountDownLatch(1);
      ExecutorService executor = Executors.newSingleThreadExecutor();

      try {
        Future<List<Long>> firstRelay = executor.submit(() -> tx.execute(status -> {
          List<Long> ids = idsOf(outboxRepository.findPendingForUpdate(10));
          locked.countDown();
          awaitQuietly(release);
          return ids;
        }));
        assertThat(locked.await(5, TimeUnit.SECONDS)).isTrue();
        Outbox newRow = outboxRepository.save(pending());

        // when
        List<Long> secondRelay = tx.execute(status ->
            idsOf(outboxRepository.findPendingForUpdate(10)));
        release.countDown();

        // then
        assertThat(firstRelay.get(5, TimeUnit.SECONDS))
            .containsExactly(lockedRows.get(0).getId(), lockedRows.get(1).getId());
        assertThat(secondRelay).containsExactly(newRow.getId());
      } finally {
        release.countDown();
        executor.shutdown();
      }
    }

    private void awaitQuietly(CountDownLatch latch) {
      try {
        latch.await(5, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
  }

  @Nested
  @DisplayName("발행 완료 행 삭제 [deleteByStatusAndCreatedAtBefore]")
  class DeleteSent {

    @Test
    @DisplayName("기준 시각 이전에 만든 SENT 행만 지우는지")
    void deletesOnlySentCreatedBefore() {
      // given
      Outbox sent = persist(sent());
      Outbox pending = persist(pending());
      entityManager.flush();
      entityManager.clear();

      // when
      int deleted = outboxRepository.deleteByStatusAndCreatedAtBefore(
          OutboxStatus.SENT, Instant.now().plusSeconds(60));

      // then
      assertThat(deleted).isEqualTo(1);
      assertThat(outboxRepository.findById(sent.getId())).isEmpty();
      assertThat(outboxRepository.findById(pending.getId())).isPresent();
    }

    @Test
    @DisplayName("기준 시각 이후에 만든 SENT 행은 남기는지")
    void keepsSentCreatedAfter() {
      // given
      Outbox sent = persist(sent());
      entityManager.flush();
      entityManager.clear();

      // when
      int deleted = outboxRepository.deleteByStatusAndCreatedAtBefore(
          OutboxStatus.SENT, Instant.now().minusSeconds(60));

      // then
      assertThat(deleted).isZero();
      assertThat(outboxRepository.findById(sent.getId())).isPresent();
    }
  }
}
