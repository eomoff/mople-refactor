package com.codeit.mople.global.outbox.repository;

import com.codeit.mople.global.outbox.Outbox;
import com.codeit.mople.global.outbox.OutboxStatus;
import java.time.Instant;
import java.util.List;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

public interface OutboxRepository extends JpaRepository<Outbox, Long> {

  @Query(value = """
      SELECT * FROM outbox
      WHERE status = 'PENDING'
      ORDER BY id
      LIMIT :limit
      FOR UPDATE SKIP LOCKED
      """, nativeQuery = true)
  List<Outbox> findPendingForUpdate(@Param("limit") int limit);

  @Modifying
  @Query("DELETE FROM Outbox o WHERE o.status = :status AND o.createdAt < :before")
  int deleteByStatusAndCreatedAtBefore(
      @Param("status") OutboxStatus status, @Param("before") Instant before);
}
