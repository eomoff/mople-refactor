package com.codeit.mople.global.outbox;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EntityListeners;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Index;
import jakarta.persistence.Table;
import java.time.Instant;
import java.util.Objects;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.CreatedDate;
import org.springframework.data.jpa.domain.support.AuditingEntityListener;

@Entity
@Table(name = "outbox",
    indexes = @Index(name = "idx_outbox_status_created_at", columnList = "status, created_at"))
@EntityListeners(AuditingEntityListener.class)
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class Outbox {

  @Id
  @GeneratedValue(strategy = GenerationType.IDENTITY)
  private Long id;

  @Column(nullable = false, length = 100)
  private String aggregateType;

  @Column(nullable = false, length = 100)
  private String aggregateId;

  @Column(nullable = false, length = 100)
  private String eventType;

  @Column(columnDefinition = "TEXT", nullable = false)
  private String payload;

  @Enumerated(EnumType.STRING)
  @Column(nullable = false, length = 20)
  private OutboxStatus status;

  @Column(nullable = false)
  private int retryCount;

  @CreatedDate
  @Column(nullable = false, updatable = false)
  private Instant createdAt;

  @Column
  private Instant sentAt;

  private Outbox(String aggregateType, String aggregateId, String eventType, String payload) {
    this.aggregateType = Objects.requireNonNull(aggregateType, "aggregateType");
    this.aggregateId = Objects.requireNonNull(aggregateId, "aggregateId");
    this.eventType = Objects.requireNonNull(eventType, "eventType");
    this.payload = Objects.requireNonNull(payload, "payload");
    this.status = OutboxStatus.PENDING;
  }

  public static Outbox create(
      String aggregateType, String aggregateId, String eventType, String payload) {
    return new Outbox(aggregateType, aggregateId, eventType, payload);
  }
}
