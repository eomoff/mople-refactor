package com.codeit.mople.domain.notification.repository;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import jakarta.persistence.PersistenceException;

import com.codeit.mople.domain.notification.entity.Notification;
import com.codeit.mople.domain.notification.entity.NotificationLevel;
import com.codeit.mople.domain.notification.entity.NotificationTemplate;
import com.codeit.mople.domain.notification.entity.NotificationType;
import com.codeit.mople.global.config.JpaAuditingConfig;
import com.codeit.mople.global.config.QueryDslConfig;
import java.time.Instant;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.boot.test.autoconfigure.orm.jpa.TestEntityManager;
import org.springframework.context.annotation.Import;

@DataJpaTest
@Import({JpaAuditingConfig.class, QueryDslConfig.class})
@DisplayName("NotificationRepository 테스트")
class NotificationRepositoryTest {

  @Autowired
  NotificationRepository notificationRepository;

  @Autowired
  TestEntityManager entityManager;

  UUID receiverId;
  UUID otherReceiverId;

  @BeforeEach
  void setUp() {
    receiverId = UUID.randomUUID();
    otherReceiverId = UUID.randomUUID();
  }

  private Notification createNotification(UUID receiverId, String title, NotificationType type) {
    return Notification.create(receiverId, new NotificationTemplate(title, "내용"), type,
        UUID.randomUUID().toString());
  }

  private Notification saveNotification(UUID receiverId, String title, NotificationType type) {
    return persistAndFind(createNotification(receiverId, title, type));
  }

  private Notification saveReadNotification(UUID receiverId, String title) {
    Notification notification = createNotification(receiverId, title,
        NotificationType.FOLLOW_CREATED);
    notification.markAsRead();
    return persistAndFind(notification);
  }

  private Notification persistAndFind(Notification notification) {
    entityManager.persist(notification);
    entityManager.flush();
    UUID savedId = notification.getId();
    entityManager.clear();
    // DB에서 재조회해 실제 저장된 createdAt을 반영 (정밀도 차이 방지)
    return entityManager.find(Notification.class, savedId);
  }

  // id와 createdAt은 저장할 때 정해지므로, 커서 경계를 고정하려면 저장 후 native SQL로 덮어쓴다
  private void overwriteIdAndCreatedAt(UUID savedId, UUID id, Instant createdAt) {
    entityManager
        .getEntityManager()
        .createNativeQuery(
            "UPDATE notifications SET id = :id, created_at = :createdAt WHERE id = :savedId")
        .setParameter("id", id)
        .setParameter("createdAt", createdAt)
        .setParameter("savedId", savedId)
        .executeUpdate();
    entityManager.clear();
  }

  @Nested
  @DisplayName("알림 저장 [save]")
  class Save {

    @Test
    @DisplayName("저장 후 모든 필드와 createdAt이 조회된다")
    void save_success_allFieldsAndCreatedAtPersisted() {
      Notification notification = Notification.create(receiverId,
          new NotificationTemplate("제목", "내용"), NotificationType.ROLE_CHANGED, "hashId");

      Notification saved = notificationRepository.save(notification);
      entityManager.flush();
      entityManager.clear();

      Notification found = entityManager.find(Notification.class, saved.getId());
      assertThat(found).isNotNull();
      assertThat(found.getReceiverId()).isEqualTo(receiverId);
      assertThat(found.getTemplate()).isEqualTo(new NotificationTemplate("제목", "내용"));
      assertThat(found.getNotificationType()).isEqualTo(NotificationType.ROLE_CHANGED);
      assertThat(found.getLevel()).isEqualTo(NotificationLevel.WARNING);
      assertThat(found.getHashId()).isEqualTo("hashId");
      assertThat(found.getReadAt()).isNull();
      assertThat(found.getCreatedAt()).isNotNull();
    }

    @Test
    @DisplayName("템플릿의 content가 null이어도 DB에 저장된다")
    void save_success_whenContentIsNull() {
      Notification notification = Notification.create(receiverId,
          new NotificationTemplate("제목", null), NotificationType.FOLLOW_CREATED, "hashId");

      Notification saved = notificationRepository.save(notification);
      entityManager.flush();
      entityManager.clear();

      Notification found = entityManager.find(Notification.class, saved.getId());
      assertThat(found).isNotNull();
      assertThat(found.getTemplate().content()).isNull();
    }

    @Test
    @DisplayName("같은 hashId로 저장하면 unique 제약 위반 예외가 발생한다")
    void save_fail_whenHashIdDuplicated() {
      entityManager.persist(Notification.create(receiverId, new NotificationTemplate("제목", "내용"),
          NotificationType.FOLLOW_CREATED, "duplicatedHashId"));
      entityManager.flush();

      assertThatThrownBy(
          () -> {
            entityManager.persist(Notification.create(receiverId,
                new NotificationTemplate("제목", "내용"), NotificationType.FOLLOW_CREATED,
                "duplicatedHashId"));
            entityManager.flush();
          })
          .isInstanceOf(PersistenceException.class);
    }
  }

  @Nested
  @DisplayName("hashId 존재 여부 조회 [existsByHashId]")
  class ExistsByHashId {

    @Test
    @DisplayName("같은 hashId의 알림이 있으면 true를 반환한다.")
    void existsByHashId_success_returnsTrueWhenExists() {
      Notification saved = saveNotification(receiverId, "알림", NotificationType.FOLLOW_CREATED);

      assertThat(notificationRepository.existsByHashId(saved.getHashId())).isTrue();
    }

    @Test
    @DisplayName("같은 hashId의 알림이 없으면 false를 반환한다.")
    void existsByHashId_success_returnsFalseWhenNotExists() {
      saveNotification(receiverId, "알림", NotificationType.FOLLOW_CREATED);

      assertThat(notificationRepository.existsByHashId("notExistsHashId")).isFalse();
    }
  }

  @Nested
  @DisplayName("안 읽은 알림 커서 조회 [findUnreadNotificationByCursor]")
  class FindUnreadNotificationByCursor {

    @Test
    @DisplayName("cursor 없이 조회하면 receiver의 알림만 최신순으로 반환된다.")
    void findByCursor_success_firstPageReturnsOwnNotificationsDesc() {
      saveNotification(receiverId, "알림1", NotificationType.FOLLOW_CREATED);
      saveNotification(receiverId, "알림2", NotificationType.PLAYLIST_SUBSCRIBED);
      saveNotification(receiverId, "알림3", NotificationType.DIRECT_MESSAGE_RECEIVED);
      saveNotification(otherReceiverId, "타유저 알림", NotificationType.FOLLOW_CREATED);

      List<Notification> result =
          notificationRepository.findUnreadNotificationByCursor(receiverId, null, null, 20);

      // receiver 알림 3개만 반환 (otherReceiver 알림 제외)
      assertThat(result).hasSize(3);
      assertThat(result).allMatch(n -> n.getReceiverId().equals(receiverId));
      // 최신순 정렬 확인
      assertThat(result)
          .isSortedAccordingTo(Comparator.comparing(Notification::getCreatedAt).reversed());
    }

    @Test
    @DisplayName("읽은 알림은 반환되지 않는다.")
    void findByCursor_success_excludesReadNotifications() {
      Notification unread = saveNotification(receiverId, "안 읽은 알림",
          NotificationType.FOLLOW_CREATED);
      saveReadNotification(receiverId, "읽은 알림");

      List<Notification> result =
          notificationRepository.findUnreadNotificationByCursor(receiverId, null, null, 20);

      assertThat(result).hasSize(1);
      assertThat(result.get(0).getId()).isEqualTo(unread.getId());
    }

    @Test
    @DisplayName("알림이 없으면 빈 리스트를 반환한다.")
    void findByCursor_success_emptyListWhenNoNotifications() {
      List<Notification> result =
          notificationRepository.findUnreadNotificationByCursor(receiverId, null, null, 20);

      assertThat(result).isEmpty();
    }

    @Test
    @DisplayName("알림이 limit+1개 초과 존재하면 최대 limit+1개까지만 반환된다.")
    void findByCursor_success_returnsLimitPlusOneWhenExceedsLimit() {
      for (int i = 0; i < 5; i++) {
        saveNotification(receiverId, "알림" + i, NotificationType.FOLLOW_CREATED);
      }

      List<Notification> result =
          notificationRepository.findUnreadNotificationByCursor(receiverId, null, null, 3);

      // limit=3이므로 limit+1=4개까지만 반환 (Service에서 hasNext 판별용)
      assertThat(result).hasSize(4);
    }

    @Test
    @DisplayName("알림이 limit개 이하이면 전체를 반환한다.")
    void findByCursor_success_returnsAllWhenUnderLimit() {
      saveNotification(receiverId, "알림1", NotificationType.FOLLOW_CREATED);
      saveNotification(receiverId, "알림2", NotificationType.PLAYLIST_SUBSCRIBED);

      List<Notification> result =
          notificationRepository.findUnreadNotificationByCursor(receiverId, null, null, 5);

      assertThat(result).hasSize(2);
    }

    @Test
    @DisplayName("알림이 정확히 limit개이면 limit개를 반환한다.")
    void findByCursor_success_returnsExactlyLimitWhenEqualsLimit() {
      for (int i = 0; i < 3; i++) {
        saveNotification(receiverId, "알림" + i, NotificationType.FOLLOW_CREATED);
      }

      List<Notification> result =
          notificationRepository.findUnreadNotificationByCursor(receiverId, null, null, 3);

      // 데이터 수 == limit 이면 limit+1 쿼리 결과도 3개 → hasNext=false 판별 기준
      assertThat(result).hasSize(3);
    }

    @Test
    @DisplayName("cursor 시각보다 오래된 알림만 반환되고 cursor 시각 이후 알림은 제외된다.")
    void findByCursor_success_returnsOnlyOlderThanCursor() throws InterruptedException {
      Notification older = saveNotification(receiverId, "오래된 알림",
          NotificationType.FOLLOW_CREATED);
      Thread.sleep(10); // 타임스탬프 차이 보장
      Notification newer = saveNotification(receiverId, "최신 알림",
          NotificationType.FOLLOW_CREATED);

      List<Notification> result =
          notificationRepository.findUnreadNotificationByCursor(
              receiverId, newer.getCreatedAt(), newer.getId(), 20);

      assertThat(result).hasSize(1);
      assertThat(result.get(0).getId()).isEqualTo(older.getId());
      assertThat(result.get(0).getTemplate().title()).isEqualTo("오래된 알림");
      // cursor 시각 이후인 newer는 포함되지 않아야 함
      assertThat(result).noneMatch(n -> n.getId().equals(newer.getId()));
    }

    @Test
    @DisplayName("cursor 이후에 저장된 알림이 여러 개여도 cursor 이전 알림만 반환된다.")
    void findByCursor_success_returnsOnlyOlderWhenMultipleNewerExist() throws InterruptedException {
      Notification oldest = saveNotification(receiverId, "가장 오래된 알림",
          NotificationType.FOLLOW_CREATED);
      Thread.sleep(10);
      Notification middle = saveNotification(receiverId, "중간 알림",
          NotificationType.PLAYLIST_SUBSCRIBED);
      Thread.sleep(10);
      saveNotification(receiverId, "최신 알림1", NotificationType.DIRECT_MESSAGE_RECEIVED);
      saveNotification(receiverId, "최신 알림2", NotificationType.ROLE_CHANGED);

      List<Notification> result =
          notificationRepository.findUnreadNotificationByCursor(
              receiverId, middle.getCreatedAt(), middle.getId(), 20);

      assertThat(result).hasSize(1);
      assertThat(result.get(0).getId()).isEqualTo(oldest.getId());
    }

    @Test
    @DisplayName("같은 createdAt을 가진 알림은 idAfter 기준으로 이전 알림만 반환된다.")
    void findByCursor_success_returnsOlderByIdAfterWhenSameCreatedAt() {
      Instant fixedTime = Instant.parse("2024-01-01T00:00:00Z");
      // UUID 대소 관계를 명확히 고정: smallId < largeId (바이트 단위 비교)
      UUID smallId = UUID.fromString("00000000-0000-0000-0000-000000000001");
      UUID largeId = UUID.fromString("ffffffff-ffff-ffff-ffff-ffffffffffff");

      Notification small = saveNotification(receiverId, "알림_small",
          NotificationType.FOLLOW_CREATED);
      Notification large = saveNotification(receiverId, "알림_large",
          NotificationType.FOLLOW_CREATED);
      overwriteIdAndCreatedAt(small.getId(), smallId, fixedTime);
      overwriteIdAndCreatedAt(large.getId(), largeId, fixedTime);

      // largeId를 cursor로 사용 → createdAt이 같으므로 id < largeId인 smallId 알림만 반환
      List<Notification> result =
          notificationRepository.findUnreadNotificationByCursor(
              receiverId, fixedTime, largeId, 20);

      assertThat(result).hasSize(1);
      assertThat(result.get(0).getId()).isEqualTo(smallId);
    }
  }

  @Nested
  @DisplayName("수신자별 안 읽은 알림 개수 조회 [countByReceiverIdAndReadAtIsNull]")
  class CountByReceiverIdAndReadAtIsNull {

    @Test
    @DisplayName("알림이 없으면 0을 반환한다.")
    void countUnread_success_returnsZeroWhenNoNotifications() {
      long count = notificationRepository.countByReceiverIdAndReadAtIsNull(receiverId);

      assertThat(count).isZero();
    }

    @Test
    @DisplayName("receiver의 안 읽은 알림 개수를 정확히 반환한다.")
    void countUnread_success_returnsExactCount() {
      saveNotification(receiverId, "알림1", NotificationType.FOLLOW_CREATED);
      saveNotification(receiverId, "알림2", NotificationType.PLAYLIST_SUBSCRIBED);
      saveNotification(receiverId, "알림3", NotificationType.DIRECT_MESSAGE_RECEIVED);

      long count = notificationRepository.countByReceiverIdAndReadAtIsNull(receiverId);

      assertThat(count).isEqualTo(3);
    }

    @Test
    @DisplayName("다른 receiver의 알림은 개수에 포함되지 않는다.")
    void countUnread_success_excludesOtherReceiverNotifications() {
      saveNotification(receiverId, "내 알림", NotificationType.FOLLOW_CREATED);
      saveNotification(otherReceiverId, "타유저 알림1", NotificationType.FOLLOW_CREATED);
      saveNotification(otherReceiverId, "타유저 알림2", NotificationType.FOLLOW_CREATED);

      long count = notificationRepository.countByReceiverIdAndReadAtIsNull(receiverId);

      assertThat(count).isEqualTo(1);
    }

    @Test
    @DisplayName("읽은 알림은 개수에 포함되지 않는다.")
    void countUnread_success_excludesReadNotifications() {
      saveNotification(receiverId, "안 읽은 알림", NotificationType.FOLLOW_CREATED);
      saveReadNotification(receiverId, "읽은 알림1");
      saveReadNotification(receiverId, "읽은 알림2");

      long count = notificationRepository.countByReceiverIdAndReadAtIsNull(receiverId);

      assertThat(count).isEqualTo(1);
    }
  }
}
