package com.codeit.mople.domain.notification.integration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.csrf;
import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.user;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultHandlers.print;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.codeit.mople.domain.auth.security.CustomUserDetails;
import com.codeit.mople.domain.notification.entity.Notification;
import com.codeit.mople.domain.notification.entity.NotificationTemplate;
import com.codeit.mople.domain.notification.entity.NotificationType;
import com.codeit.mople.domain.notification.repository.NotificationRepository;
import com.codeit.mople.domain.user.entity.Role;
import com.codeit.mople.global.config.SecurityConfig;
import com.codeit.mople.global.jwt.JwtProvider;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;
import java.time.Instant;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.transaction.annotation.Transactional;

@Import(SecurityConfig.class)
@Transactional
@SpringBootTest
@ActiveProfiles("test")
@AutoConfigureMockMvc
@DisplayName("알림 커서 페이지네이션 통합 테스트")
public class NotificationIntegrationTest {

  private static final UUID RECEIVER_ID = UUID.fromString("00000000-0000-0000-0000-000000000000");

  @Autowired
  private MockMvc mockMvc;

  @Autowired
  private ObjectMapper objectMapper;

  @Autowired
  private NotificationRepository notificationRepository;

  @MockitoBean
  private JwtProvider jwtProvider;

  @PersistenceContext
  private EntityManager entityManager;

  private CustomUserDetails principal;
  private int alertCounter = 0;

  @BeforeEach
  void setUp() {
    alertCounter = 0;
    principal = new CustomUserDetails(RECEIVER_ID, Role.USER);
  }

  private UUID createNotification(String title, NotificationType type) {
    return createNotification(RECEIVER_ID, title, type);
  }

  // 알림마다 1초씩 다른 명시적 timestamp로 덮어써 저장 시각의 정밀도 문제 방지
  private UUID createNotification(UUID receiverId, String title, NotificationType type) {
    alertCounter++;
    Notification saved = notificationRepository.saveAndFlush(Notification.create(receiverId,
        new NotificationTemplate(title, "내용"), type, UUID.randomUUID().toString()));
    entityManager.createNativeQuery(
            "UPDATE notifications SET created_at = :createdAt WHERE id = :id")
        .setParameter("createdAt", Instant.EPOCH.plusSeconds(alertCounter))
        .setParameter("id", saved.getId())
        .executeUpdate();
    entityManager.clear();
    return saved.getId();
  }

  @Nested
  @DisplayName("GET /api/notifications - 첫 페이지 조회")
  class FirstPage {

    @Test
    @DisplayName("성공: 알림이 없으면 200과 빈 data가 반환된다.")
    void success_empty_notifications() throws Exception {
      mockMvc.perform(get("/api/notifications")
              .param("limit", "20")
              .with(user(principal)))
          .andDo(print())
          .andExpect(status().isOk())
          .andExpect(jsonPath("$.data").isArray())
          .andExpect(jsonPath("$.data").isEmpty())
          .andExpect(jsonPath("$.hasNext").value(false))
          .andExpect(jsonPath("$.totalCount").value(0))
          .andExpect(jsonPath("$.sortBy").value("createdAt"))
          .andExpect(jsonPath("$.sortDirection").value("DESCENDING"));
    }

    @Test
    @DisplayName("성공: 알림이 있으면 200과 최신순 알림 목록이 반환된다.")
    void success_returns_notifications_in_descending_order() throws Exception {
      createNotification("오래된 알림", NotificationType.FOLLOW_CREATED);
      createNotification("중간 알림", NotificationType.PLAYLIST_SUBSCRIBED);
      createNotification("최신 알림", NotificationType.DIRECT_MESSAGE_RECEIVED);

      mockMvc.perform(get("/api/notifications")
              .param("limit", "20")
              .with(user(principal)))
          .andDo(print())
          .andExpect(status().isOk())
          .andExpect(jsonPath("$.data.length()").value(3))
          .andExpect(jsonPath("$.data[0].title").value("최신 알림"))
          .andExpect(jsonPath("$.data[1].title").value("중간 알림"))
          .andExpect(jsonPath("$.data[2].title").value("오래된 알림"))
          .andExpect(jsonPath("$.hasNext").value(false))
          .andExpect(jsonPath("$.totalCount").value(3));
    }

    @Test
    @DisplayName("성공: 알림이 limit+1개 이상이면 hasNext=true이고 nextCursor가 반환된다.")
    void success_has_next_when_more_than_limit() throws Exception {
      createNotification("알림1", NotificationType.FOLLOW_CREATED);
      createNotification("알림2", NotificationType.FOLLOW_CREATED);
      createNotification("알림3", NotificationType.FOLLOW_CREATED);

      mockMvc.perform(get("/api/notifications")
              .param("limit", "2")
              .with(user(principal)))
          .andDo(print())
          .andExpect(status().isOk())
          .andExpect(jsonPath("$.data.length()").value(2))
          .andExpect(jsonPath("$.hasNext").value(true))
          .andExpect(jsonPath("$.nextCursor").isNotEmpty())
          .andExpect(jsonPath("$.nextIdAfter").isNotEmpty());
    }

    @Test
    @DisplayName("성공: 다른 receiver의 알림은 조회되지 않는다.")
    void success_notifications_are_isolated_by_receiver() throws Exception {
      UUID otherReceiverId = UUID.randomUUID();

      createNotification("내 알림", NotificationType.FOLLOW_CREATED);
      createNotification(otherReceiverId, "타유저 알림1", NotificationType.FOLLOW_CREATED);
      createNotification(otherReceiverId, "타유저 알림2", NotificationType.FOLLOW_CREATED);

      mockMvc.perform(get("/api/notifications")
              .param("limit", "20")
              .with(user(principal)))
          .andDo(print())
          .andExpect(status().isOk())
          .andExpect(jsonPath("$.data.length()").value(1))
          .andExpect(jsonPath("$.data[0].title").value("내 알림"))
          .andExpect(jsonPath("$.totalCount").value(1));
    }

    @Test
    @DisplayName("성공: 각 알림의 응답 필드가 올바르게 반환된다.")
    void success_notification_fields_are_correct() throws Exception {
      createNotification("팔로우 알림", NotificationType.FOLLOW_CREATED);

      mockMvc.perform(get("/api/notifications")
              .param("limit", "20")
              .with(user(principal)))
          .andDo(print())
          .andExpect(status().isOk())
          .andExpect(jsonPath("$.data[0].id").isNotEmpty())
          .andExpect(jsonPath("$.data[0].title").value("팔로우 알림"))
          .andExpect(jsonPath("$.data[0].content").value("내용"))
          .andExpect(jsonPath("$.data[0].level").value("INFO"))
          .andExpect(jsonPath("$.data[0].receiverId").value(RECEIVER_ID.toString()))
          .andExpect(jsonPath("$.data[0].createdAt").isNotEmpty());
    }

    @Test
    @DisplayName("성공: limit을 생략하면 기본값 20이 적용된다.")
    void success_default_limit_when_omitted() throws Exception {
      for (int i = 1; i <= 25; i++) {
        createNotification("알림" + i, NotificationType.FOLLOW_CREATED);
      }

      mockMvc.perform(get("/api/notifications")
              .with(user(principal)))
          .andDo(print())
          .andExpect(status().isOk())
          .andExpect(jsonPath("$.data.length()").value(20))
          .andExpect(jsonPath("$.hasNext").value(true));
    }
  }

  @Nested
  @DisplayName("DELETE /api/notifications/{notificationId} - 알림 읽음 처리")
  class ReadNotification {

    @Test
    @DisplayName("성공: 본인 알림을 읽음 처리하면 204가 반환되고 목록과 totalCount에서 빠진다.")
    void success_read_notification() throws Exception {
      // given
      UUID notificationId = createNotification("읽을 알림", NotificationType.FOLLOW_CREATED);
      createNotification("안 읽은 알림", NotificationType.FOLLOW_CREATED);

      // when
      mockMvc.perform(delete("/api/notifications/{notificationId}", notificationId)
              .with(user(principal))
              .with(csrf()))
          .andDo(print())
          .andExpect(status().isNoContent());

      // then: 읽음 처리 후 알림 목록에는 안 읽은 알림만 남음
      mockMvc.perform(get("/api/notifications")
              .param("limit", "20")
              .with(user(principal)))
          .andExpect(status().isOk())
          .andExpect(jsonPath("$.totalCount").value(1))
          .andExpect(jsonPath("$.data.length()").value(1))
          .andExpect(jsonPath("$.data[0].title").value("안 읽은 알림"));
    }

    @Test
    @DisplayName("성공: 읽음 처리한 알림은 삭제되지 않고 readAt이 기록된다.")
    void success_read_notification_keeps_notification_with_read_at() throws Exception {
      // given
      UUID notificationId = createNotification("읽을 알림", NotificationType.FOLLOW_CREATED);

      // when
      mockMvc.perform(delete("/api/notifications/{notificationId}", notificationId)
              .with(user(principal))
              .with(csrf()))
          .andExpect(status().isNoContent());

      // then
      entityManager.flush();
      entityManager.clear();
      Notification notification = notificationRepository.findById(notificationId).orElseThrow();
      assertThat(notification.getReadAt()).isNotNull();
    }

    @Test
    @DisplayName("실패: 존재하지 않는 알림을 읽음 처리하면 404를 반환한다.")
    void fail_404_when_notification_not_found() throws Exception {
      // given
      UUID nonExistentId = UUID.randomUUID();

      // when & then
      mockMvc.perform(delete("/api/notifications/{notificationId}", nonExistentId)
              .with(user(principal))
              .with(csrf()))
          .andDo(print())
          .andExpect(status().isNotFound())
          .andExpect(jsonPath("$.success").value(false))
          .andExpect(jsonPath("$.error.code").value("NOTIFICATION-001"));
    }

    @Test
    @DisplayName("실패: 다른 유저의 알림을 읽음 처리하면 403을 반환한다.")
    void fail_403_when_reading_other_user_notification() throws Exception {
      // given
      UUID otherNotificationId = createNotification(UUID.randomUUID(), "타유저 알림",
          NotificationType.FOLLOW_CREATED);

      // when & then: RECEIVER_ID 유저가 타유저 알림 읽음 처리 시도
      mockMvc.perform(delete("/api/notifications/{notificationId}", otherNotificationId)
              .with(user(principal))
              .with(csrf()))
          .andDo(print())
          .andExpect(status().isForbidden())
          .andExpect(jsonPath("$.success").value(false))
          .andExpect(jsonPath("$.error.code").value("NOTIFICATION-002"));
    }
  }

  @Nested
  @DisplayName("GET /api/notifications - 커서 기반 다음 페이지 조회")
  class NextPage {

    @Test
    @DisplayName("성공: 첫 페이지의 nextCursor로 다음 페이지를 조회하면 이전에 없던 알림이 반환된다.")
    void success_next_page_with_cursor() throws Exception {
      createNotification("알림1", NotificationType.FOLLOW_CREATED);
      createNotification("알림2", NotificationType.PLAYLIST_SUBSCRIBED);
      createNotification("알림3", NotificationType.DIRECT_MESSAGE_RECEIVED);

      // 첫 페이지 (limit=2) → 알림3, 알림2 반환
      MvcResult firstResult = mockMvc.perform(get("/api/notifications")
              .param("limit", "2")
              .with(user(principal)))
          .andExpect(status().isOk())
          .andExpect(jsonPath("$.data.length()").value(2))
          .andExpect(jsonPath("$.data[0].title").value("알림3"))
          .andExpect(jsonPath("$.data[1].title").value("알림2"))
          .andExpect(jsonPath("$.hasNext").value(true))
          .andReturn();

      String body = firstResult.getResponse().getContentAsString();
      String nextCursor = objectMapper.readTree(body).get("nextCursor").asText();
      String nextIdAfter = objectMapper.readTree(body).get("nextIdAfter").asText();

      // 두 번째 페이지 → 알림1만 반환
      mockMvc.perform(get("/api/notifications")
              .param("limit", "2")
              .param("cursor", nextCursor)
              .param("idAfter", nextIdAfter)
              .with(user(principal)))
          .andDo(print())
          .andExpect(status().isOk())
          .andExpect(jsonPath("$.data.length()").value(1))
          .andExpect(jsonPath("$.data[0].title").value("알림1"))
          .andExpect(jsonPath("$.hasNext").value(false))
          .andExpect(jsonPath("$.nextCursor").doesNotExist())
          .andExpect(jsonPath("$.nextIdAfter").doesNotExist());
    }

    @Test
    @DisplayName("성공: 3페이지에 걸친 연속 cursor 페이지네이션이 올바르게 동작한다.")
    void success_three_page_cursor_pagination() throws Exception {
      createNotification("알림1", NotificationType.FOLLOW_CREATED);
      createNotification("알림2", NotificationType.PLAYLIST_SUBSCRIBED);
      createNotification("알림3", NotificationType.DIRECT_MESSAGE_RECEIVED);

      // 1페이지: limit=1 → 알림3 반환, hasNext=true
      MvcResult page1 = mockMvc.perform(get("/api/notifications")
              .param("limit", "1")
              .with(user(principal)))
          .andExpect(status().isOk())
          .andExpect(jsonPath("$.data.length()").value(1))
          .andExpect(jsonPath("$.data[0].title").value("알림3"))
          .andExpect(jsonPath("$.hasNext").value(true))
          .andReturn();

      String body1 = page1.getResponse().getContentAsString();
      String cursor1 = objectMapper.readTree(body1).get("nextCursor").asText();
      String idAfter1 = objectMapper.readTree(body1).get("nextIdAfter").asText();

      // 2페이지: 알림2 반환, hasNext=true
      MvcResult page2 = mockMvc.perform(get("/api/notifications")
              .param("limit", "1")
              .param("cursor", cursor1)
              .param("idAfter", idAfter1)
              .with(user(principal)))
          .andExpect(status().isOk())
          .andExpect(jsonPath("$.data.length()").value(1))
          .andExpect(jsonPath("$.data[0].title").value("알림2"))
          .andExpect(jsonPath("$.hasNext").value(true))
          .andReturn();

      String body2 = page2.getResponse().getContentAsString();
      String cursor2 = objectMapper.readTree(body2).get("nextCursor").asText();
      String idAfter2 = objectMapper.readTree(body2).get("nextIdAfter").asText();

      // 3페이지: 알림1 반환, hasNext=false, nextCursor 없음
      mockMvc.perform(get("/api/notifications")
              .param("limit", "1")
              .param("cursor", cursor2)
              .param("idAfter", idAfter2)
              .with(user(principal)))
          .andDo(print())
          .andExpect(status().isOk())
          .andExpect(jsonPath("$.data.length()").value(1))
          .andExpect(jsonPath("$.data[0].title").value("알림1"))
          .andExpect(jsonPath("$.hasNext").value(false))
          .andExpect(jsonPath("$.nextCursor").doesNotExist())
          .andExpect(jsonPath("$.nextIdAfter").doesNotExist());
    }
  }
}
