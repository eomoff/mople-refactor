package com.codeit.mople.domain.playlist.event;

import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import com.codeit.mople.domain.follow.service.FollowService;
import com.codeit.mople.domain.notification.entity.NotificationTemplate;
import com.codeit.mople.domain.notification.entity.NotificationType;
import com.codeit.mople.domain.notification.service.NotificationCreator;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class PlaylistEventConsumerTest {

  @Mock
  private NotificationCreator notificationCreator;

  @Mock
  private FollowService followService;

  @InjectMocks
  private PlaylistEventConsumer eventConsumer;

  private UUID playlistId;
  private UUID ownerId;

  @BeforeEach
  void setUp() {
    playlistId = UUID.randomUUID();
    ownerId = UUID.randomUUID();
  }

  @Nested
  @DisplayName("플레이리스트 구독 이벤트")
  class SubscribedEvent {

    @Test
    @DisplayName("플레이리스트 구독 이벤트 수신 시 owner에게 PLAYLIST_SUBSCRIBED 알림을 생성한다")
    void handle_playlist_subscribed() {
      // given
      UUID eventId = UUID.randomUUID();
      UUID subscriberId = UUID.randomUUID();
      PlaylistSubscribedEvent event = new PlaylistSubscribedEvent(
          eventId, Instant.now(), ownerId, playlistId, subscriberId, "구독자유저", "테스트 플레이리스트"
      );

      // when
      eventConsumer.handle(event);

      // then
      verify(notificationCreator).createNotification(
          eventId,
          ownerId,
          new NotificationTemplate(
              "플레이리스트에 새 구독자가 생겼습니다.",
              "구독자유저님이 테스트 플레이리스트을(를) 구독했습니다."
          ),
          NotificationType.PLAYLIST_SUBSCRIBED,
          playlistId + ":" + subscriberId
      );
    }
  }

  @Nested
  @DisplayName("플레이리스트 생성 이벤트")
  class CreatedEvent {

    @Test
    @DisplayName("플레이리스트 생성 이벤트 수신 시 모든 팔로워에게 FOLLOWEE_ACTIVITY 알림을 생성한다")
    void handle_playlist_created() {
      // given
      UUID eventId = UUID.randomUUID();
      UUID followerA = UUID.randomUUID();
      UUID followerB = UUID.randomUUID();
      PlaylistCreatedEvent event = new PlaylistCreatedEvent(
          eventId, Instant.now(), playlistId, ownerId, "플레이리스트_생성자", "새 플레이리스트"
      );

      given(followService.getFollowerIds(ownerId)).willReturn(List.of(followerA, followerB));

      // when
      eventConsumer.handle(event);

      // then
      NotificationTemplate template = new NotificationTemplate(
          "플레이리스트_생성자님의 새로운 활동이 있습니다.",
          "새 플레이리스트를 만들었습니다."
      );
      verify(notificationCreator).createNotification(
          eventId, followerA, template, NotificationType.FOLLOWEE_ACTIVITY, playlistId.toString());
      verify(notificationCreator).createNotification(
          eventId, followerB, template, NotificationType.FOLLOWEE_ACTIVITY, playlistId.toString());
      verifyNoMoreInteractions(notificationCreator);
    }

    @Test
    @DisplayName("팔로워가 없으면 플레이리스트 생성 알림을 생성하지 않는다")
    void handle_playlist_created_without_followers() {
      // given
      UUID eventId = UUID.randomUUID();
      PlaylistCreatedEvent event = new PlaylistCreatedEvent(
          eventId, Instant.now(), playlistId, ownerId, "플레이리스트_생성자", "새 플레이리스트"
      );

      given(followService.getFollowerIds(ownerId)).willReturn(List.of());

      // when
      eventConsumer.handle(event);

      // then
      verify(followService).getFollowerIds(ownerId);
      verifyNoInteractions(notificationCreator);
    }
  }
}
