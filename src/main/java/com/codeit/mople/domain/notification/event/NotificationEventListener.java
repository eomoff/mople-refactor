package com.codeit.mople.domain.notification.event;

import com.codeit.mople.domain.directmessage.event.DirectMessageReceivedEvent;
import com.codeit.mople.domain.follow.service.FollowService;
import com.codeit.mople.domain.notification.entity.NotificationTemplate;
import com.codeit.mople.domain.notification.entity.NotificationType;
import com.codeit.mople.domain.notification.service.NotificationCreator;
import com.codeit.mople.domain.playlist.event.PlaylistCreatedEvent;
import com.codeit.mople.domain.playlist.event.PlaylistSubscribedEvent;
import com.codeit.mople.domain.review.event.ReviewWrittenEvent;
import com.codeit.mople.domain.user.admin.enums.ForceLogoutReason;
import com.codeit.mople.domain.user.admin.event.UserAccountStatusChangedEvent;
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;
import org.springframework.transaction.event.TransactionPhase;
import org.springframework.transaction.event.TransactionalEventListener;

@Slf4j
@Component
@RequiredArgsConstructor
public class NotificationEventListener {

  private final NotificationCreator notificationCreator;
  private final FollowService followService;

  @Async
  @TransactionalEventListener(phase = TransactionPhase.AFTER_COMMIT)
  public void handleUserAccountStatusChanged(UserAccountStatusChangedEvent event) {
    log.debug("계정 상태 변경 알림 처리 시작 - userId: {}, reason: {}", event.userId(), event.reason());

    // 알림 요구사항에는 권한 변경만 있어서 계정 잠금과 잠금 해제는 알림을 만들지 않는다.
    if (event.reason() != ForceLogoutReason.ROLE_CHANGE) {
      return;
    }

    // 권한 변경은 가리킬 대상이 없어서 변경될 때마다 알림을 만든다.
    notificationCreator.createNotification(
        event.userId(),
        new NotificationTemplate(
            "권한이 변경되었습니다.",
            "관리자에 의해 권한이 변경되었습니다. 다시 로그인해주세요."
        ),
        NotificationType.ROLE_CHANGED,
        UUID.randomUUID().toString()
    );
  }

  @Async
  @TransactionalEventListener(phase = TransactionPhase.AFTER_COMMIT)
  public void handlePlaylistSubscribed(PlaylistSubscribedEvent event) {
    log.debug("플레이리스트 구독 알림 처리 시작 - ownerId: {}", event.ownerId());
    // 같은 사람이 같은 플레이리스트를 다시 구독해도 알림은 한 번만 만든다.
    notificationCreator.createNotification(
        event.ownerId(),
        new NotificationTemplate(
            "플레이리스트에 새 구독자가 생겼습니다.",
            event.subscriberName() + "님이 " + event.playlistTitle() + "을(를) 구독했습니다."
        ),
        NotificationType.PLAYLIST_SUBSCRIBED,
        event.playlistId() + ":" + event.subscriberId()
    );
  }

  @Async
  @TransactionalEventListener(phase = TransactionPhase.AFTER_COMMIT)
  public void handleDirectMessageReceived(DirectMessageReceivedEvent event) {
    log.debug("DM 수신 알림 처리 시작 - receiverId: {}", event.receiverId());
    notificationCreator.createNotification(
        event.receiverId(),
        new NotificationTemplate(
            "새로운 메시지가 도착했습니다.",
            event.senderName() + ": " + event.messageContent()
        ),
        NotificationType.DIRECT_MESSAGE_RECEIVED,
        event.directMessageId().toString()
    );
  }

  @Async
  @TransactionalEventListener(phase = TransactionPhase.AFTER_COMMIT)
  public void handlePlaylistCreated(PlaylistCreatedEvent event) {
    log.debug("플레이리스트 생성 팔로워 알림 처리 시작 - ownerId: {}", event.ownerId());
    followService.getFollowerIds(event.ownerId())
        .forEach(followerId -> notificationCreator.createNotification(
            followerId,
            new NotificationTemplate(
                event.ownerName() + "님의 새로운 활동이 있습니다.",
                "새 플레이리스트를 만들었습니다."
            ),
            NotificationType.FOLLOWEE_ACTIVITY,
            event.playlistId().toString()
        ));
  }

  @Async
  @TransactionalEventListener(phase = TransactionPhase.AFTER_COMMIT)
  public void handleReviewWritten(ReviewWrittenEvent event) {
    log.debug("리뷰 작성 팔로워 알림 처리 시작 - authorId: {}", event.authorId());
    followService.getFollowerIds(event.authorId())
        .forEach(followerId -> notificationCreator.createNotification(
            followerId,
            new NotificationTemplate(
                event.authorName() + "님의 새로운 활동이 있습니다.",
                "리뷰를 작성했습니다."
            ),
            NotificationType.FOLLOWEE_ACTIVITY,
            event.reviewId().toString()
        ));
  }
}
