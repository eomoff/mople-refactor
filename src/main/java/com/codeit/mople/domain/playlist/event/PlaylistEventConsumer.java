package com.codeit.mople.domain.playlist.event;

import com.codeit.mople.domain.follow.service.FollowService;
import com.codeit.mople.domain.notification.entity.NotificationTemplate;
import com.codeit.mople.domain.notification.entity.NotificationType;
import com.codeit.mople.domain.notification.service.NotificationCreator;
import com.codeit.mople.global.config.KafkaProperties;
import com.codeit.mople.global.event.processed.ProcessedEventRepository;
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Slf4j
@Component
@RequiredArgsConstructor
@ConditionalOnProperty(prefix = KafkaProperties.PREFIX, name = "enabled", havingValue = "true")
public class PlaylistEventConsumer {

  private final NotificationCreator notificationCreator;
  private final FollowService followService;
  private final ProcessedEventRepository processedEventRepository;

  @Transactional
  @KafkaListener(topics = "${spring.kafka.topics.playlist-subscribed}")
  public void handle(PlaylistSubscribedEvent event) {
    if (checkAndRecordProcessedEvent(event.eventId())) {
      return;
    }

    notificationCreator.createNotification(
        event.ownerId(),
        new NotificationTemplate(
            "플레이리스트에 새 구독자가 생겼습니다.",
            event.subscriberName() + "님이 " + event.playlistTitle() + "을(를) 구독했습니다."
        ),
        NotificationType.PLAYLIST_SUBSCRIBED,
        event.playlistId() + ":" + event.subscriberId()
    );

    log.info("플레이리스트 구독 이벤트 처리 완료: playlistId={}", event.playlistId());
  }

  @Transactional
  @KafkaListener(topics = "${spring.kafka.topics.playlist-created}")
  public void handle(PlaylistCreatedEvent event) {
    if (checkAndRecordProcessedEvent(event.eventId())) {
      return;
    }

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

    log.info("플레이리스트 생성 이벤트 처리 완료: playlistId={}", event.playlistId());
  }

  private boolean checkAndRecordProcessedEvent(UUID eventId) {
    // 이미 해당 eventId가 존재하면 스킵
    int inserted = processedEventRepository.insertIfAbsent(eventId);

    if (inserted == 0) {
      log.info("이미 처리된 이벤트입니다: eventId={}", eventId);
      return true;
    }

    return false;
  }

}
