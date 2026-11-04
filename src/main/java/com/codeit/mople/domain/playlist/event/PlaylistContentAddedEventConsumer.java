package com.codeit.mople.domain.playlist.event;

import com.codeit.mople.domain.notification.entity.NotificationTemplate;
import com.codeit.mople.domain.notification.entity.NotificationType;
import com.codeit.mople.domain.notification.service.NotificationCreator;
import com.codeit.mople.domain.playlist.service.PlaylistService;
import com.codeit.mople.global.config.KafkaProperties;
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
public class PlaylistContentAddedEventConsumer {

  private final PlaylistService playlistService;

  private final NotificationCreator notificationCreator;

  @Transactional
  @KafkaListener(topics = "${spring.kafka.topics.playlist-content-added}")
  public void handle(PlaylistContentAddedEvent event) {
    playlistService.getSubscriberIds(event.playlistId())
        .forEach(subscriberId -> notificationCreator.createNotification(
            event.eventId(),
            subscriberId,
            new NotificationTemplate(
                "구독한 플레이리스트에 새 콘텐츠가 추가되었습니다.",
                event.playlistTitle() + "에 새 콘텐츠가 추가되었습니다."
            ),
            NotificationType.PLAYLIST_CONTENT_ADDED,
            event.playlistContentId().toString()
        ));

    log.info("플레이리스트 콘텐츠 추가 이벤트 처리 완료: playlistId={}", event.playlistId());
  }

}
