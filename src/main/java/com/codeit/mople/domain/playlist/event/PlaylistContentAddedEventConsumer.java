package com.codeit.mople.domain.playlist.event;

import com.codeit.mople.domain.notification.entity.NotificationTemplate;
import com.codeit.mople.domain.notification.entity.NotificationType;
import com.codeit.mople.domain.notification.service.NotificationCreator;
import com.codeit.mople.domain.playlist.service.PlaylistService;
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
public class PlaylistContentAddedEventConsumer {

  private final PlaylistService playlistService;

  private final NotificationCreator notificationCreator;

  private final ProcessedEventRepository processedEventRepository;

  @Transactional
  @KafkaListener(topics = "${spring.kafka.topics.playlist-content-added}")
  public void handle(PlaylistContentAddedEvent event) {
    if (checkAndRecordProcessedEvent(event.eventId())) {
      return;
    }

    playlistService.getSubscriberIds(event.playlistId())
        .forEach(subscriberId -> notificationCreator.createNotification(
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
