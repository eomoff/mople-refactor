package com.codeit.mople.domain.review.event;

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
public class ReviewEventConsumer {

  private final ProcessedEventRepository processedEventRepository;

  private final NotificationCreator notificationCreator;

  private final FollowService followService;

  @Transactional
  @KafkaListener(topics = "${spring.kafka.topics.review-written}")
  public void handle(ReviewWrittenEvent event) {
    log.debug("리뷰 작성 팔로워 알림 시도: reviewId={}, authorId={}",
        event.reviewId(), event.authorId());

    if (checkAndRecordProcessedEvent(event.eventId())) {
      return;
    }

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

    log.info("리뷰 작성 팔로워 알림 완료: reviewId={}, authorId={}",
        event.reviewId(), event.authorId());
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
