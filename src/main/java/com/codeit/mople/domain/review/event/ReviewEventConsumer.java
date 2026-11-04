package com.codeit.mople.domain.review.event;

import com.codeit.mople.domain.follow.service.FollowService;
import com.codeit.mople.domain.notification.entity.NotificationTemplate;
import com.codeit.mople.domain.notification.entity.NotificationType;
import com.codeit.mople.domain.notification.service.NotificationCreator;
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
public class ReviewEventConsumer {

  private final NotificationCreator notificationCreator;

  private final FollowService followService;

  @Transactional
  @KafkaListener(topics = "${spring.kafka.topics.review-written}")
  public void handle(ReviewWrittenEvent event) {
    log.debug("리뷰 작성 팔로워 알림 시도: reviewId={}, authorId={}",
        event.reviewId(), event.authorId());

    followService.getFollowerIds(event.authorId())
        .forEach(followerId -> notificationCreator.createNotification(
            event.eventId(),
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

}
