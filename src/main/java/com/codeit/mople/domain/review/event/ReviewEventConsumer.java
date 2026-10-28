package com.codeit.mople.domain.review.event;

import com.codeit.mople.domain.content.repository.ContentRepository;
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

  private final ContentRepository contentRepository;

  private final ProcessedEventRepository processedEventRepository;

  private final NotificationCreator notificationCreator;

  private final FollowService followService;


  // 기본 전파레벨은 REQUIRED(트랜잭션 내에서 이 메서드가 호출되면 트랜잭션 새로 생성하지 않고 그 트랜잭션에 참여)
  @Transactional
  @KafkaListener(topics = "${spring.kafka.topics.review-created}")
  public void handle(ReviewCreatedEvent event) {
    log.debug("리뷰 생성 후 콘텐츠 통계 업데이트 시도: contentId={}, rating={}",
        event.contentId(), event.rating());

    if (checkAndRecordProcessedEvent(event.eventId())) {
      return;
    }

    contentRepository.increaseRating(
        event.contentId(),
        event.rating()
    );

    log.info("리뷰 생성 후 콘텐츠 통계 업데이트 완료: contentId={}, rating={}",
        event.contentId(), event.rating());
  }

  @Transactional
  @KafkaListener(topics = "${spring.kafka.topics.review-updated}")
  public void handle(ReviewUpdatedEvent event) {
    log.debug("리뷰 수정 후 콘텐츠 통계 업데이트 시도: contentId={}, oldRating={}, newRating={}",
        event.contentId(), event.oldRating(), event.newRating());

    if (checkAndRecordProcessedEvent(event.eventId())) {
      return;
    }

    contentRepository.updateRating(
        event.contentId(),
        event.oldRating(),
        event.newRating()
    );

    log.info("리뷰 수정 후 콘텐츠 통계 업데이트 완료: contentId={}, oldRating={}, newRating={}",
        event.contentId(), event.oldRating(), event.newRating());
  }

  @Transactional
  @KafkaListener(topics = "${spring.kafka.topics.review-deleted}")
  public void handle(ReviewDeletedEvent event) {
    log.debug("리뷰 삭제 후 콘텐츠 통계 업데이트 시도: contentId={}, rating={}",
        event.contentId(), event.rating());

    if (checkAndRecordProcessedEvent(event.eventId())) {
      return;
    }

    contentRepository.decreaseRating(
        event.contentId(),
        event.rating()
    );

    log.info("리뷰 삭제 후 콘텐츠 통계 업데이트 완료: contentId={}, rating={}",
        event.contentId(), event.rating());
  }

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
