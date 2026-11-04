package com.codeit.mople.domain.follow.event;

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
public class FollowEventConsumer {

  private final NotificationCreator notificationCreator;

  @Transactional
  @KafkaListener(topics = "${spring.kafka.topics.follow-created}")
  public void handle(FollowCreatedEvent event) {
    notificationCreator.createNotification(
        event.eventId(),
        event.followeeId(),
        new NotificationTemplate(
            "새로운 팔로워가 생겼습니다.",
            event.followerName() + "님이 팔로우했습니다."
        ),
        NotificationType.FOLLOW_CREATED,
        event.followerId().toString()
    );

    log.info("팔로우 생성 이벤트 처리 완료: followId={}", event.followId());
  }

}
