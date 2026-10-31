package com.codeit.mople.domain.review.event;

import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import com.codeit.mople.domain.follow.service.FollowService;
import com.codeit.mople.domain.notification.entity.NotificationTemplate;
import com.codeit.mople.domain.notification.entity.NotificationType;
import com.codeit.mople.domain.notification.service.NotificationCreator;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class ReviewConsumerTest {

  @Mock
  private NotificationCreator notificationCreator;

  @Mock
  private FollowService followService;

  @InjectMocks
  private ReviewEventConsumer eventConsumer;

  @Nested
  @DisplayName("리뷰 작성 이벤트(팔로워 알림)")
  class WrittenEvent {

    @Test
    @DisplayName("리뷰 작성 이벤트 수신 시 모든 팔로워에게 FOLLOWEE_ACTIVITY 알림을 생성한다")
    void handle_review_written() {
      // given
      UUID eventId = UUID.randomUUID();
      UUID reviewId = UUID.randomUUID();
      UUID authorId = UUID.randomUUID();
      UUID followerA = UUID.randomUUID();
      UUID followerB = UUID.randomUUID();
      ReviewWrittenEvent event = new ReviewWrittenEvent(
          eventId, Instant.now(), reviewId, authorId, "리뷰작성자"
      );

      given(followService.getFollowerIds(authorId)).willReturn(List.of(followerA, followerB));

      // when
      eventConsumer.handle(event);

      // then
      NotificationTemplate template = new NotificationTemplate(
          "리뷰작성자님의 새로운 활동이 있습니다.",
          "리뷰를 작성했습니다."
      );
      verify(notificationCreator).createNotification(
          followerA, template, NotificationType.FOLLOWEE_ACTIVITY, reviewId.toString());
      verify(notificationCreator).createNotification(
          followerB, template, NotificationType.FOLLOWEE_ACTIVITY, reviewId.toString());
      verifyNoMoreInteractions(notificationCreator);
    }

  }

}
