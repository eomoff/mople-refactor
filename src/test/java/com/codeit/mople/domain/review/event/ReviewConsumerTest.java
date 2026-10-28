package com.codeit.mople.domain.review.event;

import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import com.codeit.mople.domain.content.repository.ContentRepository;
import com.codeit.mople.domain.follow.service.FollowService;
import com.codeit.mople.domain.notification.entity.NotificationTemplate;
import com.codeit.mople.domain.notification.entity.NotificationType;
import com.codeit.mople.domain.notification.service.NotificationCreator;
import com.codeit.mople.global.event.processed.ProcessedEventRepository;
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
public class ReviewConsumerTest {

  @Mock
  private ContentRepository contentRepository;

  @Mock
  private ProcessedEventRepository processedEventRepository;

  @Mock
  private NotificationCreator notificationCreator;

  @Mock
  private FollowService followService;

  @InjectMocks
  private ReviewEventConsumer eventConsumer;

  private UUID contentId;

  @BeforeEach
  void setUp() {
    contentId = UUID.randomUUID();
  }

  @Nested
  @DisplayName("리뷰 생성 이벤트(콘텐츠 개수, 평균 평점 업데이트)")
  class CreatedEvent {

    @Test
    @DisplayName("리뷰 생성 이벤트 성공")
    void handle_success() {
      // given

      // BeforeEach에서 contentId를 초기화

      UUID eventId = UUID.randomUUID();

      ReviewCreatedEvent event = new ReviewCreatedEvent(eventId, Instant.now(), contentId, 4.0);

      given(processedEventRepository.insertIfAbsent(eventId))
          .willReturn(1);

      // when
      eventConsumer.handle(event);

      // then
      verify(processedEventRepository).insertIfAbsent(eventId);
      verify(contentRepository).increaseRating(contentId, 4.0);
    }

  }

  @Nested
  @DisplayName("리뷰 수정 이벤트(콘텐츠 평균 평점 업데이트)")
  class UpdatedEvent {

    @Test
    @DisplayName("리뷰 수정 이벤트 성공")
    void handle_success() {
      // given

      // BeforeEach에서 contentId를 초기화

      UUID eventId = UUID.randomUUID();

      ReviewUpdatedEvent event = new ReviewUpdatedEvent(eventId, Instant.now(), contentId, 4.0,
          5.0);

      given(processedEventRepository.insertIfAbsent(eventId))
          .willReturn(1);

      // when
      eventConsumer.handle(event);

      // then
      verify(processedEventRepository).insertIfAbsent(eventId);
      verify(contentRepository).updateRating(contentId, 4.0, 5.0);
    }

  }

  @Nested
  @DisplayName("리뷰 삭제 이벤트(콘텐츠 개수, 평균 평점 업데이트)")
  class ReviewDeleted {

    @Test
    @DisplayName("리뷰 삭제 이벤트 성공")
    void handle_success() {
      // given

      // BeforeEach에서 contentId를 초기화

      UUID eventId = UUID.randomUUID();

      ReviewDeletedEvent event = new ReviewDeletedEvent(eventId, Instant.now(), contentId, 4.0);

      given(processedEventRepository.insertIfAbsent(eventId))
          .willReturn(1);

      // when
      eventConsumer.handle(event);

      // then
      verify(processedEventRepository).insertIfAbsent(eventId);
      verify(contentRepository).decreaseRating(contentId, 4.0);
    }

  }

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

      given(processedEventRepository.insertIfAbsent(eventId)).willReturn(1);
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

    @Test
    @DisplayName("이미 처리된 이벤트는 알림을 생성하지 않고 스킵한다")
    void handle_success_already_processed_skip() {
      // given
      UUID eventId = UUID.randomUUID();
      ReviewWrittenEvent event = new ReviewWrittenEvent(
          eventId, Instant.now(), UUID.randomUUID(), UUID.randomUUID(), "리뷰작성자"
      );

      given(processedEventRepository.insertIfAbsent(eventId)).willReturn(0);

      // when
      eventConsumer.handle(event);

      // then
      verify(processedEventRepository).insertIfAbsent(eventId);
      verifyNoInteractions(followService, notificationCreator);
    }

  }

}
