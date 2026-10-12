package com.codeit.mople.domain.notification.service;

import com.codeit.mople.domain.notification.entity.Notification;
import com.codeit.mople.domain.notification.event.NotificationCreatedEvent;
import com.codeit.mople.domain.notification.repository.NotificationRepository;
import com.codeit.mople.domain.user.entity.User;
import com.codeit.mople.domain.user.exception.UserErrorCode;
import com.codeit.mople.domain.user.exception.UserException;
import com.codeit.mople.domain.user.repository.UserRepository;
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.dao.TransientDataAccessException;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Recover;
import org.springframework.retry.annotation.Retryable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Slf4j
@Service
@RequiredArgsConstructor
public class NotificationCreator {

  private final NotificationRepository notificationRepository;
  private final UserRepository userRepository;

  private final ApplicationEventPublisher eventPublisher;

  @Retryable(retryFor = TransientDataAccessException.class, maxAttempts = 3, backoff = @Backoff(delay = 1000, multiplier = 2))
  @Transactional
  public void createNotification(UUID receiverId, String title, String content,
      NotificationType type) {
    log.debug("알림 생성 요청 - receiverId: {}, type: {}", receiverId, type);
    User receiver = userRepository.findById(receiverId)
        .orElseThrow(() -> new UserException(UserErrorCode.USER_NOT_FOUND));
    Notification notification =
        notificationRepository.save(Notification.create(receiver, title, content, type));

    eventPublisher.publishEvent(
        new NotificationCreatedEvent(UUID.randomUUID(), notification.getCreatedAt(), receiverId,
            notification.getId()));
    log.info("알림 생성 완료 - receiverId: {}, type: {}", receiverId, type);
  }

  @Recover
  public void recoverCreateNotification(TransientDataAccessException e, UUID receiverId,
      String title, String content, NotificationType type) {
    log.error("알림 생성 최종 실패 (3회 재시도 소진) - receiverId: {}, type: {}", receiverId, type, e);
  }
}
