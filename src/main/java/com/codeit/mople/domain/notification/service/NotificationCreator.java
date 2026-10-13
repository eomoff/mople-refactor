package com.codeit.mople.domain.notification.service;

import com.codeit.mople.domain.notification.entity.Notification;
import com.codeit.mople.domain.notification.entity.NotificationTemplate;
import com.codeit.mople.domain.notification.entity.NotificationType;
import com.codeit.mople.domain.notification.event.NotificationCreatedEvent;
import com.codeit.mople.domain.notification.exception.NotificationErrorCode;
import com.codeit.mople.domain.notification.exception.NotificationException;
import com.codeit.mople.domain.notification.repository.NotificationRepository;
import com.codeit.mople.domain.user.repository.UserRepository;
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Slf4j
@Service
@RequiredArgsConstructor
public class NotificationCreator {

  private final NotificationRepository notificationRepository;
  private final UserRepository userRepository;

  private final ApplicationEventPublisher eventPublisher;

  @Transactional
  public void createNotification(
      UUID receiverId,
      NotificationTemplate template,
      NotificationType type,
      String targetKey
  ) {
    log.debug("알림 생성 요청: receiverId={}, type={}", receiverId, type);
    // 1. 요청 받은 수신자id가 실제 존재하는지 검증
    if (!userRepository.existsById(receiverId)) {
      throw new NotificationException(NotificationErrorCode.NOTIFICATION_USER_NOT_FOUND);
    }

    // 2. 이미 생성된 알림이 존재하는지 검증
    String hashId = Notification.hashIdOf(receiverId, type, targetKey);
    if (notificationRepository.existsByHashId(hashId)) {
      log.debug("중복 알림 생략: receiverId={}, type={}", receiverId, type);
      return;
    }

    // 3. 알림 저장
    Notification saved =
        notificationRepository.save(Notification.create(receiverId, template, type, hashId));

    // 4. 이벤트 발행
    eventPublisher.publishEvent(
        new NotificationCreatedEvent(UUID.randomUUID(), saved.getCreatedAt(), receiverId, saved.getId()));

    log.info("알림 생성 완료: receiverId={}, type={}", receiverId, type);
  }
}
