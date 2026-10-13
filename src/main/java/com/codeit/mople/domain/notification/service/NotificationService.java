package com.codeit.mople.domain.notification.service;

import com.codeit.mople.domain.notification.dto.request.NotificationCursorRequest;
import com.codeit.mople.domain.notification.dto.response.CursorResponseNotificationDto;
import com.codeit.mople.domain.notification.dto.response.NotificationResponse;
import com.codeit.mople.domain.notification.entity.Notification;
import com.codeit.mople.domain.notification.exception.NotificationErrorCode;
import com.codeit.mople.domain.notification.exception.NotificationException;
import com.codeit.mople.domain.notification.repository.NotificationRepository;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Slf4j
@Service
@RequiredArgsConstructor
@Transactional(readOnly = true)
public class NotificationService {

  private final NotificationRepository notificationRepository;

  // 알림 목록조회
  public CursorResponseNotificationDto getNotifications(UUID receiverId,
      NotificationCursorRequest request) {
    log.debug("알림 목록 조회 요청 - receiverId: {}, limit: {}, cursor: {}", receiverId,
        request.limit(), request.cursor());

    request.validateCursorPair();
    Instant cursorTime = request.parseCursorToInstant();
    List<Notification> notifications = notificationRepository.findNotificationByCursor(
        receiverId, cursorTime, request.idAfter(), request.limit());

    boolean hasNext = notifications.size() > request.limit();
    List<Notification> sliced = hasNext ? notifications.subList(0, request.limit()) : notifications;

    List<NotificationResponse> data = sliced.stream()
        .map(NotificationResponse::from)
        .toList();

    String nextCursor = null;
    UUID nextIdAfter = null;

    if (hasNext && !sliced.isEmpty()) {
      Notification lastItem = sliced.get(sliced.size() - 1);
      nextCursor = lastItem.getCreatedAt().toString();
      nextIdAfter = lastItem.getId();
    }

    long unreadCount = notificationRepository.countByReceiver_Id(receiverId);
    log.info("알림 목록 조회 완료 - receiverId: {}", receiverId);

    return new CursorResponseNotificationDto(
        data,
        nextCursor,
        nextIdAfter,
        hasNext,
        unreadCount,
        "createdAt",
        "DESCENDING"
    );
  }

  // 알림 확인
  @Transactional
  public void readNotification(UUID notificationId, UUID receiverId) {
    log.debug("알림 확인: notificationId={}, receiverId={}", notificationId, receiverId);
    // 1. 알림이 존재하는지 조회
    Notification notification = notificationRepository.findById(notificationId)
        .orElseThrow(() -> new NotificationException(NotificationErrorCode.NOTIFICATION_NOT_FOUND));

    // 2. 존재 확인한 알림의 수신자와 요청 수신자가 동일한지 검사
    if (!notification.getReceiverId().equals(receiverId)) {
      throw new NotificationException(NotificationErrorCode.NOTIFICATION_FORBIDDEN);
    }

    // 3. 알림 readAt 컬럼 기록
    notification.markAsRead();
    log.info("알림 확인 완료: notificationId={}, receiverId={}", notificationId, receiverId);
  }
}
