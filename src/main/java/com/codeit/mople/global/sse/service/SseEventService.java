package com.codeit.mople.global.sse.service;

import com.codeit.mople.domain.directmessage.dto.response.DirectMessageDto;
import com.codeit.mople.domain.directmessage.entity.DirectMessage;
import com.codeit.mople.domain.directmessage.exception.DirectMessageErrorCode;
import com.codeit.mople.domain.directmessage.exception.DirectMessageException;
import com.codeit.mople.domain.directmessage.repository.DirectMessageRepository;
import com.codeit.mople.domain.notification.dto.response.NotificationResponse;
import com.codeit.mople.domain.notification.entity.Notification;
import com.codeit.mople.domain.notification.exception.NotificationErrorCode;
import com.codeit.mople.domain.notification.exception.NotificationException;
import com.codeit.mople.domain.notification.repository.NotificationRepository;
import java.util.Map;
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
@RequiredArgsConstructor
public class SseEventService {

  private final DirectMessageRepository directMessageRepository;
  private final NotificationRepository notificationRepository;

  @Transactional(readOnly = true)
  public DirectMessageDto getDirectMessageDto(UUID directMessageId) {
    DirectMessage directMessage = directMessageRepository.findById(directMessageId)
        .orElseThrow(() ->
            new DirectMessageException(
                DirectMessageErrorCode.DIRECT_MESSAGE_NOT_FOUND,
                Map.of("directMessageId", directMessageId)
            )
        );

    return DirectMessageDto.from(directMessage);
  }

  @Transactional(readOnly = true)
  public NotificationResponse getNotificationResponse(UUID notificationId) {
    Notification notification = notificationRepository.findById(notificationId)
        .orElseThrow(() ->
            new NotificationException(
                NotificationErrorCode.NOTIFICATION_NOT_FOUND,
                Map.of("notificationId", notificationId)
            )
        );

    return NotificationResponse.from(notification);
  }

}
