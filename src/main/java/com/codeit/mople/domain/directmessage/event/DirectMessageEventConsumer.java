package com.codeit.mople.domain.directmessage.event;

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
public class DirectMessageEventConsumer {

  private final NotificationCreator notificationCreator;

  @Transactional
  @KafkaListener(topics = "${spring.kafka.topics.direct-message-received}")
  public void handle(DirectMessageReceivedEvent event) {
    notificationCreator.createNotification(
        event.receiverId(),
        new NotificationTemplate(
            "새로운 메시지가 도착했습니다.",
            event.senderName() + ": " + event.messageContent()
        ),
        NotificationType.DIRECT_MESSAGE_RECEIVED,
        event.directMessageId().toString()
    );

    log.info("DM 수신 이벤트 처리 완료: directMessageId={}", event.directMessageId());
  }

}
