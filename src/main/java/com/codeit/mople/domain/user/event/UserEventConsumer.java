package com.codeit.mople.domain.user.event;

import com.codeit.mople.domain.notification.entity.NotificationTemplate;
import com.codeit.mople.domain.notification.entity.NotificationType;
import com.codeit.mople.domain.notification.service.NotificationCreator;
import com.codeit.mople.domain.user.admin.enums.ForceLogoutReason;
import com.codeit.mople.domain.user.admin.event.UserAccountStatusChangedEvent;
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
public class UserEventConsumer {

  private final NotificationCreator notificationCreator;
  private final ProcessedEventRepository processedEventRepository;

  @Transactional
  @KafkaListener(topics = "${spring.kafka.topics.user-account-status-changed}")
  public void handle(UserAccountStatusChangedEvent event) {
    // 알림 요구사항에는 권한 변경만 있어서 계정 잠금과 잠금 해제는 알림을 만들지 않는다.
    if (event.reason() != ForceLogoutReason.ROLE_CHANGE) {
      return;
    }

    if (checkAndRecordProcessedEvent(event.eventId())) {
      return;
    }

    // 권한 변경은 가리킬 대상이 없어서 변경될 때마다 알림을 만든다.
    notificationCreator.createNotification(
        event.userId(),
        new NotificationTemplate(
            "권한이 변경되었습니다.",
            "관리자에 의해 권한이 변경되었습니다. 다시 로그인해주세요."
        ),
        NotificationType.ROLE_CHANGED,
        UUID.randomUUID().toString()
    );

    log.info("권한 변경 이벤트 처리 완료: userId={}", event.userId());
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
