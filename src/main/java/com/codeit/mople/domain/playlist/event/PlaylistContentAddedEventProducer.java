package com.codeit.mople.domain.playlist.event;

import com.codeit.mople.global.config.KafkaProperties;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.event.TransactionPhase;
import org.springframework.transaction.event.TransactionalEventListener;

@Slf4j
@Component
@ConditionalOnProperty(prefix = KafkaProperties.PREFIX, name = "enabled", havingValue = "true")
public class PlaylistContentAddedEventProducer {

  private final KafkaTemplate<String, Object> kafkaTemplate;
  private final String topic;

  public PlaylistContentAddedEventProducer(KafkaTemplate<String, Object> kafkaTemplate,
      KafkaProperties kafkaProperties) {
    this.kafkaTemplate = kafkaTemplate;
    this.topic = kafkaProperties.topics().playlistContentAdded();
  }

  @TransactionalEventListener(phase = TransactionPhase.AFTER_COMMIT)
  public void on(PlaylistContentAddedEvent event) {
    String key = event.playlistId().toString();

    log.debug("플레이리스트 콘텐츠 추가 이벤트 발행: playlistContentId={}, playlistId={}, eventId={}",
        event.playlistContentId(), event.playlistId(), event.eventId());
    kafkaTemplate.send(topic, key, event);
  }
}
