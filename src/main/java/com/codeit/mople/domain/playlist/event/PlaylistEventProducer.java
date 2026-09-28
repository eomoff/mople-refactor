package com.codeit.mople.domain.playlist.event;

import com.codeit.mople.global.config.KafkaProperties;
import com.codeit.mople.global.event.KafkaEventPublisher;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import org.springframework.transaction.event.TransactionPhase;
import org.springframework.transaction.event.TransactionalEventListener;

@Component
@ConditionalOnProperty(prefix = KafkaProperties.PREFIX, name = "enabled", havingValue = "true")
public class PlaylistEventProducer {

  private final KafkaEventPublisher eventPublisher;

  private final String playlistEventTopic;
  private final String playlistSearchIndexTopic;

  public PlaylistEventProducer(
      KafkaEventPublisher eventPublisher,
      KafkaProperties properties
  ) {
    this.eventPublisher = eventPublisher;
    this.playlistEventTopic = properties.topics().playlistEvents();
    this.playlistSearchIndexTopic = properties.topics().playlistSearchIndex();
  }
  // 구독
  @TransactionalEventListener(phase = TransactionPhase.AFTER_COMMIT)
  public void on(PlaylistSubscribedEvent event) {
    eventPublisher.publish(playlistEventTopic, event.playlistId().toString(), event);
  }
  // 구독 취소
  @TransactionalEventListener(phase = TransactionPhase.AFTER_COMMIT)
  public void on(PlaylistUnsubscribedEvent event) {
    eventPublisher.publish(playlistEventTopic, event.playlistId().toString(), event);
  }

  @TransactionalEventListener(phase = TransactionPhase.AFTER_COMMIT)
  public void on(PlaylistSearchIndexEvent event) {
    eventPublisher.publish(playlistSearchIndexTopic, event.playlistId().toString(), event);
  }

  @TransactionalEventListener(phase = TransactionPhase.AFTER_COMMIT)
  public void on(PlaylistSearchIndexDeleteEvent event) {
    eventPublisher.publish(
        playlistSearchIndexTopic,
        event.playlistId().toString(),
        event
    );
  }

}
