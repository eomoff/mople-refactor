package com.codeit.mople.domain.playlist.event;

import com.codeit.mople.global.config.KafkaProperties;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.event.TransactionPhase;
import org.springframework.transaction.event.TransactionalEventListener;

@Component
@ConditionalOnProperty(prefix = KafkaProperties.PREFIX, name = "enabled", havingValue = "true")
public class PlaylistEventProducer {

  private final KafkaTemplate<String, Object> kafkaTemplate;

  private final String playlistSearchIndexTopic;

  public PlaylistEventProducer(
      KafkaTemplate<String, Object> kafkaTemplate,
      KafkaProperties properties
  ) {
    this.kafkaTemplate = kafkaTemplate;
    this.playlistSearchIndexTopic = properties.topics().playlistSearchIndex();
  }

  @TransactionalEventListener(phase = TransactionPhase.AFTER_COMMIT)
  public void on(PlaylistSearchIndexEvent event) {
    kafkaTemplate.send(playlistSearchIndexTopic, event.playlistId().toString(), event);
  }

  @TransactionalEventListener(phase = TransactionPhase.AFTER_COMMIT)
  public void on(PlaylistSearchIndexDeleteEvent event) {
    kafkaTemplate.send(
        playlistSearchIndexTopic,
        event.playlistId().toString(),
        event
    );
  }

}
