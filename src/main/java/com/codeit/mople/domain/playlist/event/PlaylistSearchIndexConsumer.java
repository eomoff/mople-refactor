package com.codeit.mople.domain.playlist.event;

import com.codeit.mople.domain.playlist.repository.search.PlaylistDocument;
import com.codeit.mople.domain.playlist.repository.search.PlaylistSearchRepository;
import com.codeit.mople.global.config.KafkaProperties;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
@ConditionalOnProperty(
    prefix = KafkaProperties.PREFIX,
    name = "enabled",
    havingValue = "true"
)
@KafkaListener(topics = "${spring.kafka.topics.playlist-search-index}")
public class PlaylistSearchIndexConsumer {

  private final PlaylistSearchRepository playlistSearchRepository;

  @KafkaHandler
  public void handle(PlaylistSearchIndexEvent event) {
    log.debug("플레이리스트 검색 인덱스 반영 시도: playlistId={}",
        event.playlistId());

    playlistSearchRepository.save(
        new PlaylistDocument(
            event.playlistId(),
            event.title(),
            event.subscribeCount(),
            event.updatedAt()
        )
    );

    log.info("플레이리스트 검색 인덱스 반영 완료: playlistId={}",
        event.playlistId());
  }

  @KafkaHandler
  public void handle(PlaylistSearchIndexDeleteEvent event) {
    log.debug("플레이리스트 검색 인덱스 삭제 시도: playlistId={}",
        event.playlistId());

    playlistSearchRepository.deleteById(event.playlistId());

    log.info("플레이리스트 검색 인덱스 삭제 완료: playlistId={}",
        event.playlistId());
  }
}