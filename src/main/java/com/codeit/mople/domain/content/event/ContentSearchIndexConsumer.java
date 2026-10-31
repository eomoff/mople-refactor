package com.codeit.mople.domain.content.event;

import com.codeit.mople.domain.content.repository.search.ContentDocument;
import com.codeit.mople.domain.content.repository.search.ContentSearchRepository;
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
@ConditionalOnProperty(prefix = KafkaProperties.PREFIX, name = "enabled", havingValue = "true")
@KafkaListener(topics = "${spring.kafka.topics.content-search-index}")
public class ContentSearchIndexConsumer {

  private final ContentSearchRepository contentSearchRepository;

  @KafkaHandler
  public void handle(ContentSearchIndexEvent event) {
    log.debug("콘텐츠 검색 인덱스 반영 시도: contentId={}",
        event.contentId());

    contentSearchRepository.save(
        new ContentDocument(
            event.contentId(),
            event.title(),
            event.type(),
            event.rating(),
            event.watcherCount(),
            event.createdAt()
        )
    );

    log.info("콘텐츠 검색 인덱스 반영 완료: contentId={}",
        event.contentId());
  }

  @KafkaHandler
  public void handle(ContentSearchIndexDeleteEvent event) {
    log.debug("콘텐츠 검색 인덱스 삭제 시도: contentId={}",
        event.contentId());

    contentSearchRepository.deleteById(event.contentId());

    log.info("콘텐츠 검색 인덱스 삭제 완료: contentId={}",
        event.contentId());
  }
}