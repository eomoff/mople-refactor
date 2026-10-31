package com.codeit.mople.domain.user.event;

import com.codeit.mople.domain.user.repository.search.UserDocument;
import com.codeit.mople.domain.user.repository.search.UserSearchRepository;
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
@KafkaListener(topics = "${spring.kafka.topics.user-search-index}")
public class UserSearchIndexConsumer {

  private final UserSearchRepository userSearchRepository;

  @KafkaHandler
  public void handle(UserSearchIndexEvent event) {
    log.debug("사용자 검색 인덱스 반영 시도: userId={}",
        event.userId());

    userSearchRepository.save(
        new UserDocument(
            event.userId(),
            event.email(),
            event.name(),
            event.createdAt(),
            event.locked(),
            event.role()
        )
    );

    log.info("사용자 검색 인덱스 반영 완료: userId={}",
        event.userId());
  }
}