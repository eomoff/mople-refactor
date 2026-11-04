package com.codeit.mople.domain.follow.event;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.codeit.mople.domain.follow.dto.FollowRequest;
import com.codeit.mople.domain.follow.dto.FollowResponse;
import com.codeit.mople.domain.follow.repository.FollowRepository;
import com.codeit.mople.domain.follow.service.FollowService;
import com.codeit.mople.domain.user.entity.User;
import com.codeit.mople.domain.user.repository.UserRepository;
import com.codeit.mople.global.config.KafkaProperties;
import com.codeit.mople.global.outbox.Outbox;
import com.codeit.mople.global.outbox.OutboxStatus;
import com.codeit.mople.global.outbox.repository.OutboxRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.transaction.support.TransactionTemplate;

@SpringBootTest
@ActiveProfiles("test")
@DisplayName("팔로우 생성 이벤트 발행 시점 테스트")
class FollowCreatedEventPublishTest {

  @Autowired
  FollowService followService;

  @Autowired
  FollowRepository followRepository;

  @Autowired
  UserRepository userRepository;

  @Autowired
  OutboxRepository outboxRepository;

  @Autowired
  TransactionTemplate transactionTemplate;

  @Autowired
  KafkaProperties kafkaProperties;

  @Autowired
  ObjectMapper objectMapper;

  UUID followeeId;
  UUID followerId;

  @BeforeEach
  void setUp() {
    String suffix = UUID.randomUUID().toString();
    followeeId = userRepository.save(
        User.createUser("followee-" + suffix + "@mople.com", "password", "팔로우대상")).getId();
    followerId = userRepository.save(
        User.createUser("follower-" + suffix + "@mople.com", "password", "팔로워")).getId();
  }

  @AfterEach
  void tearDown() {
    outboxRepository.deleteAll(outboxesOfFollowee());
    followRepository.findByFolloweeIdAndFollowerId(followeeId, followerId)
        .ifPresent(followRepository::delete);
    userRepository.deleteById(followeeId);
    userRepository.deleteById(followerId);
  }

  private List<Outbox> outboxesOfFollowee() {
    return outboxRepository.findAll().stream()
        .filter(outbox -> outbox.getAggregateId().equals(followeeId.toString()))
        .toList();
  }

  @Test
  @DisplayName("트랜잭션이 커밋되면 설정된 토픽의 미발행 outbox 행이 함께 남는지")
  void savesPendingOutboxAfterCommit() throws Exception {
    // when
    FollowResponse response = followService.follow(new FollowRequest(followeeId), followerId);

    // then
    List<Outbox> outboxes = outboxesOfFollowee();
    assertThat(outboxes).hasSize(1);

    Outbox outbox = outboxes.get(0);
    assertThat(outbox.getAggregateType()).isEqualTo(kafkaProperties.topics().followCreated());
    assertThat(outbox.getEventType()).isEqualTo(FollowCreatedEvent.class.getName());
    assertThat(outbox.getStatus()).isEqualTo(OutboxStatus.PENDING);

    FollowCreatedEvent event =
        objectMapper.readValue(outbox.getPayload(), FollowCreatedEvent.class);
    assertThat(event.eventId()).isEqualTo(outbox.getEventId());
    assertThat(event.followId()).isEqualTo(response.id());
    assertThat(event.followerId()).isEqualTo(followerId);
  }

  @Test
  @DisplayName("바깥 트랜잭션이 롤백되면 outbox 행도 남지 않는지")
  void doesNotSaveOutboxAfterRollback() {
    // when
    assertThatThrownBy(() -> transactionTemplate.executeWithoutResult(status -> {
      followService.follow(new FollowRequest(followeeId), followerId);
      throw new IllegalStateException("강제 롤백");
    })).isInstanceOf(IllegalStateException.class);

    // then
    assertThat(outboxesOfFollowee()).isEmpty();
    assertThat(followRepository.findByFolloweeIdAndFollowerId(followeeId, followerId)).isEmpty();
  }
}
