package com.codeit.mople.global.event;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import com.codeit.mople.domain.content.entity.ContentType;
import com.codeit.mople.domain.content.event.ContentSearchIndexDeleteEvent;
import com.codeit.mople.domain.content.event.ContentSearchIndexEvent;
import com.codeit.mople.domain.directmessage.event.DirectMessageCreatedEvent;
import com.codeit.mople.domain.directmessage.event.DirectMessageReceivedEvent;
import com.codeit.mople.domain.follow.event.FollowCreatedEvent;
import com.codeit.mople.domain.notification.event.NotificationCreatedEvent;
import com.codeit.mople.domain.playlist.event.PlaylistContentAddedEvent;
import com.codeit.mople.domain.playlist.event.PlaylistCreatedEvent;
import com.codeit.mople.domain.playlist.event.PlaylistSearchIndexDeleteEvent;
import com.codeit.mople.domain.playlist.event.PlaylistSearchIndexEvent;
import com.codeit.mople.domain.playlist.event.PlaylistSubscribedEvent;
import com.codeit.mople.domain.review.event.ReviewWrittenEvent;
import com.codeit.mople.domain.user.admin.enums.ForceLogoutReason;
import com.codeit.mople.domain.user.admin.event.UserAccountStatusChangedEvent;
import com.codeit.mople.domain.user.event.UserSearchIndexEvent;
import com.codeit.mople.global.config.KafkaProperties;
import com.codeit.mople.global.outbox.OutboxRelay;
import com.codeit.mople.global.outbox.OutboxService;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.util.ReflectionUtils;

@SpringBootTest(properties = {
    "spring.kafka.enabled=true",
    "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
    "spring.kafka.listener.auto-startup=false",
    "spring.datasource.url=jdbc:h2:mem:kafka-publish-smoke;MODE=PostgreSQL;DB_CLOSE_DELAY=-1"
})
@ActiveProfiles("test")
@EmbeddedKafka(partitions = 1, topics = {
    KafkaPublishSmokeTest.FOLLOW_TOPIC,
    KafkaPublishSmokeTest.PLAYLIST_CONTENT_TOPIC,
    KafkaPublishSmokeTest.PLAYLIST_SEARCH_INDEX_TOPIC,
    KafkaPublishSmokeTest.CONTENT_SEARCH_INDEX_TOPIC,
    KafkaPublishSmokeTest.USER_SEARCH_INDEX_TOPIC,
    KafkaPublishSmokeTest.DIRECT_MESSAGE_TOPIC,
    KafkaPublishSmokeTest.NOTIFICATION_TOPIC,
    KafkaPublishSmokeTest.USER_ACCOUNT_STATUS_CHANGED_TOPIC,
    KafkaPublishSmokeTest.PLAYLIST_SUBSCRIBED_TOPIC,
    KafkaPublishSmokeTest.PLAYLIST_CREATED_TOPIC,
    KafkaPublishSmokeTest.DIRECT_MESSAGE_RECEIVED_TOPIC,
    KafkaPublishSmokeTest.REVIEW_WRITTEN_TOPIC
})
@DisplayName("Kafka 발행 스모크 테스트")
class KafkaPublishSmokeTest {

  static final String FOLLOW_TOPIC = "mople.follow.created.v1";
  static final String PLAYLIST_CONTENT_TOPIC = "mople.playlist.content-added.v1";
  static final String PLAYLIST_SEARCH_INDEX_TOPIC = "mople.playlist-search-index-events.v1";
  static final String CONTENT_SEARCH_INDEX_TOPIC = "mople.content-search-index-events.v1";
  static final String USER_SEARCH_INDEX_TOPIC = "mople.user-search-index-events.v1";
  static final String DIRECT_MESSAGE_TOPIC = "mople.direct-message.created.v1";
  static final String NOTIFICATION_TOPIC = "mople.notification.created.v1";
  static final String USER_ACCOUNT_STATUS_CHANGED_TOPIC = "mople.user.account-status-changed.v1";
  static final String PLAYLIST_SUBSCRIBED_TOPIC = "mople.playlist.subscribed.v1";
  static final String PLAYLIST_CREATED_TOPIC = "mople.playlist.created.v1";
  static final String DIRECT_MESSAGE_RECEIVED_TOPIC = "mople.direct-message.received.v1";
  static final String REVIEW_WRITTEN_TOPIC = "mople.review.written.v1";

  static final String TYPE_ID_HEADER = "__TypeId__";

  @Autowired
  ApplicationEventPublisher eventPublisher;

  @Autowired
  TransactionTemplate transactionTemplate;

  @Autowired
  EmbeddedKafkaBroker embeddedKafka;

  @Autowired
  ObjectMapper objectMapper;

  @Autowired
  KafkaProperties kafkaProperties;

  @Autowired
  OutboxService outboxService;

  private void saveToOutbox(UUID eventId, String topic, String key, Object event) {
    transactionTemplate.executeWithoutResult(status ->
        outboxService.save(eventId, topic, key, event));
  }

  private Consumer<String, String> consumerFor(String topic) {
    Map<String, Object> props =
        KafkaTestUtils.consumerProps("smoke-" + UUID.randomUUID(), "true", embeddedKafka);
    Consumer<String, String> consumer = new DefaultKafkaConsumerFactory<>(
        props, new StringDeserializer(), new StringDeserializer()).createConsumer();
    embeddedKafka.consumeFromAnEmbeddedTopic(consumer, true, topic);

    return consumer;
  }

  private String typeIdOf(ConsumerRecord<String, String> record) {
    return headerOf(record, TYPE_ID_HEADER);
  }

  private String headerOf(ConsumerRecord<String, String> record, String name) {
    Header header = record.headers().lastHeader(name);

    assertThat(header)
        .as("%s 헤더가 안 실림", name)
        .isNotNull();

    return new String(header.value(), StandardCharsets.UTF_8);
  }

  @Nested
  @DisplayName("커밋 이후 발행")
  class PublishAfterCommit {

    @Test
    @DisplayName("outbox에 저장한 팔로우 생성 이벤트가 커밋된 뒤 relay로 토픽에 1건 나가는지")
    void publishesFollowCreated() {
      // given
      UUID followId = UUID.randomUUID();
      UUID followeeId = UUID.randomUUID();
      UUID followerId = UUID.randomUUID();
      FollowCreatedEvent event = new FollowCreatedEvent(
          UUID.randomUUID(), Instant.now(), followId, followeeId, followerId, "아메리카노좋아");

      try (Consumer<String, String> consumer = consumerFor(FOLLOW_TOPIC)) {
        // when
        saveToOutbox(event.eventId(), FOLLOW_TOPIC, followeeId.toString(), event);

        // then
        ConsumerRecord<String, String> record =
            KafkaTestUtils.getSingleRecord(consumer, FOLLOW_TOPIC, Duration.ofSeconds(10));

        assertThat(record.key()).isEqualTo(followeeId.toString());
        assertThat(record.value()).contains(
            followId.toString(), followeeId.toString(), followerId.toString(), "아메리카노좋아");
      }
    }

    @Test
    @DisplayName("outbox에 저장한 알림 생성 이벤트가 커밋된 뒤 relay로 토픽에 1건 나가는지")
    void publishesNotificationCreated() throws Exception {
      // given
      NotificationCreatedEvent event = new NotificationCreatedEvent(
          UUID.randomUUID(), Instant.now(), UUID.randomUUID(), UUID.randomUUID());

      try (Consumer<String, String> consumer = consumerFor(NOTIFICATION_TOPIC)) {
        // when
        saveToOutbox(event.eventId(), NOTIFICATION_TOPIC, event.receiverId().toString(), event);

        // then
        ConsumerRecord<String, String> record =
            KafkaTestUtils.getSingleRecord(consumer, NOTIFICATION_TOPIC, Duration.ofSeconds(10));

        assertThat(record.key()).isEqualTo(event.receiverId().toString());
        assertThat(record.value()).isEqualTo(objectMapper.writeValueAsString(event));
        assertThat(typeIdOf(record)).isEqualTo(NotificationCreatedEvent.class.getName());
      }
    }

    @Test
    @DisplayName("outbox에 저장한 콘텐츠 추가 이벤트가 커밋된 뒤 relay로 토픽에 1건 나가는지")
    void publishesPlaylistContentAdded() {
      // given
      UUID playlistContentId = UUID.randomUUID();
      UUID playlistId = UUID.randomUUID();
      UUID contentId = UUID.randomUUID();
      PlaylistContentAddedEvent event = new PlaylistContentAddedEvent(
          UUID.randomUUID(), Instant.now(), playlistContentId, playlistId, contentId,
          "테스트 플레이리스트");

      try (Consumer<String, String> consumer = consumerFor(PLAYLIST_CONTENT_TOPIC)) {
        // when
        saveToOutbox(event.eventId(), PLAYLIST_CONTENT_TOPIC, playlistId.toString(), event);

        // then
        ConsumerRecord<String, String> record =
            KafkaTestUtils.getSingleRecord(consumer, PLAYLIST_CONTENT_TOPIC,
                Duration.ofSeconds(10));

        assertThat(record.key()).isEqualTo(playlistId.toString());
        assertThat(record.value()).contains(
            playlistContentId.toString(), playlistId.toString(), contentId.toString(),
            "테스트 플레이리스트");
      }
    }
  }

  @Nested
  @DisplayName("타입 헤더")
  class TypeHeader {

    @Test
    @DisplayName("relay가 보낸 팔로우 생성 이벤트에 원래 JSON과 클래스 이름, eventId 헤더가 실리는지")
    void carriesClassNameAndEventIdForFollowCreated() throws Exception {
      // given
      FollowCreatedEvent event = new FollowCreatedEvent(
          UUID.randomUUID(), Instant.now(), UUID.randomUUID(), UUID.randomUUID(),
          UUID.randomUUID(), "아메리카노좋아");

      try (Consumer<String, String> consumer = consumerFor(FOLLOW_TOPIC)) {
        // when
        saveToOutbox(event.eventId(), FOLLOW_TOPIC, event.followeeId().toString(), event);

        // then
        ConsumerRecord<String, String> record =
            KafkaTestUtils.getSingleRecord(consumer, FOLLOW_TOPIC, Duration.ofSeconds(10));

        assertThat(record.value()).isEqualTo(objectMapper.writeValueAsString(event));
        assertThat(typeIdOf(record)).isEqualTo(FollowCreatedEvent.class.getName());
        assertThat(headerOf(record, OutboxRelay.EVENT_ID_HEADER))
            .isEqualTo(event.eventId().toString());
      }
    }

    @Test
    @DisplayName("relay가 보낸 콘텐츠 추가 이벤트 헤더에 클래스 이름이 실리는지")
    void carriesClassNameForPlaylistContentAdded() {
      // given
      UUID playlistId = UUID.randomUUID();
      PlaylistContentAddedEvent event = new PlaylistContentAddedEvent(
          UUID.randomUUID(), Instant.now(), UUID.randomUUID(), playlistId,
          UUID.randomUUID(), "테스트 플레이리스트");

      try (Consumer<String, String> consumer = consumerFor(PLAYLIST_CONTENT_TOPIC)) {
        // when
        saveToOutbox(event.eventId(), PLAYLIST_CONTENT_TOPIC, playlistId.toString(), event);

        // then
        ConsumerRecord<String, String> record =
            KafkaTestUtils.getSingleRecord(consumer, PLAYLIST_CONTENT_TOPIC,
                Duration.ofSeconds(10));

        assertThat(typeIdOf(record)).isEqualTo(PlaylistContentAddedEvent.class.getName());
      }
    }

  }

  @Nested
  @DisplayName("전체 발행 이벤트")
  class EveryPublishedEvent {

    static Stream<Arguments> events() {
      UUID id = UUID.randomUUID();

      return Stream.of(
          arguments(new ContentSearchIndexEvent(UUID.randomUUID(), Instant.now(), id, "콘텐츠",
                  ContentType.MOVIE, 4.5, 10L, Instant.now()),
              CONTENT_SEARCH_INDEX_TOPIC, ContentSearchIndexEvent.class.getName()),
          arguments(new ContentSearchIndexDeleteEvent(UUID.randomUUID(), Instant.now(), id),
              CONTENT_SEARCH_INDEX_TOPIC, ContentSearchIndexDeleteEvent.class.getName()),
          arguments(
              new UserSearchIndexEvent(UUID.randomUUID(), Instant.now(), id, "a@mople.com", "이름",
                  Instant.now(), false, "USER"),
              USER_SEARCH_INDEX_TOPIC, UserSearchIndexEvent.class.getName()),
          arguments(new DirectMessageCreatedEvent(UUID.randomUUID(), Instant.now(), id, id),
              DIRECT_MESSAGE_TOPIC, DirectMessageCreatedEvent.class.getName()),
          arguments(new UserAccountStatusChangedEvent(UUID.randomUUID(), Instant.now(), id,
                  ForceLogoutReason.ROLE_CHANGE, true),
              USER_ACCOUNT_STATUS_CHANGED_TOPIC, UserAccountStatusChangedEvent.class.getName()),
          arguments(new DirectMessageReceivedEvent(UUID.randomUUID(), Instant.now(), id, id,
                  "발신자", "안녕하세요"),
              DIRECT_MESSAGE_RECEIVED_TOPIC, DirectMessageReceivedEvent.class.getName())
      );
    }

    static Stream<Arguments> outboxEvents() {
      UUID id = UUID.randomUUID();

      return Stream.of(
          arguments(new ReviewWrittenEvent(UUID.randomUUID(), Instant.now(), id, id, "작성자"),
              REVIEW_WRITTEN_TOPIC, ReviewWrittenEvent.class.getName()),
          arguments(new PlaylistSearchIndexEvent(UUID.randomUUID(), Instant.now(), id, "플리",
                  Instant.now(), 3L),
              PLAYLIST_SEARCH_INDEX_TOPIC, PlaylistSearchIndexEvent.class.getName()),
          arguments(new PlaylistSearchIndexDeleteEvent(UUID.randomUUID(), Instant.now(), id),
              PLAYLIST_SEARCH_INDEX_TOPIC, PlaylistSearchIndexDeleteEvent.class.getName()),
          arguments(new PlaylistSubscribedEvent(UUID.randomUUID(), Instant.now(), id, id, id,
                  "구독자", "플리"),
              PLAYLIST_SUBSCRIBED_TOPIC, PlaylistSubscribedEvent.class.getName()),
          arguments(new PlaylistCreatedEvent(UUID.randomUUID(), Instant.now(), id, id, "주인",
                  "플리"),
              PLAYLIST_CREATED_TOPIC, PlaylistCreatedEvent.class.getName())
      );
    }

    @ParameterizedTest(name = "{2}")
    @MethodSource("events")
    @DisplayName("커밋 뒤 제 토픽으로 나가고 헤더에 제 클래스 이름이 실리는지")
    void publishesWithClassName(Object event, String topic, String className) throws Exception {
      try (Consumer<String, String> consumer = consumerFor(topic)) {
        // when
        transactionTemplate.executeWithoutResult(status -> eventPublisher.publishEvent(event));

        // then
        ConsumerRecord<String, String> record =
            KafkaTestUtils.getSingleRecord(consumer, topic, Duration.ofSeconds(10));

        assertThat(typeIdOf(record)).isEqualTo(className);
        assertThat(record.value()).isEqualTo(objectMapper.writeValueAsString(event));
      }
    }

    @ParameterizedTest(name = "{2}")
    @MethodSource("outboxEvents")
    @DisplayName("outbox에 저장하면 relay가 제 토픽으로 보내고 원래 JSON과 클래스 이름, eventId 헤더가 실리는지")
    void relaysWithClassName(Object event, String topic, String className) throws Exception {
      // given
      UUID eventId = (UUID) event.getClass().getMethod("eventId").invoke(event);

      try (Consumer<String, String> consumer = consumerFor(topic)) {
        // when
        saveToOutbox(eventId, topic, eventId.toString(), event);

        // then
        ConsumerRecord<String, String> record =
            KafkaTestUtils.getSingleRecord(consumer, topic, Duration.ofSeconds(10));

        assertThat(typeIdOf(record)).isEqualTo(className);
        assertThat(record.value()).isEqualTo(objectMapper.writeValueAsString(event));
        assertThat(headerOf(record, OutboxRelay.EVENT_ID_HEADER)).isEqualTo(eventId.toString());
      }
    }
  }

  @Nested
  @DisplayName("롤백 이후 미발행")
  class NoPublishOnRollback {

    @Test
    @DisplayName("팔로우 생성 이벤트의 outbox 저장이 롤백되면 토픽에 아무것도 안 나가는지")
    void doesNotPublishFollowCreated() {
      // given
      FollowCreatedEvent event = new FollowCreatedEvent(
          UUID.randomUUID(), Instant.now(), UUID.randomUUID(), UUID.randomUUID(),
          UUID.randomUUID(), "아메리카노좋아");

      try (Consumer<String, String> consumer = consumerFor(FOLLOW_TOPIC)) {
        // when
        transactionTemplate.executeWithoutResult(status -> {
          outboxService.save(
              event.eventId(), FOLLOW_TOPIC, event.followeeId().toString(), event);
          status.setRollbackOnly();
        });

        // then
        ConsumerRecords<String, String> records =
            KafkaTestUtils.getRecords(consumer, Duration.ofSeconds(2));

        assertThat(records.count()).isZero();
      }
    }

    @Test
    @DisplayName("콘텐츠 추가 이벤트의 outbox 저장이 롤백되면 토픽에 아무것도 안 나가는지")
    void doesNotPublishPlaylistContentAdded() {
      // given
      UUID playlistId = UUID.randomUUID();
      PlaylistContentAddedEvent event = new PlaylistContentAddedEvent(
          UUID.randomUUID(), Instant.now(), UUID.randomUUID(), playlistId, UUID.randomUUID(),
          "테스트 플레이리스트");

      try (Consumer<String, String> consumer = consumerFor(PLAYLIST_CONTENT_TOPIC)) {
        // when
        transactionTemplate.executeWithoutResult(status -> {
          outboxService.save(event.eventId(), PLAYLIST_CONTENT_TOPIC, playlistId.toString(), event);
          status.setRollbackOnly();
        });

        // then
        ConsumerRecords<String, String> records =
            KafkaTestUtils.getRecords(consumer, Duration.ofSeconds(2));

        assertThat(records.count()).isZero();
      }
    }
  }

  @Nested
  @DisplayName("토픽 파티션")
  class TopicPartitions {

    @Test
    @DisplayName("파티션 1개로 먼저 만들어진 토픽까지 설정된 토픽이 모두 파티션 3개인지")
    void configuredTopicsHaveThreePartitions() throws Exception {
      // given
      List<String> topics = Arrays.stream(KafkaProperties.Topics.class.getRecordComponents())
          .map(component -> (String) ReflectionUtils.invokeMethod(
              component.getAccessor(), kafkaProperties.topics()))
          .toList();

      try (AdminClient admin = AdminClient.create(Map.of(
          AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, embeddedKafka.getBrokersAsString()))) {
        // when
        Map<String, TopicDescription> descriptions =
            admin.describeTopics(topics).allTopicNames().get(10, TimeUnit.SECONDS);

        // then
        assertThat(descriptions).containsOnlyKeys(topics);
        assertThat(descriptions.values())
            .allSatisfy(description -> assertThat(description.partitions()).hasSize(3));
      }
    }
  }
}