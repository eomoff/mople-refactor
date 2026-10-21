package com.codeit.mople.global.event;

import static org.assertj.core.api.Assertions.assertThat;

import com.codeit.mople.global.config.KafkaProperties;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.config.YamlPropertiesFactoryBean;
import org.springframework.context.annotation.ClassPathScanningCandidateComponentProvider;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.type.filter.AnnotationTypeFilter;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.mock.env.MockEnvironment;
import org.springframework.stereotype.Component;
import org.springframework.transaction.event.TransactionalEventListener;
import org.springframework.util.ClassUtils;

@DisplayName("타입 이름 매핑 테스트")
class PublishableEventTypeMappingTest {

  static final String BASE_PACKAGE = "com.codeit.mople";
  static final String MAPPING_KEY = "mople.kafka.type-mapping";

  static List<String> entries;

  @BeforeAll
  static void loadMapping() {
    YamlPropertiesFactoryBean yaml = new YamlPropertiesFactoryBean();
    yaml.setResources(new ClassPathResource("application.yaml"));

    Properties properties = yaml.getObject();
    String mapping = properties == null ? null : properties.getProperty(MAPPING_KEY);

    assertThat(mapping).as(MAPPING_KEY).isNotBlank();
    entries = Arrays.stream(mapping.split(","))
        .map(String::trim)
        .filter(entry -> !entry.isEmpty())
        .toList();
  }

  // 프로듀서는 Kafka가 켜졌을 때만 빈이 되므로 켜진 환경으로 스캔하고,
  // KafkaTemplate을 가진 빈이 받아서 그대로 보내는 이벤트를 발행 이벤트로 봄
  private Set<String> publishableEventNames() {
    ClassPathScanningCandidateComponentProvider scanner =
        new ClassPathScanningCandidateComponentProvider(false,
            new MockEnvironment().withProperty(KafkaProperties.PREFIX + ".enabled", "true"));
    scanner.addIncludeFilter(new AnnotationTypeFilter(Component.class));

    return scanner.findCandidateComponents(BASE_PACKAGE).stream()
        .map(BeanDefinition::getBeanClassName)
        .map(className -> ClassUtils.resolveClassName(className, null))
        .filter(this::sendsToKafka)
        .flatMap(type -> Arrays.stream(type.getDeclaredMethods()))
        .filter(method -> method.isAnnotationPresent(TransactionalEventListener.class))
        .map(method -> method.getParameterTypes()[0].getName())
        .collect(Collectors.toSet());
  }

  private boolean sendsToKafka(Class<?> type) {
    return Arrays.stream(type.getDeclaredFields())
        .anyMatch(field -> field.getType() == KafkaTemplate.class);
  }

  private String aliasOf(String entry) {
    return entry.substring(0, entry.indexOf(':'));
  }

  private String classNameOf(String entry) {
    return entry.substring(entry.indexOf(':') + 1);
  }

  @Test
  @DisplayName("발행하는 이벤트가 매핑에 하나도 빠지지 않았는지")
  void mapsEveryPublishableEvent() {
    // given
    Set<String> mapped = entries.stream().map(this::classNameOf).collect(Collectors.toSet());

    // when
    Set<String> publishable = publishableEventNames();

    // then
    assertThat(publishable).isNotEmpty();
    assertThat(mapped).containsAll(publishable);
  }

  @Test
  @DisplayName("타입 이름이 겹치지 않는지")
  void hasNoDuplicateAlias() {
    // when
    List<String> aliases = entries.stream().map(this::aliasOf).toList();

    // then
    assertThat(aliases).doesNotHaveDuplicates();
  }

  @Test
  @DisplayName("타입 이름 끝에 버전이 붙어 있는지")
  void versionsEveryAlias() {
    // when
    List<String> aliases = entries.stream().map(this::aliasOf).toList();

    // then
    assertThat(aliases)
        .as("끝에 버전이 있어야 나중에 .v2로 넘어갈 수 있음")
        .allMatch(alias -> alias.matches(".+\\.v\\d+"));
  }

  @Test
  @DisplayName("매핑에 적힌 클래스가 전부 실재하는지")
  void mapsExistingClassesOnly() {
    // when
    List<String> classNames = entries.stream().map(this::classNameOf).toList();

    // then
    assertThat(classNames).allSatisfy(className ->
        assertThat(Class.forName(className)).isNotNull());
  }
}