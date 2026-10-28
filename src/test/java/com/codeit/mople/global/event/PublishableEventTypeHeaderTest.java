package com.codeit.mople.global.event;

import static org.assertj.core.api.Assertions.assertThat;

import com.codeit.mople.global.config.KafkaProperties;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.config.YamlPropertiesFactoryBean;
import org.springframework.context.annotation.ClassPathScanningCandidateComponentProvider;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.type.filter.AnnotationTypeFilter;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.mapping.AbstractJavaTypeMapper;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.mock.env.MockEnvironment;
import org.springframework.stereotype.Component;
import org.springframework.transaction.event.TransactionalEventListener;
import org.springframework.util.ClassUtils;

@DisplayName("타입 헤더 테스트")
class PublishableEventTypeHeaderTest {

  static final String BASE_PACKAGE = "com.codeit.mople";
  static final String TRUSTED_PACKAGES_KEY =
      "spring.kafka.consumer.properties." + JsonDeserializer.TRUSTED_PACKAGES;
  static final byte[] EMPTY_JSON = "{}".getBytes(StandardCharsets.UTF_8);

  static String trustedPackages;

  @BeforeAll
  static void loadTrustedPackages() {
    YamlPropertiesFactoryBean yaml = new YamlPropertiesFactoryBean();
    yaml.setResources(new ClassPathResource("application.yaml"));

    Properties properties = yaml.getObject();
    trustedPackages = properties == null ? null : properties.getProperty(TRUSTED_PACKAGES_KEY);

    assertThat(trustedPackages).as(TRUSTED_PACKAGES_KEY).isNotBlank();
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

  @Test
  @DisplayName("발행하는 이벤트를 컨슈머가 헤더의 클래스 이름으로 역직렬화할 수 있는지")
  void deserializesEveryPublishableEventByClassName() {
    // given
    JsonDeserializer<Object> deserializer = new JsonDeserializer<>();
    deserializer.configure(Map.of(JsonDeserializer.TRUSTED_PACKAGES, trustedPackages), false);

    // when
    Set<String> publishable = publishableEventNames();

    // then
    assertThat(publishable).isNotEmpty().allSatisfy(className -> {
      Headers headers = new RecordHeaders();
      headers.add(AbstractJavaTypeMapper.DEFAULT_CLASSID_FIELD_NAME,
          className.getBytes(StandardCharsets.UTF_8));

      assertThat(deserializer.deserialize("topic", headers, EMPTY_JSON))
          .isInstanceOf(Class.forName(className));
    });
  }
}
