package com.codeit.mople.global.sse.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.codeit.mople.global.sse.model.SseEvent;
import com.codeit.mople.global.sse.repository.SseEmitterRepository;
import com.codeit.mople.global.sse.repository.SseEventRepository;
import java.io.IOException;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

@ExtendWith(MockitoExtension.class)
public class SseServiceTest {

  @Mock
  private SseEmitterRepository emitterRepository;

  @Mock
  private SseEventRepository eventRepository;

  @InjectMocks
  private SseService sseService;

  private UUID receiverId;
  private SseEmitter sseEmitter;

  private UUID lastEventId;
  private SseEvent sseEvent1;
  private SseEvent sseEvent2;

  @BeforeEach
  void setUp() {

    receiverId = UUID.randomUUID();
    sseEmitter = mock(SseEmitter.class);

    lastEventId = UUID.randomUUID();
    sseEvent1 = new SseEvent(
        UUID.randomUUID(),
        receiverId,
        "notifications",
        "data"
    );
    sseEvent2 = new SseEvent(
        UUID.randomUUID(),
        receiverId,
        "direct-messages",
        "data2"
    );

  }

  @Nested
  @DisplayName("SSE 연결")
  class Connect {

    @Test
    @DisplayName("SSE 연결 성공")
    void connect_success() {
      // given

      // BeforeEach에서 receiverId를 초기화

      // when
      SseEmitter result = sseService.connect(receiverId, null);

      // then
      assertThat(result).isNotNull();

      // 레포지토리 호출 검증
      verify(emitterRepository).save(receiverId, result);

      // resendEvents() 메서드 검증(내부에 sseEventRepository.findAfter 메서드 검증)
      verify(eventRepository, never()).findAfter(any(), any());
    }

    @Test
    @DisplayName("SSE 연결 성공 - 유실 이벤트 재전송")
    void connect_success_resendEvents() throws IOException {
      // given

      // BeforeEach에서 receiverId, lastEventId, sseEvent1, sseEvent2를 초기화

      when(eventRepository.findAfter(receiverId, lastEventId))
          .thenReturn(List.of(sseEvent1, sseEvent2));

      // when
      SseEmitter result = sseService.connect(receiverId, lastEventId);

      // then
      assertThat(result).isNotNull();

      verify(emitterRepository).save(receiverId, result);
      verify(eventRepository).findAfter(receiverId, lastEventId);
    }

  }

  @Nested
  @DisplayName("SSE 이벤트 전송")
  class Send {

    @Test
    @DisplayName("SSE 이벤트 전송 성공 - 재전송용으로는 저장하지 않음")
    void send_success() throws IOException {
      // given

      // BeforeEach에서 receiverId, sseEmitter를 초기화

      when(emitterRepository.find(receiverId))
          .thenReturn(sseEmitter);

      // when
      sseService.send(UUID.randomUUID(), receiverId, "eventName", "data");

      // then
      verify(sseEmitter).send(any(SseEmitter.SseEventBuilder.class));

      verifyNoInteractions(eventRepository);
    }

    @Test
    @DisplayName("SSE 이벤트 전송 무시 - 이 서버에 사용자의 연결이 없는 경우")
    void send_ignore_noEmitter() {
      // given

      // BeforeEach에서 receiverId를 초기화

      when(emitterRepository.find(receiverId))
          .thenReturn(null);

      // when
      sseService.send(UUID.randomUUID(), receiverId, "eventName", "data");

      // then
      verify(emitterRepository).find(receiverId);

      verifyNoInteractions(eventRepository);
    }

    @Test
    @DisplayName("SSE 이벤트 전송 실패 - 연결 종료")
    void send_fail_completeWithError() throws IOException {
      // given

      // BeforeEach에서 receiverId, sseEmitter를 초기화

      when(emitterRepository.find(receiverId))
          .thenReturn(sseEmitter);

      doThrow(new IOException())
          .when(sseEmitter)
          .send(any(SseEmitter.SseEventBuilder.class));

      // when
      sseService.send(UUID.randomUUID(), receiverId, "eventName", "data");

      // then
      verify(sseEmitter).completeWithError(any(IOException.class));
    }

  }

  @Nested
  @DisplayName("SSE 연결 여부")
  class IsConnected {

    @Test
    @DisplayName("이 서버에 emitter가 있으면 연결된 것으로 본다")
    void isConnected_true() {
      // given
      when(emitterRepository.find(receiverId))
          .thenReturn(sseEmitter);

      // when & then
      assertThat(sseService.isConnected(receiverId)).isTrue();
    }

    @Test
    @DisplayName("이 서버에 emitter가 없으면 연결되지 않은 것으로 본다")
    void isConnected_false() {
      // given
      when(emitterRepository.find(receiverId))
          .thenReturn(null);

      // when & then
      assertThat(sseService.isConnected(receiverId)).isFalse();
    }

  }

  @Nested
  @DisplayName("재전송용 저장")
  class SaveForResend {

    @Test
    @DisplayName("받은 이벤트 ID 그대로 재전송용으로 저장")
    void saveForResend_success() {
      // given
      UUID eventId = UUID.randomUUID();

      // when
      sseService.saveForResend(eventId, receiverId, "notifications", "data");

      // then
      verify(eventRepository).save(new SseEvent(eventId, receiverId, "notifications", "data"));
    }

  }

}
