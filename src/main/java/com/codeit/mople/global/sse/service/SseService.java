package com.codeit.mople.global.sse.service;

import com.codeit.mople.global.sse.model.SseEvent;
import com.codeit.mople.global.sse.repository.SseEmitterRepository;
import com.codeit.mople.global.sse.repository.SseEventRepository;
import java.io.IOException;
import java.util.List;
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

@Slf4j
@Service
@RequiredArgsConstructor
public class SseService {

  private static final long TIMEOUT = 60 * 60 * 1000L; // 1시간

  private final SseEmitterRepository emitterRepository;
  private final SseEventRepository eventRepository;

  public SseEmitter connect(UUID receiverId, UUID lastEventId) {
    SseEmitter emitter = new SseEmitter(TIMEOUT);
    log.info("SSE 연결 등록: receiverId={}, emitter={}",
        receiverId, emitter);

    // 입력, 반환 둘 다 없음(Runnable)
    emitter.onCompletion(() -> {
      log.debug("SSE 연결 종료 - receiverId={}, emitter={}", receiverId, emitter);

      emitterRepository.remove(receiverId, emitter);
    });
    emitter.onTimeout(() -> {
      log.debug("SSE 연결 시간 초과 - receiverId={}, emitter={}", receiverId, emitter);

      emitterRepository.remove(receiverId, emitter);
    });

    // Consumer(void)
    emitter.onError(throwable -> {
      log.warn("SSE 연결 오류 발생 - receiverId={}, emitter={}", receiverId, emitter, throwable);

      emitterRepository.remove(receiverId, emitter);
    });

    emitterRepository.save(receiverId, emitter);

    resendEvents(receiverId, lastEventId, emitter);

    return emitter;
  }

  public boolean isConnected(UUID receiverId) {
    return emitterRepository.find(receiverId) != null;
  }

  public void saveForResend(UUID eventId, UUID receiverId, String eventName, Object data) {
    eventRepository.save(new SseEvent(eventId, receiverId, eventName, data));
  }

  public void send(UUID eventId, UUID receiverId, String eventName, Object data) {
    SseEmitter emitter = emitterRepository.find(receiverId);

    if (emitter == null) {
      log.debug("emitter가 존재하지 않습니다: receiverId={}",
          receiverId);

      return;
    }

    log.info(
        "SSE 전송 시도: receiverId={}, emitter={}",
        receiverId,
        System.identityHashCode(emitter)
    );

    try {
      emitter.send(
          SseEmitter.event()
              .id(eventId.toString())
              .name(eventName)
              .data(data)
      );

      log.info(
          "SSE 전송 성공: receiverId={}, emitter={}",
          receiverId,
          System.identityHashCode(emitter)
      );

    } catch (IOException e) {
      log.warn("SSE 전송 실패 receiverId={}",
          receiverId, e);

      // connect()의 onError 콜백 메서드 실행
      emitter.completeWithError(e);
    }
  }

  private void resendEvents(UUID receiverId, UUID lastEventId, SseEmitter emitter) {
    // lastEventId가 존재하지 않을 경우 스킵
    if (lastEventId == null) {
      return;
    }

    // 유실 이벤트들을 리스트로 가져옴(순서 보장)
    List<SseEvent> events = eventRepository.findAfter(receiverId, lastEventId);

    // 리스트의 각 SseEvent를 전송시킴
    for (SseEvent event : events) {
      try {
        emitter.send(
            SseEmitter.event()
                .id(event.id().toString())
                .name(event.eventName())
                .data(event.data())
        );
      } catch (IOException e) {
        log.warn("SSE 유실 이벤트 재전송 실패 - receiverId={}",
            receiverId, e);

        // connect()의 onError 콜백 메서드 실행
        // 해당 SSE 재전송 실패 이후의 SSE들도 막도록 설정(현재 emitter가 정상적이지 않은 상태로 간주)
        emitter.completeWithError(e);
        return;
      }
    }
  }

}
