package com.codeit.mople.domain.user.admin.event;

import com.codeit.mople.domain.user.admin.enums.ForceLogoutReason;
import java.time.Instant;
import java.util.UUID;

// sessionInvalidated: 이 이벤트 발행 시점에 실제로 sessionVersion이 올라가 기존 세션이
// 무효화됐는지 여부. reason만으로 이를 추론하지 않도록(예: ACCOUNT_UNLOCKED는 세션을 끊지 않음)
// 구독자가 명시적으로 확인할 수 있게 필드로 노출한다.
//
// 이름을 UserForceLogoutEvent가 아니라 UserAccountStatusChangedEvent로 둔 이유: 강제
// 로그아웃(세션 무효화)이 항상 일어나는 건 아니라서(ACCOUNT_UNLOCKED), "강제 로그아웃"이라는
// 이름 자체가 구독자에게 잘못된 신호를 줄 수 있다. "무엇이 일어났는가"는 reason/sessionInvalidated
// 필드로 판단하고, 이벤트 이름은 "계정 상태가 바뀌었다"는 사실만 나타낸다.
public record UserAccountStatusChangedEvent(
    UUID eventId,
    Instant occurredAt,
    UUID userId,
    ForceLogoutReason reason,
    boolean sessionInvalidated
) {

}
