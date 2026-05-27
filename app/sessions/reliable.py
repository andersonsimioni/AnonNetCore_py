from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from uuid import uuid4

from common import utc_now


@dataclass(slots=True)
class ReliableOutboundMessage:
    session_id: str
    session_scope: str
    reliable_message_id: str
    sequence_number: int
    inner_message_type: str
    inner_payload: dict[str, object]
    created_at: datetime
    retry_after_seconds: float
    max_attempts: int
    attempts: int = 0
    last_sent_at: datetime | None = None
    acked_at: datetime | None = None
    failed_at: datetime | None = None
    failure_reason: str | None = None

    @property
    def pending(self) -> bool:
        return self.acked_at is None and self.failed_at is None

    def due_for_send(self, now: datetime, retry_after_seconds: float | None = None) -> bool:
        if not self.pending:
            return False
        if self.last_sent_at is None:
            return True
        retry_after = retry_after_seconds if retry_after_seconds is not None else self.retry_after_seconds
        return self.last_sent_at + timedelta(seconds=retry_after) <= now

    def to_reliable_payload(self) -> dict[str, object]:
        return {
            "reliable_message_id": self.reliable_message_id,
            "sequence_number": self.sequence_number,
            "inner_message_type": self.inner_message_type,
            "inner_payload": self.inner_payload,
            "created_at": self.created_at.isoformat(),
            "attempt": self.attempts + 1,
        }


@dataclass(slots=True)
class ReliableInboundMessage:
    reliable_message_id: str
    sequence_number: int
    inner_message_type: str
    inner_payload: dict[str, object]
    received_at: datetime = field(default_factory=utc_now)


@dataclass(slots=True)
class ReliableSessionState:
    session_id: str
    session_scope: str
    next_outbound_sequence: int = 1
    next_expected_inbound_sequence: int = 1
    outbound_by_sequence: dict[int, ReliableOutboundMessage] = field(default_factory=dict)
    inbound_buffer_by_sequence: dict[int, ReliableInboundMessage] = field(default_factory=dict)
    delivered_message_ids: set[str] = field(default_factory=set)


@dataclass(slots=True, frozen=True)
class ReliableReceiveResult:
    ack_payload: dict[str, object]
    deliveries: tuple[ReliableInboundMessage, ...]
    duplicate: bool = False
    buffered: bool = False


def build_reliable_outbound(
    state: ReliableSessionState,
    *,
    inner_message_type: str,
    inner_payload: dict[str, object],
    retry_after_seconds: float,
    max_attempts: int,
) -> ReliableOutboundMessage:
    message = ReliableOutboundMessage(
        session_id=state.session_id,
        session_scope=state.session_scope,
        reliable_message_id=str(uuid4()),
        sequence_number=state.next_outbound_sequence,
        inner_message_type=inner_message_type,
        inner_payload=inner_payload,
        created_at=utc_now(),
        retry_after_seconds=retry_after_seconds,
        max_attempts=max_attempts,
    )
    state.outbound_by_sequence[message.sequence_number] = message
    state.next_outbound_sequence += 1
    return message


def receive_reliable_payload(
    state: ReliableSessionState,
    payload: dict[str, object],
) -> ReliableReceiveResult:
    inbound = parse_reliable_payload(payload)
    ack_payload = {
        "ack_for_message_id": inbound.reliable_message_id,
        "ack_for_sequence_number": inbound.sequence_number,
        "received_at": utc_now().isoformat(),
    }

    if inbound.reliable_message_id in state.delivered_message_ids:
        return ReliableReceiveResult(ack_payload=ack_payload, deliveries=(), duplicate=True)

    if inbound.sequence_number < state.next_expected_inbound_sequence:
        return ReliableReceiveResult(ack_payload=ack_payload, deliveries=(), duplicate=True)

    if inbound.sequence_number > state.next_expected_inbound_sequence:
        state.inbound_buffer_by_sequence.setdefault(inbound.sequence_number, inbound)
        return ReliableReceiveResult(ack_payload=ack_payload, deliveries=(), buffered=True)

    state.inbound_buffer_by_sequence[inbound.sequence_number] = inbound
    deliveries: list[ReliableInboundMessage] = []
    while True:
        next_message = state.inbound_buffer_by_sequence.pop(
            state.next_expected_inbound_sequence,
            None,
        )
        if next_message is None:
            break
        if next_message.reliable_message_id not in state.delivered_message_ids:
            state.delivered_message_ids.add(next_message.reliable_message_id)
            deliveries.append(next_message)
        state.next_expected_inbound_sequence += 1

    return ReliableReceiveResult(ack_payload=ack_payload, deliveries=tuple(deliveries))


def parse_reliable_payload(payload: dict[str, object]) -> ReliableInboundMessage:
    reliable_message_id = payload.get("reliable_message_id")
    sequence_number = payload.get("sequence_number")
    inner_message_type = payload.get("inner_message_type")
    inner_payload = payload.get("inner_payload")

    if not isinstance(reliable_message_id, str) or not reliable_message_id:
        raise ValueError("reliable_message_id_required")
    if not isinstance(sequence_number, int) or sequence_number < 1:
        raise ValueError("sequence_number_invalid")
    if not isinstance(inner_message_type, str) or not inner_message_type:
        raise ValueError("inner_message_type_required")
    if not isinstance(inner_payload, dict):
        raise ValueError("inner_payload_must_be_object")

    return ReliableInboundMessage(
        reliable_message_id=reliable_message_id,
        sequence_number=sequence_number,
        inner_message_type=inner_message_type,
        inner_payload=inner_payload,
    )
