from __future__ import annotations

from datetime import timedelta

from common import load_json_object
from sessions import is_observed_only_physical_endpoint
from .base import PeriodicRuntime


class SessionRuntime(PeriodicRuntime):
    """Executa manutencao periodica para sessoes fisicas e virtuais."""

    def __init__(self, engine) -> None:
        super().__init__(
            engine,
            loop_interval_seconds=engine.services.config.session_runtime_interval_seconds,
            task_name="session-runtime",
        )

    async def _run_once(self) -> None:
        await self._resend_due_reliable_messages()
        for session in self.engine.services.session_manager.list_active_sessions():
            if self._should_close_session(session):
                await self._close_expired_session(session)
                continue

            if self._should_send_keepalive(session):
                try:
                    await self._send_keepalive(session)
                except Exception as error:
                    self.engine.services.log_service.warning(
                        "session_runtime",
                        "failed to send session keepalive",
                        session_id=session.session_id,
                        session_scope=session.session_scope,
                        remote_identity_id=session.remote_identity_id,
                        transport=session.transport,
                        remote_host=session.remote_host,
                        remote_port=session.remote_port,
                        error_type=type(error).__name__,
                        error=repr(error),
                    )

    async def _resend_due_reliable_messages(self) -> None:
        due_messages = self._list_due_reliable_messages()
        if not due_messages:
            return

        self.engine.services.log_service.debug(
            "session_runtime",
            "reliable session resend scan found due messages",
            due_count=len(due_messages),
        )
        for message in due_messages:
            session = self.engine.services.session_manager.get_session_by_session_id(
                message.session_id
            )
            if session is None or session.session_state != "active":
                self.engine.services.log_service.warning(
                    "session_runtime",
                    "cannot resend reliable message because session is missing or inactive",
                    session_id=message.session_id,
                    session_scope=message.session_scope,
                    reliable_message_id=message.reliable_message_id,
                    sequence_number=message.sequence_number,
                    inner_message_type=message.inner_message_type,
                )
                continue

            try:
                if message.session_scope == "physical":
                    await self.engine.services.protocol_clients.physical.session.resend_reliable_message(
                        message
                    )
                elif message.session_scope == "virtual":
                    await self.engine.services.protocol_clients.virtual.session.resend_reliable_message(
                        message
                    )
                else:
                    self.engine.services.log_service.warning(
                        "session_runtime",
                        "cannot resend reliable message for unknown session scope",
                        session_id=message.session_id,
                        session_scope=message.session_scope,
                        reliable_message_id=message.reliable_message_id,
                    )
                    continue

                self.engine.services.log_service.info(
                    "session_runtime",
                    "resent reliable session message",
                    session_id=message.session_id,
                    session_scope=message.session_scope,
                    reliable_message_id=message.reliable_message_id,
                    sequence_number=message.sequence_number,
                    attempts=message.attempts,
                    inner_message_type=message.inner_message_type,
                    retry_after_seconds=round(message.retry_after_seconds, 3),
                )
            except Exception as error:
                self.engine.services.log_service.error(
                    "session_runtime",
                    "failed to resend reliable session message",
                    session_id=message.session_id,
                    session_scope=message.session_scope,
                    reliable_message_id=message.reliable_message_id,
                    sequence_number=message.sequence_number,
                    attempts=message.attempts,
                    inner_message_type=message.inner_message_type,
                    error_type=type(error).__name__,
                    error=repr(error),
                )

    def _list_due_reliable_messages(self):
        now = self_now()
        due_messages = []
        pending_messages = self.engine.services.session_manager.list_pending_reliable_outbound_messages()
        for message in pending_messages:
            if message.attempts >= message.max_attempts:
                self.engine.services.session_manager.mark_reliable_outbound_failed(
                    session_id=message.session_id,
                    sequence_number=message.sequence_number,
                    reason="max_attempts_exceeded",
                )
                self.engine.services.log_service.warning(
                    "session_runtime",
                    "reliable session message failed after max attempts",
                    session_id=message.session_id,
                    session_scope=message.session_scope,
                    reliable_message_id=message.reliable_message_id,
                    sequence_number=message.sequence_number,
                    attempts=message.attempts,
                    max_attempts=message.max_attempts,
                    inner_message_type=message.inner_message_type,
                )
                continue

            session = self.engine.services.session_manager.get_session_by_session_id(
                message.session_id
            )
            retry_after_seconds = self._resolve_reliable_retry_after_seconds(session, message)
            message.retry_after_seconds = retry_after_seconds
            if message.due_for_send(now, retry_after_seconds=retry_after_seconds):
                due_messages.append(message)

        return sorted(
            due_messages,
            key=lambda message: (message.last_sent_at or message.created_at, message.sequence_number),
        )

    def _resolve_reliable_retry_after_seconds(self, session, message) -> float:
        if session is None or message.session_scope == "physical":
            return float(self.engine.services.config.physical_reliable_retry_seconds)

        if message.session_scope != "virtual":
            return float(message.retry_after_seconds)

        route_rtt_ms = self._resolve_virtual_session_route_rtt_ms(session)
        if route_rtt_ms is None:
            fallback_seconds = float(
                self.engine.services.config.virtual_reliable_retry_fallback_seconds
            )
            self.engine.services.log_service.debug(
                "session_runtime",
                "using fallback reliable retry for virtual session without known route rtt",
                session_id=session.session_id,
                bound_route_id=session.bound_route_id,
                reliable_message_id=message.reliable_message_id,
                retry_after_seconds=fallback_seconds,
            )
            return fallback_seconds

        raw_retry_seconds = (
            route_rtt_ms
            / 1000.0
            * float(self.engine.services.config.virtual_reliable_retry_rtt_multiplier)
        )
        retry_after_seconds = max(
            float(self.engine.services.config.virtual_reliable_retry_min_seconds),
            min(
                raw_retry_seconds,
                float(self.engine.services.config.virtual_reliable_retry_max_seconds),
            ),
        )
        self.engine.services.log_service.debug(
            "session_runtime",
            "resolved dynamic reliable retry for virtual session",
            session_id=session.session_id,
            bound_route_id=session.bound_route_id,
            reliable_message_id=message.reliable_message_id,
            route_rtt_ms=round(route_rtt_ms, 3),
            raw_retry_seconds=round(raw_retry_seconds, 3),
            retry_after_seconds=round(retry_after_seconds, 3),
        )
        return retry_after_seconds

    def _resolve_virtual_session_route_rtt_ms(self, session) -> float | None:
        if session.bound_route_id:
            route_rtt_ms = self.engine.services.route_service.resolve_route_rtt_ms(
                route_id=session.bound_route_id,
            )
            if route_rtt_ms is not None and route_rtt_ms > 0:
                return float(route_rtt_ms)

        metadata_rtt = _read_positive_float_from_session_metadata(
            session,
            "entry_point_rtt",
        )
        if metadata_rtt is not None:
            return metadata_rtt

        return None

    def _should_close_session(self, session) -> bool:
        if session.keepalive_deadline is None or session.keepalive_deadline > self_now():
            return False

        if session.session_scope != "virtual":
            return True

        return self._should_close_virtual_session(session)

    def _should_close_virtual_session(self, session) -> bool:
        route_rtt_ms = self._resolve_virtual_session_route_rtt_ms(session)
        if route_rtt_ms is None:
            return True

        timeout_seconds = max(
            float(self.engine.services.config.virtual_session_timeout_min_seconds),
            session.keepalive_interval_seconds * 3.0,
            route_rtt_ms
            / 1000.0
            * float(self.engine.services.config.virtual_session_timeout_rtt_multiplier),
        )
        expires_at = session.last_activity_at + timedelta(seconds=timeout_seconds)
        if expires_at <= self_now():
            return True

        self.engine.services.log_service.debug(
            "session_runtime",
            "kept virtual session open using route-aware timeout",
            session_id=session.session_id,
            bound_route_id=session.bound_route_id,
            route_rtt_ms=round(route_rtt_ms, 3),
            timeout_seconds=round(timeout_seconds, 3),
            keepalive_interval_seconds=session.keepalive_interval_seconds,
        )
        return False

    def _should_send_keepalive(self, session) -> bool:
        if not self._can_send_session_message(session):
            return False

        interval = timedelta(seconds=session.keepalive_interval_seconds)
        send_at = session.last_activity_at + (interval / 2)
        if send_at > self_now():
            return False

        if session.last_keepalive_sent_at is None:
            return True

        return session.last_keepalive_sent_at < session.last_activity_at

    async def _send_keepalive(self, session) -> None:
        if session.session_scope == "physical":
            await self.engine.services.protocol_clients.physical.session.send_keepalive(
                session_id=session.session_id,
            )
            return

        if session.session_scope == "virtual":
            await self.engine.services.protocol_clients.virtual.session.send_keepalive(
                session_id=session.session_id,
            )

    async def _close_expired_session(self, session) -> None:
        if self._can_send_session_message(session):
            try:
                await self._send_close_message(session)
            except Exception as error:
                self.engine.services.log_service.warning(
                    "session_runtime",
                    "failed to send close for expired session",
                    session_id=session.session_id,
                    session_scope=session.session_scope,
                    remote_identity_id=session.remote_identity_id,
                    error_type=type(error).__name__,
                    error=repr(error),
                )
                pass

        self.engine.services.session_manager.close_session(
            session.session_id,
            close_reason="keepalive_timeout",
        )
        self.engine.services.log_service.warning(
            "session_runtime",
            "closed expired session",
            session_id=session.session_id,
            session_scope=session.session_scope,
            remote_identity_id=session.remote_identity_id,
        )

    async def _send_close_message(self, session) -> None:
        if session.session_scope == "physical":
            await self.engine.services.protocol_clients.physical.session.close_session(
                session_id=session.session_id,
                close_reason="keepalive_timeout",
            )
            return

        if session.session_scope == "virtual":
            await self.engine.services.protocol_clients.virtual.session.close_session(
                session_id=session.session_id,
                close_reason="keepalive_timeout",
            )

    @staticmethod
    def _can_send_session_message(session) -> bool:
        if session.session_scope == "physical":
            if is_observed_only_physical_endpoint(session):
                return False
            return bool(session.transport and session.remote_host and session.remote_port is not None)

        if session.session_scope == "virtual":
            return bool(session.bound_route_id)

        return False


def self_now():
    from sessions.models import utc_now

    return utc_now()


def _read_positive_float_from_session_metadata(session, key: str) -> float | None:
    value = load_json_object(session.metadata_json).get(key)
    if isinstance(value, (int, float)) and value > 0:
        return float(value)
    return None
