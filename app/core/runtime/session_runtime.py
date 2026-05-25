from __future__ import annotations

from datetime import timedelta

from sessions import is_observed_only_physical_endpoint
from .base import PeriodicRuntime


class SessionRuntime(PeriodicRuntime):
    """Executa manutencao periodica para sessoes fisicas e virtuais."""

    def __init__(self, engine) -> None:
        super().__init__(
            engine,
            loop_interval_seconds=engine.services.config.physical_session_runtime_interval_seconds,
            task_name="session-runtime",
        )

    async def _run_once(self) -> None:
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

    @staticmethod
    def _should_close_session(session) -> bool:
        return session.keepalive_deadline is not None and session.keepalive_deadline <= self_now()

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
