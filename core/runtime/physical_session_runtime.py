from __future__ import annotations

import asyncio
from datetime import timedelta

class PhysicalSessionRuntime:
    """Executa manutencao de sessoes fisicas em background."""

    def __init__(self, engine) -> None:
        self.engine = engine
        self._task: asyncio.Task[None] | None = None
        self._stop_event = asyncio.Event()
        self._loop_interval_seconds = (
            self.engine.services.config.physical_session_runtime_interval_seconds
        )

    async def start(self) -> None:
        if self._task is not None and not self._task.done():
            return

        self._stop_event = asyncio.Event()
        self._task = asyncio.create_task(self._run_loop(), name="physical-session-runtime")

    async def stop(self) -> None:
        if self._task is None:
            return

        self._stop_event.set()
        try:
            await self._task
        finally:
            self._task = None

    async def _run_loop(self) -> None:
        while not self._stop_event.is_set():
            await self._run_once()
            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=self._loop_interval_seconds)
            except TimeoutError:
                continue

    async def _run_once(self) -> None:
        session_manager = self.engine.services.session_manager
        for session in session_manager.list_active_physical_sessions():
            if self._should_close_session(session):
                await self._close_expired_session(session)
                continue

            if self._should_send_keepalive(session):
                await self._send_keepalive(session)

    @staticmethod
    def _should_close_session(session) -> bool:
        return session.keepalive_deadline is not None and session.keepalive_deadline <= self_now()

    @staticmethod
    def _should_send_keepalive(session) -> bool:
        if not session.transport or not session.remote_host or session.remote_port is None:
            return False

        interval = timedelta(seconds=session.keepalive_interval_seconds)
        send_at = session.last_activity_at + (interval / 2)
        if send_at > self_now():
            return False

        if session.last_keepalive_sent_at is None:
            return True

        return session.last_keepalive_sent_at < session.last_activity_at

    async def _send_keepalive(self, session) -> None:
        await self.engine.services.protocol_clients.physical.session.send_keepalive(
            session_id=session.session_id,
        )

    async def _close_expired_session(self, session) -> None:
        if session.transport and session.remote_host and session.remote_port is not None:
            try:
                await self.engine.services.protocol_clients.physical.session.close_session(
                    session_id=session.session_id,
                    close_reason="keepalive_timeout",
                )
            except Exception:
                pass

        self.engine.services.session_manager.close_session(
            session.session_id,
            close_reason="keepalive_timeout",
        )


def self_now():
    from sessions.models import utc_now

    return utc_now()
