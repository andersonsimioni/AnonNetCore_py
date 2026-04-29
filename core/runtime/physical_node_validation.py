from __future__ import annotations

import asyncio

from identity import RemotePhysicalNodeValidationCandidate
from sessions.models import utc_now


class PhysicalNodeValidationRuntime:
    """Valida physical nodes descobertos tentando estabelecer uma physical session."""

    def __init__(self, engine) -> None:
        self.engine = engine
        self._task: asyncio.Task[None] | None = None
        self._stop_event = asyncio.Event()
        self._loop_interval_seconds = (
            self.engine.services.config.physical_node_validation_runtime_interval_seconds
        )
        self._validation_backoff_seconds = (
            self.engine.services.config.physical_node_validation_backoff_seconds
        )

    async def start(self) -> None:
        if self._task is not None and not self._task.done():
            return

        self._stop_event = asyncio.Event()
        self._task = asyncio.create_task(self._run_loop(), name="physical-node-validation-runtime")

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
        candidate = self._select_candidate()
        if candidate is None:
            return

        if self.engine.services.session_manager.has_open_physical_session(candidate.node_id):
            return

        await self._try_validate_candidate(candidate)

    def _select_candidate(self) -> RemotePhysicalNodeValidationCandidate | None:
        retry_before = utc_now() - self._build_backoff_delta()
        candidates = self.engine.services.identity_service.list_remote_physical_nodes_for_validation(
            limit=1,
            failed_before=retry_before,
        )
        if not candidates:
            return None

        return candidates[0]

    async def _try_validate_candidate(
        self,
        candidate: RemotePhysicalNodeValidationCandidate,
    ) -> None:
        try:
            session_id = await self.engine.services.protocol_clients.physical.session.start_session(
                remote_physical_node_id=candidate.node_id,
            )
        except Exception:
            return

        self._mark_validation_success(session_id)

    def _mark_validation_success(
        self,
        session_id: str,
    ) -> None:
        session = self.engine.services.session_manager.get_session_by_session_id(session_id)
        if session is None or not session.transport or not session.remote_host or session.remote_port is None:
            return

        self.engine.services.identity_service.mark_remote_physical_node_validated(
            node_id=session.remote_identity_id,
            transport=session.transport,
            host=session.remote_host,
            port=session.remote_port,
        )

    def _build_backoff_delta(self):
        from datetime import timedelta

        return timedelta(seconds=self._validation_backoff_seconds)
