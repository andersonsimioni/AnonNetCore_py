from __future__ import annotations

import asyncio

from identity import RemotePhysicalNodePingCandidate


class PhysicalPingRuntime:
    """Executa PING periodico em physical nodes ativos e persiste estatisticas de RTT."""

    def __init__(self, engine) -> None:
        self.engine = engine
        self._task: asyncio.Task[None] | None = None
        self._stop_event = asyncio.Event()
        self._loop_interval_seconds = self.engine.services.config.physical_ping_runtime_interval_seconds
        self._candidate_limit = self.engine.services.config.physical_ping_runtime_candidate_limit

    async def start(self) -> None:
        if self._task is not None and not self._task.done():
            return

        self._stop_event = asyncio.Event()
        self._task = asyncio.create_task(self._run_loop(), name="physical-ping-runtime")

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

        try:
            ping_result = await self.engine.services.protocol_clients.physical.ping.ping_physical_node(
                remote_physical_node_id=candidate.node_id,
            )
        except Exception:
            return

        observed_rtt_ms = ping_result.get("observed_rtt_ms")
        if not isinstance(observed_rtt_ms, (int, float)):
            return

        self.engine.services.identity_service.upsert_rtt_info(
            remote_physical_node_id=candidate.node_id,
            observed_rtt_ms=float(observed_rtt_ms),
        )

    def _select_candidate(self) -> RemotePhysicalNodePingCandidate | None:
        candidates = self.engine.services.identity_service.list_remote_physical_nodes_for_ping(
            limit=self._candidate_limit,
        )
        if not candidates:
            return None

        return candidates[0]
