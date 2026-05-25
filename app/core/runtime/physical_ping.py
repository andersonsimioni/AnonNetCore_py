from __future__ import annotations

from identity import RemotePhysicalNodePingCandidate

from .base import PeriodicRuntime


class PhysicalPingRuntime(PeriodicRuntime):
    """Executa PING periodico em physical nodes ativos e persiste estatisticas de RTT."""

    def __init__(self, engine) -> None:
        super().__init__(
            engine,
            loop_interval_seconds=engine.services.config.physical_ping_runtime_interval_seconds,
            task_name="physical-ping-runtime",
        )
        self._candidate_limit = self.engine.services.config.physical_ping_runtime_candidate_limit

    async def _run_once(self) -> None:
        candidate = self._select_candidate()
        if candidate is None:
            return

        try:
            ping_result = await self.engine.services.protocol_clients.physical.ping.ping_physical_node(
                remote_physical_node_id=candidate.node_id,
            )
        except Exception as error:
            self.engine.services.log_service.warning(
                "physical_ping_runtime",
                "physical ping runtime candidate failed",
                remote_physical_node_id=candidate.node_id,
                error_type=type(error).__name__,
                error=repr(error),
            )
            return

        observed_rtt_ms = ping_result.get("observed_rtt_ms")
        if not isinstance(observed_rtt_ms, (int, float)):
            self.engine.services.log_service.warning(
                "physical_ping_runtime",
                "physical ping result did not include a valid rtt",
                remote_physical_node_id=candidate.node_id,
                result_status=ping_result.get("status"),
            )
            return

        self.engine.services.identity_service.upsert_rtt_info(
            remote_physical_node_id=candidate.node_id,
            observed_rtt_ms=float(observed_rtt_ms),
        )
        self.engine.services.log_service.debug(
            "physical_ping_runtime",
            "stored physical ping rtt",
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
