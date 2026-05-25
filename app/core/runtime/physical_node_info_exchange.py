from __future__ import annotations

import random
from datetime import timedelta

from identity import RemotePhysicalNodeExchangeCandidate
from sessions.models import utc_now

from .base import PeriodicRuntime


class PhysicalNodeInfoExchangeRuntime(PeriodicRuntime):
    """Executa periodicamente o protocolo de troca de physical nodes conhecidos."""

    def __init__(self, engine) -> None:
        super().__init__(
            engine,
            loop_interval_seconds=(
                engine.services.config.physical_node_info_exchange_runtime_interval_seconds
            ),
            task_name="physical-node-info-exchange-runtime",
        )

    async def _run_once(self) -> None:
        candidate = self._select_remote_node_for_exchange()
        if candidate is None:
            return

        session = await self._ensure_session(candidate.node_id)
        if session is None:
            return

        try:
            await self.engine.services.protocol_clients.physical.node_info_exchange.request_known_physical_nodes(
                session_id=session.session_id,
            )
        except Exception as error:
            self.engine.services.log_service.warning(
                "physical_node_info_exchange_runtime",
                "physical node info exchange request failed",
                remote_physical_node_id=candidate.node_id,
                session_id=session.session_id,
                error_type=type(error).__name__,
                error=repr(error),
            )
            return

    def _select_remote_node_for_exchange(self) -> RemotePhysicalNodeExchangeCandidate | None:
        candidates = self.engine.services.identity_service.list_remote_physical_nodes_for_info_exchange(
            limit=10,
        )
        if not candidates:
            return None

        shuffled_candidates = list(candidates)
        random.shuffle(shuffled_candidates)
        threshold = utc_now() - self._build_exchange_interval()
        for candidate in shuffled_candidates:
            if self._was_requested_recently(candidate.node_id, threshold):
                continue
            return candidate

        return None

    async def _ensure_session(self, remote_physical_node_id: str):
        active_session = self.engine.services.session_manager.get_active_physical_session_by_remote_node_id(
            remote_physical_node_id
        )
        if active_session is not None:
            return active_session

        try:
            session_id = await self.engine.services.protocol_clients.physical.session.start_session(
                remote_physical_node_id=remote_physical_node_id,
            )
        except Exception as error:
            self.engine.services.log_service.warning(
                "physical_node_info_exchange_runtime",
                "could not open session for physical node info exchange",
                remote_physical_node_id=remote_physical_node_id,
                error_type=type(error).__name__,
                error=repr(error),
            )
            return None

        return self.engine.services.session_manager.get_session_by_session_id(session_id)

    def _was_requested_recently(
        self,
        remote_physical_node_id: str,
        threshold,
    ) -> bool:
        return self.engine.services.identity_service.was_physical_node_info_exchange_requested_after(
            remote_physical_node_id=remote_physical_node_id,
            threshold=threshold,
        )

    def _build_exchange_interval(self) -> timedelta:
        return timedelta(seconds=self.engine.services.config.physical_node_info_exchange_interval_seconds)
