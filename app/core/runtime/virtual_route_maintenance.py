from __future__ import annotations

import asyncio
from dataclasses import dataclass
from time import monotonic

from identity import RemotePhysicalNodeRouteCandidate


@dataclass(slots=True, frozen=True)
class RouteBuildPair:
    first_hop: RemotePhysicalNodeRouteCandidate
    final_node: RemotePhysicalNodeRouteCandidate


class VirtualRouteMaintenanceRuntime:
    """Mantem uma rota fisica publicada para cada virtual node local ativo."""

    def __init__(self, engine) -> None:
        self.engine = engine
        self._task: asyncio.Task[None] | None = None
        self._stop_event = asyncio.Event()
        self._loop_interval_seconds = (
            self.engine.services.config.virtual_route_maintenance_runtime_interval_seconds
        )
        self._route_build_interval_seconds = (
            self.engine.services.config.virtual_route_maintenance_route_build_interval_seconds
        )
        self._candidate_limit = self.engine.services.config.virtual_route_maintenance_candidate_limit
        self._expected_round_trip_ttl_ms = (
            self.engine.services.config.virtual_route_maintenance_expected_round_trip_ttl_ms
        )
        self._last_route_build_by_virtual_node_id: dict[str, float] = {}

    async def start(self) -> None:
        if self._task is not None and not self._task.done():
            return

        self._stop_event = asyncio.Event()
        self._task = asyncio.create_task(
            self._run_loop(),
            name="virtual-route-maintenance-runtime",
        )

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
        local_virtual_nodes = self.engine.services.identity_service.list_local_virtual_nodes(
            only_active=True,
        )
        if not local_virtual_nodes:
            return

        for virtual_node in local_virtual_nodes:
            if not self._should_build_new_route(virtual_node.id):
                continue

            route_pair = self._select_route_build_pair()
            if route_pair is None:
                self.engine.services.log_service.debug(
                    "virtual_route_maintenance_runtime",
                    "not enough validated physical nodes to build virtual route",
                    local_virtual_node_id=virtual_node.id,
                    candidate_limit=self._candidate_limit,
                )
                continue

            await self._start_route_build(
                local_virtual_node_id=virtual_node.id,
                route_pair=route_pair,
            )

    def _should_build_new_route(self, local_virtual_node_id: str) -> bool:
        if self._has_pending_route_build(local_virtual_node_id):
            return False

        last_build_at = self._last_route_build_by_virtual_node_id.get(local_virtual_node_id)
        if last_build_at is None:
            return True

        elapsed_seconds = monotonic() - last_build_at
        if elapsed_seconds >= self._route_build_interval_seconds:
            return True

        self.engine.services.log_service.debug(
            "virtual_route_maintenance_runtime",
            "virtual node route build interval not reached",
            local_virtual_node_id=local_virtual_node_id,
            elapsed_seconds=round(elapsed_seconds, 3),
            route_build_interval_seconds=self._route_build_interval_seconds,
        )
        return False

    def _has_pending_route_build(self, local_virtual_node_id: str) -> bool:
        resolution = (
            self.engine.services.route_service.get_pending_initiator_resolution_for_local_virtual_node(
                local_virtual_node_id=local_virtual_node_id,
            )
        )
        if resolution is None:
            return False

        self.engine.services.log_service.debug(
            "virtual_route_maintenance_runtime",
            "virtual node already has pending route build",
            local_virtual_node_id=local_virtual_node_id,
            initial_path_id=resolution.initial_path_id,
            final_path_id=resolution.final_path_id,
            status=resolution.status,
        )
        return True

    def _select_route_build_pair(self) -> RouteBuildPair | None:
        candidates = self.engine.services.identity_service.list_remote_physical_nodes_for_random_walk_ttl(
            limit=self._candidate_limit,
        )
        if len(candidates) < 2:
            return None

        first_hop = candidates[0]
        final_node = next(
            (candidate for candidate in candidates[1:] if candidate.node_id != first_hop.node_id),
            None,
        )
        if final_node is None:
            return None

        return RouteBuildPair(first_hop=first_hop, final_node=final_node)

    async def _start_route_build(
        self,
        *,
        local_virtual_node_id: str,
        route_pair: RouteBuildPair,
    ) -> None:
        remaining_ttl_ms = max(1, int(self._expected_round_trip_ttl_ms / 2))
        try:
            result = await self.engine.services.protocol_clients.physical.route_build.start_random_walk_ttl_route(
                local_virtual_node_id=local_virtual_node_id,
                first_hop_physical_node_id=route_pair.first_hop.node_id,
                final_physical_node_public_key=route_pair.final_node.public_key,
                remaining_ttl_ms=remaining_ttl_ms,
                expected_round_trip_ttl_ms=self._expected_round_trip_ttl_ms,
            )
        except Exception as error:
            self.engine.services.log_service.warning(
                "virtual_route_maintenance_runtime",
                "virtual route build failed",
                local_virtual_node_id=local_virtual_node_id,
                first_hop_physical_node_id=route_pair.first_hop.node_id,
                final_physical_node_id=route_pair.final_node.node_id,
                error=str(error),
            )
            return

        self._last_route_build_by_virtual_node_id[local_virtual_node_id] = monotonic()
        self.engine.services.log_service.info(
            "virtual_route_maintenance_runtime",
            "virtual route build started",
            local_virtual_node_id=local_virtual_node_id,
            initial_path_id=result.get("initial_path_id"),
            first_hop_physical_node_id=route_pair.first_hop.node_id,
            final_physical_node_id=route_pair.final_node.node_id,
            remaining_ttl_ms=remaining_ttl_ms,
            expected_round_trip_ttl_ms=self._expected_round_trip_ttl_ms,
        )
