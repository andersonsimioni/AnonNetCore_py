from __future__ import annotations

from dataclasses import dataclass
from random import SystemRandom
from time import monotonic

from common import is_expired_iso_datetime
from dht.records import DrtRecordPayload, parse_record
from crypto import sha512_hex
from identity import RemotePhysicalNodeRouteCandidate

from .base import PeriodicRuntime


@dataclass(slots=True, frozen=True)
class RouteBuildPair:
    first_hop: RemotePhysicalNodeRouteCandidate
    final_node: RemotePhysicalNodeRouteCandidate


class VirtualRouteMaintenanceRuntime(PeriodicRuntime):
    """Mantem um minimo de rotas fisicas publicadas para cada virtual node local ativo."""

    def __init__(self, engine) -> None:
        super().__init__(
            engine,
            loop_interval_seconds=engine.services.config.virtual_route_maintenance_interval_seconds,
            task_name="virtual-route-maintenance-runtime",
        )
        self._route_min_online_routes = max(
            1,
            int(self.engine.services.config.virtual_route_min_published_routes),
        )
        self._max_pending_routes_before_first_active_route = max(
            1,
            int(
                self.engine.services.config
                .virtual_route_max_pending_builds_before_first_route
            ),
        )
        self._pending_route_timeout_seconds = (
            self.engine.services.config.virtual_route_build_timeout_seconds
        )
        self._candidate_limit = self.engine.services.config.random_walk_candidate_limit
        self._expected_round_trip_ttl_ms = (
            self.engine.services.config.default_random_walk_ttl_ms
        )
        self._pending_seen_at_by_initial_path_id: dict[str, float] = {}
        self._random = SystemRandom()

    async def _run_once(self) -> None:
        local_virtual_nodes = self.engine.services.identity_service.list_local_virtual_nodes(
            only_active=True,
        )
        if not local_virtual_nodes:
            return

        for virtual_node in local_virtual_nodes:
            if not await self._should_build_new_route(virtual_node.id):
                continue

            route_pair = self._select_route_build_pair()
            if route_pair is None:
                diagnostics = (
                    self.engine.services.identity_service
                    .build_remote_physical_node_route_diagnostics()
                )
                self.engine.services.log_service.debug(
                    "virtual_route_maintenance_runtime",
                    "not enough validated physical nodes to build virtual route",
                    local_virtual_node_id=virtual_node.id,
                    candidate_limit=self._candidate_limit,
                    **diagnostics,
                )
                continue

            await self._start_route_build(
                local_virtual_node_id=virtual_node.id,
                route_pair=route_pair,
            )
            # Start one route per tick. Pending routes are tracked per VN, so
            # one slow route does not block maintenance for all local VNs.
            return

    async def _should_build_new_route(self, local_virtual_node_id: str) -> bool:
        pending_route_count = self._expire_stale_pending_route_builds(local_virtual_node_id)

        active_route = (
            self.engine.services.route_service.get_active_initiator_resolution_for_local_virtual_node(
                local_virtual_node_id=local_virtual_node_id,
            )
        )
        if active_route is None:
            if pending_route_count >= self._max_pending_routes_before_first_active_route:
                self.engine.services.log_service.debug(
                    "virtual_route_maintenance_runtime",
                    "virtual node is waiting for pending routes before first active route",
                    local_virtual_node_id=local_virtual_node_id,
                    pending_route_count=pending_route_count,
                    max_pending_routes=self._max_pending_routes_before_first_active_route,
                    min_online_routes=self._route_min_online_routes,
                )
                return False

            self.engine.services.log_service.info(
                "virtual_route_maintenance_runtime",
                "virtual node has no active route; scheduling route build",
                local_virtual_node_id=local_virtual_node_id,
                pending_route_count=pending_route_count,
                min_online_routes=self._route_min_online_routes,
            )
            return True

        route_health = await self._read_drt_route_health(
            local_virtual_node_id=local_virtual_node_id,
            initial_path_id=active_route.initial_path_id,
            final_path_id=active_route.final_path_id,
        )
        if route_health is None:
            return False
        if route_health.should_build_more:
            missing_route_count = max(
                0,
                self._route_min_online_routes - route_health.online_route_count,
            )
            if pending_route_count >= missing_route_count:
                self.engine.services.log_service.debug(
                    "virtual_route_maintenance_runtime",
                    "virtual node is waiting for pending routes to satisfy drt minimum",
                    local_virtual_node_id=local_virtual_node_id,
                    online_route_count=route_health.online_route_count,
                    pending_route_count=pending_route_count,
                    missing_route_count=missing_route_count,
                    min_online_routes=self._route_min_online_routes,
                )
                return False

            self.engine.services.log_service.info(
                "virtual_route_maintenance_runtime",
                "virtual node needs another route to satisfy drt minimum",
                local_virtual_node_id=local_virtual_node_id,
                online_route_count=route_health.online_route_count,
                pending_route_count=pending_route_count,
                missing_route_count=missing_route_count,
                min_online_routes=self._route_min_online_routes,
            )
            return True

        self.engine.services.log_service.debug(
            "virtual_route_maintenance_runtime",
            "virtual node has enough online routes in drt",
            local_virtual_node_id=local_virtual_node_id,
            online_route_count=route_health.online_route_count,
            pending_route_count=pending_route_count,
            min_online_routes=self._route_min_online_routes,
            active_final_path_id=active_route.final_path_id,
            active_final_path_is_visible=route_health.active_final_path_is_visible,
        )
        return False

    async def _read_drt_route_health(
        self,
        *,
        local_virtual_node_id: str,
        initial_path_id: str | None,
        final_path_id: str | None,
    ) -> "DrtRouteHealth | None":
        result = await self.engine.services.protocol_clients.physical.dht.query(
            namespace="drt",
            logical_key=local_virtual_node_id,
        )
        status = str(result.get("status") or "not_found")
        has_record = isinstance(result.get("record_json"), str) and bool(result.get("record_json"))

        if status != "found":
            self.engine.services.log_service.warning(
                "virtual_route_maintenance_runtime",
                "active virtual route is not visible in drt",
                local_virtual_node_id=local_virtual_node_id,
                initial_path_id=initial_path_id,
                final_path_id=final_path_id,
                drt_status=status,
                drt_reason=result.get("reason"),
                has_record=has_record,
                min_online_routes=self._route_min_online_routes,
            )
            if status == "not_found":
                self._invalidate_active_route(
                    local_virtual_node_id=local_virtual_node_id,
                    initial_path_id=initial_path_id,
                    final_path_id=final_path_id,
                    reason="active_route_missing_from_drt",
                )
                return DrtRouteHealth(
                    online_route_count=0,
                    active_final_path_is_visible=False,
                    should_build_more=True,
                )
            return None

        valid_final_path_ids = self._extract_valid_drt_final_path_ids(
            local_virtual_node_id=local_virtual_node_id,
            record_json=result.get("record_json"),
        )
        final_path_is_visible = bool(final_path_id and final_path_id in valid_final_path_ids)
        should_build_more = len(valid_final_path_ids) < self._route_min_online_routes
        self.engine.services.log_service.info(
            "virtual_route_maintenance_runtime",
            "checked virtual route inventory in drt",
            local_virtual_node_id=local_virtual_node_id,
            initial_path_id=initial_path_id,
            final_path_id=final_path_id,
            final_path_is_visible=final_path_is_visible,
            online_route_count=len(valid_final_path_ids),
            min_online_routes=self._route_min_online_routes,
            should_build_more=should_build_more,
            valid_final_path_ids=valid_final_path_ids[:8],
        )
        if should_build_more:
            return DrtRouteHealth(
                online_route_count=len(valid_final_path_ids),
                active_final_path_is_visible=final_path_is_visible,
                should_build_more=True,
            )

        return DrtRouteHealth(
            online_route_count=len(valid_final_path_ids),
            active_final_path_is_visible=final_path_is_visible,
            should_build_more=False,
        )

    def _extract_valid_drt_final_path_ids(
        self,
        *,
        local_virtual_node_id: str,
        record_json: object,
    ) -> list[str]:
        if not isinstance(record_json, str) or not record_json:
            self.engine.services.log_service.warning(
                "virtual_route_maintenance_runtime",
                "drt record is missing record_json during route health check",
                local_virtual_node_id=local_virtual_node_id,
            )
            return []

        try:
            record = parse_record("drt", record_json)
        except Exception as error:
            self.engine.services.log_service.warning(
                "virtual_route_maintenance_runtime",
                "drt record could not be parsed during route health check",
                local_virtual_node_id=local_virtual_node_id,
                error=str(error),
            )
            return []

        if not isinstance(record, DrtRecordPayload):
            return []

        record_virtual_node_id = sha512_hex(record.pk_virtual_node.encode("utf-8"))
        if record_virtual_node_id != local_virtual_node_id:
            self.engine.services.log_service.warning(
                "virtual_route_maintenance_runtime",
                "drt record belongs to another virtual node during route health check",
                local_virtual_node_id=local_virtual_node_id,
                record_virtual_node_id=record_virtual_node_id,
            )
            return []

        return [
            entry.final_path_id
            for entry in record.route_entries
            if entry.final_path_id and not is_expired_iso_datetime(entry.expires_at)
        ]

    def _invalidate_active_route(
        self,
        *,
        local_virtual_node_id: str,
        initial_path_id: str | None,
        final_path_id: str | None,
        reason: str,
    ) -> None:
        if self.engine.services.session_manager.has_active_virtual_session_bound_to_route(
            initial_path_id,
            final_path_id,
        ):
            self.engine.services.log_service.warning(
                "virtual_route_maintenance_runtime",
                "kept active virtual route because it is bound to an active virtual session",
                local_virtual_node_id=local_virtual_node_id,
                initial_path_id=initial_path_id,
                final_path_id=final_path_id,
                reason=reason,
            )
            return

        if not initial_path_id:
            self.engine.services.log_service.warning(
                "virtual_route_maintenance_runtime",
                "could not invalidate active route without initial path id",
                local_virtual_node_id=local_virtual_node_id,
                final_path_id=final_path_id,
                reason=reason,
            )
            return

        invalidated = self.engine.services.route_service.invalidate_initiator_resolution(
            initial_path_id=initial_path_id,
            reason=reason,
        )
        self.engine.services.log_service.warning(
            "virtual_route_maintenance_runtime",
            "invalidated active virtual route for rebuild",
            local_virtual_node_id=local_virtual_node_id,
            initial_path_id=initial_path_id,
            final_path_id=final_path_id,
            reason=reason,
            invalidated=invalidated is not None,
        )

    def _expire_stale_pending_route_builds(self, local_virtual_node_id: str) -> int:
        pending_resolutions = (
            self.engine.services.route_service
            .list_pending_initiator_resolutions_for_local_virtual_node(
                local_virtual_node_id=local_virtual_node_id,
            )
        )
        if not pending_resolutions:
            return 0

        active_pending_count = 0
        for resolution in pending_resolutions:
            pending_elapsed_seconds = self._pending_elapsed_seconds(
                initial_path_id=resolution.initial_path_id,
            )
            pending_timeout_seconds = self._pending_timeout_seconds_for_status(
                status=resolution.status,
            )
            if (
                resolution.initial_path_id is not None
                and pending_elapsed_seconds >= pending_timeout_seconds
            ):
                self.engine.services.route_service.invalidate_initiator_resolution(
                    initial_path_id=resolution.initial_path_id,
                    reason="virtual_route_build_pending_timeout",
                )
                self._pending_seen_at_by_initial_path_id.pop(resolution.initial_path_id, None)
                self.engine.services.log_service.warning(
                    "virtual_route_maintenance_runtime",
                    "expired pending virtual route build",
                    local_virtual_node_id=local_virtual_node_id,
                    initial_path_id=resolution.initial_path_id,
                    final_path_id=resolution.final_path_id,
                    status=resolution.status,
                    pending_elapsed_seconds=round(pending_elapsed_seconds, 3),
                    pending_timeout_seconds=pending_timeout_seconds,
                    base_pending_timeout_seconds=self._pending_route_timeout_seconds,
                )
                continue

            active_pending_count += 1
            self.engine.services.log_service.debug(
                "virtual_route_maintenance_runtime",
                "virtual node has pending route build",
                local_virtual_node_id=local_virtual_node_id,
                initial_path_id=resolution.initial_path_id,
                final_path_id=resolution.final_path_id,
                status=resolution.status,
                pending_elapsed_seconds=round(pending_elapsed_seconds, 3),
                pending_timeout_seconds=pending_timeout_seconds,
            )
        return active_pending_count

    def _pending_timeout_seconds_for_status(self, *, status: str | None) -> float:
        if status == "pending_final_validation":
            return self._pending_route_timeout_seconds * 2.0
        return self._pending_route_timeout_seconds

    def _pending_elapsed_seconds(
        self,
        *,
        initial_path_id: str | None,
    ) -> float:
        now = monotonic()
        if initial_path_id is None:
            return 0.0

        first_seen_at = self._pending_seen_at_by_initial_path_id.setdefault(initial_path_id, now)
        return now - first_seen_at

    def _select_route_build_pair(self) -> RouteBuildPair | None:
        candidates = self.engine.services.identity_service.list_remote_physical_nodes_for_random_walk_ttl(
            limit=self._candidate_limit,
        )
        self.engine.services.log_service.debug(
            "virtual_route_maintenance_runtime",
            "selected route build candidate pool",
            candidate_count=len(candidates),
            candidate_limit=self._candidate_limit,
            candidate_node_ids=[candidate.node_id for candidate in candidates[:8]],
        )
        if len(candidates) < 2:
            return None

        first_hop = self._random.choice(candidates)
        final_candidates = [
            candidate
            for candidate in candidates
            if candidate.node_id != first_hop.node_id
        ]
        if not final_candidates:
            return None

        final_node = self._random.choice(final_candidates)
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

        route_build_started_at = monotonic()
        initial_path_id = result.get("initial_path_id")
        if initial_path_id:
            self._pending_seen_at_by_initial_path_id[initial_path_id] = route_build_started_at
        self.engine.services.log_service.info(
            "virtual_route_maintenance_runtime",
            "virtual route build started",
            local_virtual_node_id=local_virtual_node_id,
            initial_path_id=initial_path_id,
            first_hop_physical_node_id=route_pair.first_hop.node_id,
            final_physical_node_id=route_pair.final_node.node_id,
            min_online_routes=self._route_min_online_routes,
            remaining_ttl_ms=remaining_ttl_ms,
            expected_round_trip_ttl_ms=self._expected_round_trip_ttl_ms,
        )


@dataclass(slots=True, frozen=True)
class DrtRouteHealth:
    online_route_count: int
    active_final_path_is_visible: bool
    should_build_more: bool
