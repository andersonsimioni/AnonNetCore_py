from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, timezone
from time import monotonic

from dht.records import DrtRecordPayload, parse_record
from crypto import sha512_hex
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
        self._drt_check_interval_seconds = (
            self.engine.services.config.virtual_route_maintenance_drt_check_interval_seconds
        )
        self._pending_route_timeout_seconds = (
            self.engine.services.config.virtual_route_maintenance_pending_route_timeout_seconds
        )
        self._candidate_limit = self.engine.services.config.virtual_route_maintenance_candidate_limit
        self._expected_round_trip_ttl_ms = (
            self.engine.services.config.virtual_route_maintenance_expected_round_trip_ttl_ms
        )
        self._last_route_build_by_virtual_node_id: dict[str, float] = {}
        self._last_drt_check_by_virtual_node_id: dict[str, float] = {}
        self._pending_seen_at_by_initial_path_id: dict[str, float] = {}

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
        if self._has_pending_route_build(local_virtual_node_id):
            return False

        active_route = (
            self.engine.services.route_service.get_active_initiator_resolution_for_local_virtual_node(
                local_virtual_node_id=local_virtual_node_id,
            )
        )
        if active_route is None:
            self.engine.services.log_service.info(
                "virtual_route_maintenance_runtime",
                "virtual node has no active route; scheduling route build",
                local_virtual_node_id=local_virtual_node_id,
            )
            return True

        if await self._active_route_missing_from_drt(
            local_virtual_node_id=local_virtual_node_id,
            initial_path_id=active_route.initial_path_id,
            final_path_id=active_route.final_path_id,
        ):
            return True

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

    async def _active_route_missing_from_drt(
        self,
        *,
        local_virtual_node_id: str,
        initial_path_id: str | None,
        final_path_id: str | None,
    ) -> bool:
        if not self._should_check_drt(local_virtual_node_id):
            return False

        self._last_drt_check_by_virtual_node_id[local_virtual_node_id] = monotonic()
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
            )
            if status == "not_found":
                self._invalidate_active_route(
                    local_virtual_node_id=local_virtual_node_id,
                    initial_path_id=initial_path_id,
                    final_path_id=final_path_id,
                    reason="active_route_missing_from_drt",
                )
                return True
            return False

        valid_final_path_ids = self._extract_valid_drt_final_path_ids(
            local_virtual_node_id=local_virtual_node_id,
            record_json=result.get("record_json"),
        )
        final_path_is_visible = bool(final_path_id and final_path_id in valid_final_path_ids)
        self.engine.services.log_service.info(
            "virtual_route_maintenance_runtime",
            "checked active virtual route visibility in drt",
            local_virtual_node_id=local_virtual_node_id,
            initial_path_id=initial_path_id,
            final_path_id=final_path_id,
            final_path_is_visible=final_path_is_visible,
            valid_entry_count=len(valid_final_path_ids),
            valid_final_path_ids=valid_final_path_ids[:8],
        )
        if final_path_is_visible:
            return False

        self._invalidate_active_route(
            local_virtual_node_id=local_virtual_node_id,
            initial_path_id=initial_path_id,
            final_path_id=final_path_id,
            reason="active_route_final_path_not_in_drt",
        )
        return True

    def _should_check_drt(self, local_virtual_node_id: str) -> bool:
        last_checked_at = self._last_drt_check_by_virtual_node_id.get(local_virtual_node_id)
        if last_checked_at is None:
            return True

        elapsed_seconds = monotonic() - last_checked_at
        if elapsed_seconds >= self._drt_check_interval_seconds:
            return True

        self.engine.services.log_service.debug(
            "virtual_route_maintenance_runtime",
            "virtual node drt health check interval not reached",
            local_virtual_node_id=local_virtual_node_id,
            elapsed_seconds=round(elapsed_seconds, 3),
            drt_check_interval_seconds=self._drt_check_interval_seconds,
        )
        return False

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
            if entry.final_path_id and not self._is_expired(entry.expires_at)
        ]

    def _invalidate_active_route(
        self,
        *,
        local_virtual_node_id: str,
        initial_path_id: str | None,
        final_path_id: str | None,
        reason: str,
    ) -> None:
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

    @staticmethod
    def _is_expired(expires_at: str) -> bool:
        try:
            parsed = datetime.fromisoformat(expires_at.replace("Z", "+00:00"))
        except ValueError:
            return True
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed <= datetime.now(timezone.utc)

    def _has_pending_route_build(self, local_virtual_node_id: str) -> bool:
        resolution = (
            self.engine.services.route_service.get_pending_initiator_resolution_for_local_virtual_node(
                local_virtual_node_id=local_virtual_node_id,
            )
        )
        if resolution is None:
            return False

        pending_elapsed_seconds = self._pending_elapsed_seconds(
            local_virtual_node_id=local_virtual_node_id,
            initial_path_id=resolution.initial_path_id,
        )
        if (
            resolution.initial_path_id is not None
            and pending_elapsed_seconds >= self._pending_route_timeout_seconds
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
                pending_timeout_seconds=self._pending_route_timeout_seconds,
            )
            return False

        self.engine.services.log_service.debug(
            "virtual_route_maintenance_runtime",
            "virtual node already has pending route build",
            local_virtual_node_id=local_virtual_node_id,
            initial_path_id=resolution.initial_path_id,
            final_path_id=resolution.final_path_id,
            status=resolution.status,
            pending_elapsed_seconds=round(pending_elapsed_seconds, 3),
        )
        return True

    def _pending_elapsed_seconds(
        self,
        *,
        local_virtual_node_id: str,
        initial_path_id: str | None,
    ) -> float:
        now = monotonic()
        started_at = self._last_route_build_by_virtual_node_id.get(local_virtual_node_id)
        if started_at is not None:
            return now - started_at

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
        initial_path_id = result.get("initial_path_id")
        if initial_path_id:
            self._pending_seen_at_by_initial_path_id[initial_path_id] = (
                self._last_route_build_by_virtual_node_id[local_virtual_node_id]
            )
        self.engine.services.log_service.info(
            "virtual_route_maintenance_runtime",
            "virtual route build started",
            local_virtual_node_id=local_virtual_node_id,
            initial_path_id=initial_path_id,
            first_hop_physical_node_id=route_pair.first_hop.node_id,
            final_physical_node_id=route_pair.final_node.node_id,
            remaining_ttl_ms=remaining_ttl_ms,
            expected_round_trip_ttl_ms=self._expected_round_trip_ttl_ms,
        )
