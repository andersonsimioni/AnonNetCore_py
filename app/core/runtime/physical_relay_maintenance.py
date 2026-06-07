from __future__ import annotations

from identity import RemotePhysicalRelayCandidate, RemotePhysicalNodeEndpointResult

from .base import PeriodicRuntime


class PhysicalRelayMaintenanceRuntime(PeriodicRuntime):
    """Keeps private physical nodes reachable through one public TCP relay."""

    def __init__(self, engine) -> None:
        super().__init__(
            engine,
            loop_interval_seconds=engine.services.config.physical_relay_maintenance_interval_seconds,
            task_name="physical-relay-maintenance-runtime",
        )
        self._candidate_limit = engine.services.config.physical_relay_candidate_limit
        self._selected_relay_physical_node_id: str | None = None
        self._logged_public_node_skip = False

    async def _run_once(self) -> None:
        if not self.engine.is_private_physical_node():
            if not self._logged_public_node_skip:
                self.engine.services.log_service.debug(
                    "physical_relay_maintenance_runtime",
                    "relay maintenance disabled for public physical node",
                )
                self._logged_public_node_skip = True
            return

        local_node = self.engine.services.identity_service.get_local_physical_node_result()
        if local_node is None:
            self.engine.clear_local_relay_endpoint()
            return

        previous_relay_physical_node_id = self._selected_relay_physical_node_id
        if await self._refresh_selected_relay(local_physical_node_id=local_node.id):
            return

        self.engine.clear_local_relay_endpoint()
        if previous_relay_physical_node_id is not None:
            if await self._select_relay(
                RemotePhysicalRelayCandidate(node_id=previous_relay_physical_node_id),
                local_physical_node_id=local_node.id,
            ):
                return

        for candidate in self._list_relay_candidates():
            if await self._select_relay(candidate, local_physical_node_id=local_node.id):
                return

        self.engine.services.log_service.warning(
            "physical_relay_maintenance_runtime",
            "private physical node could not keep a relay session active",
            candidate_limit=self._candidate_limit,
        )

    async def _refresh_selected_relay(self, *, local_physical_node_id: str) -> bool:
        if self._selected_relay_physical_node_id is None:
            return False

        session = (
            self.engine.services.session_manager
            .get_active_physical_session_by_remote_node_id(self._selected_relay_physical_node_id)
        )
        endpoint = self._get_relay_tcp_endpoint(self._selected_relay_physical_node_id)
        if session is None or endpoint is None:
            self.engine.services.log_service.warning(
                "physical_relay_maintenance_runtime",
                "selected relay is no longer active",
                relay_physical_node_id=self._selected_relay_physical_node_id,
                has_active_session=session is not None,
                has_tcp_endpoint=endpoint is not None,
            )
            self._selected_relay_physical_node_id = None
            return False

        self._announce_relay_endpoint(
            relay_physical_node_id=self._selected_relay_physical_node_id,
            local_physical_node_id=local_physical_node_id,
            endpoint=endpoint,
        )
        return True

    def _list_relay_candidates(self) -> list[RemotePhysicalRelayCandidate]:
        candidates = (
            self.engine.services.identity_service
            .list_remote_physical_nodes_for_relay_registration(limit=self._candidate_limit)
        )
        self.engine.services.log_service.debug(
            "physical_relay_maintenance_runtime",
            "loaded relay candidates",
            candidate_count=len(candidates),
            candidate_node_ids=[candidate.node_id for candidate in candidates],
        )
        return candidates

    async def _select_relay(
        self,
        candidate: RemotePhysicalRelayCandidate,
        *,
        local_physical_node_id: str,
    ) -> bool:
        endpoint = self._get_relay_tcp_endpoint(candidate.node_id)
        if endpoint is None:
            return False

        try:
            session_id = await self.engine.services.protocol_clients.physical.session.start_session(
                remote_physical_node_id=candidate.node_id,
            )
        except Exception as error:
            self.engine.services.log_service.warning(
                "physical_relay_maintenance_runtime",
                "failed to open physical session with relay candidate",
                relay_physical_node_id=candidate.node_id,
                error_type=type(error).__name__,
                error=repr(error),
            )
            return False

        session = self.engine.services.session_manager.get_session_by_session_id(session_id)
        if session is None or session.session_state != "active":
            self.engine.services.log_service.warning(
                "physical_relay_maintenance_runtime",
                "relay candidate session did not become active",
                relay_physical_node_id=candidate.node_id,
                session_id=session_id,
                session_state=getattr(session, "session_state", None),
            )
            return False

        self._selected_relay_physical_node_id = candidate.node_id
        self._announce_relay_endpoint(
            relay_physical_node_id=candidate.node_id,
            local_physical_node_id=local_physical_node_id,
            endpoint=endpoint,
        )
        self.engine.services.log_service.info(
            "physical_relay_maintenance_runtime",
            "private physical node selected relay",
            relay_physical_node_id=candidate.node_id,
            relay_session_id=session.session_id,
            relay_host=endpoint.host,
            relay_port=endpoint.port,
        )
        return True

    def _get_relay_tcp_endpoint(
        self,
        relay_physical_node_id: str,
    ) -> RemotePhysicalNodeEndpointResult | None:
        endpoints = self.engine.services.identity_service.list_remote_physical_node_endpoints(
            relay_physical_node_id,
            only_active=True,
        )
        for endpoint in endpoints:
            if endpoint.transport == "tcp":
                return endpoint
        return None

    def _announce_relay_endpoint(
        self,
        *,
        relay_physical_node_id: str,
        local_physical_node_id: str,
        endpoint: RemotePhysicalNodeEndpointResult,
    ) -> None:
        self.engine.set_local_relay_endpoint(
            relay_physical_node_id=relay_physical_node_id,
            target_physical_node_id=local_physical_node_id,
            host=endpoint.host,
            port=endpoint.port,
        )
