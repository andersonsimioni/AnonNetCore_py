from __future__ import annotations

from identity import RemotePhysicalNodeValidationCandidate
from sessions import is_observed_only_physical_endpoint
from sessions.models import utc_now

from .base import PeriodicRuntime


class PhysicalNodeValidationRuntime(PeriodicRuntime):
    """Valida physical nodes descobertos tentando estabelecer uma physical session."""

    def __init__(self, engine) -> None:
        super().__init__(
            engine,
            loop_interval_seconds=engine.services.config.physical_node_validation_runtime_interval_seconds,
            task_name="physical-node-validation-runtime",
        )
        self._validation_backoff_seconds = (
            self.engine.services.config.physical_node_validation_backoff_seconds
        )

    async def _run_once(self) -> None:
        candidate = self._select_candidate()
        if candidate is None:
            diagnostics = (
                self.engine.services.identity_service
                .build_remote_physical_node_route_diagnostics()
            )
            self.engine.services.log_service.debug(
                "physical_node_validation_runtime",
                "no physical node validation candidate available",
                validation_backoff_seconds=self._validation_backoff_seconds,
                **diagnostics,
            )
            return

        endpoints = self.engine.services.identity_service.list_remote_physical_node_endpoints(
            candidate.node_id,
        )
        self.engine.services.log_service.debug(
            "physical_node_validation_runtime",
            "selected physical node validation candidate",
            remote_physical_node_id=candidate.node_id,
            endpoint_count=len(endpoints),
            endpoints=[
                {
                    "transport": endpoint.transport,
                    "host": endpoint.host,
                    "port": endpoint.port,
                    "priority": endpoint.priority,
                }
                for endpoint in endpoints[:4]
            ],
        )

        if await self._try_validate_from_active_session(candidate):
            return

        await self._try_validate_candidate(candidate)

    async def _try_validate_from_active_session(
        self,
        candidate: RemotePhysicalNodeValidationCandidate,
    ) -> bool:
        session = self.engine.services.session_manager.get_active_physical_session_by_remote_node_id(
            candidate.node_id
        )
        if session is None:
            return False

        if not self._mark_validation_success(session.session_id, candidate.node_id):
            return False

        await self._publish_validated_remote_node(candidate.node_id)
        return True

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
        except Exception as error:
            self.engine.services.log_service.warning(
                "physical_node_validation_runtime",
                "physical node validation failed",
                remote_physical_node_id=candidate.node_id,
                error_type=type(error).__name__,
                error=repr(error),
            )
            return

        if not self._mark_validation_success(session_id, candidate.node_id):
            self.engine.services.log_service.warning(
                "physical_node_validation_runtime",
                "physical node validation session did not persist candidate",
                remote_physical_node_id=candidate.node_id,
                session_id=session_id,
            )
            return

        await self._publish_validated_remote_node(candidate.node_id)

    def _mark_validation_success(
        self,
        session_id: str,
        candidate_node_id: str,
    ) -> bool:
        session = self.engine.services.session_manager.get_session_by_session_id(session_id)
        if session is None or not session.transport or not session.remote_host or session.remote_port is None:
            return False
        if is_observed_only_physical_endpoint(session):
            self.engine.services.log_service.warning(
                "physical_node_validation_runtime",
                "skipped validation from transient observed endpoint",
                remote_physical_node_id=candidate_node_id,
                session_id=session_id,
                observed_host=session.remote_host,
                observed_port=session.remote_port,
            )
            return False

        validated_node = self.engine.services.identity_service.mark_remote_physical_node_validated(
            node_id=candidate_node_id,
            transport=session.transport,
            host=session.remote_host,
            port=session.remote_port,
        )
        if validated_node is None:
            self.engine.services.log_service.warning(
                "physical_node_validation_runtime",
                "validated physical node was not found in identity storage",
                remote_physical_node_id=candidate_node_id,
                session_id=session_id,
                transport=session.transport,
                host=session.remote_host,
                port=session.remote_port,
            )
            return False

        self.engine.services.log_service.info(
            "physical_node_validation_runtime",
            "physical node validated successfully",
            remote_physical_node_id=candidate_node_id,
            transport=session.transport,
            host=session.remote_host,
            port=session.remote_port,
        )
        return True

    async def _publish_validated_remote_node(
        self,
        remote_physical_node_id: str,
    ) -> None:
        publish_request = self.engine.services.identity_service.build_dpnt_publish_request_for_remote_physical_node(
            node_id=remote_physical_node_id,
        )
        if publish_request is None:
            self.engine.services.log_service.warning(
                "physical_node_validation_runtime",
                "validated physical node has no publishable dpnt descriptor",
                remote_physical_node_id=remote_physical_node_id,
            )
            return

        try:
            publish_result = await self.engine.services.protocol_clients.physical.dht.publish(
                namespace=publish_request["namespace"],
                logical_key=publish_request["logical_key"],
                record_json=publish_request["record_json"],
                expires_at=publish_request["expires_at"],
            )
        except Exception as error:
            self.engine.services.log_service.warning(
                "physical_node_validation_runtime",
                "dpnt publish failed for validated physical node",
                remote_physical_node_id=remote_physical_node_id,
                error=str(error),
            )
            return

        self.engine.services.log_service.info(
            "physical_node_validation_runtime",
            "dpnt publish finished for validated physical node",
            remote_physical_node_id=remote_physical_node_id,
            status=publish_result.get("status"),
            key=publish_result.get("key"),
        )

    def _build_backoff_delta(self):
        from datetime import timedelta

        return timedelta(seconds=self._validation_backoff_seconds)
