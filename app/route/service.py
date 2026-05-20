from __future__ import annotations

import json
from datetime import datetime, timezone

from dht import DrtRecordPayload, DrtRouteEntryRecord, serialize_record
from crypto import sha512_hex
from storage import DatabaseManager, get_database
from storage.models import RouteResolution


class RouteService:
    """Persiste e resolve o estado local das rotas fisicas."""

    def __init__(self, database: DatabaseManager | None = None) -> None:
        self.database = database or get_database()

    def create_hop_resolution(
        self,
        *,
        route_strategy: str,
        from_physical_node_id: str,
        to_physical_node_id: str,
        received_path_id: str,
        generated_path_id: str,
        from_physical_session_id: str | None = None,
    ) -> RouteResolution:
        with self.database.session_scope() as session:
            resolution = RouteResolution(
                local_role="intermediary",
                route_strategy=route_strategy,
                status="forwarding",
                from_physical_node_id=from_physical_node_id,
                to_physical_node_id=to_physical_node_id,
                received_path_id=received_path_id,
                generated_path_id=generated_path_id,
                is_valid=True,
                metadata_json=_dump_metadata(
                    {
                        "from_physical_session_id": from_physical_session_id,
                    }
                ),
            )
            session.add(resolution)
            session.flush()
            session.refresh(resolution)
            return resolution

    def get_resolution_by_received_path_id(
        self,
        *,
        received_path_id: str,
    ) -> RouteResolution | None:
        with self.database.session_scope() as session:
            return (
                session.query(RouteResolution)
                .filter(
                    RouteResolution.received_path_id == received_path_id,
                    RouteResolution.is_valid.is_(True),
                )
                .order_by(RouteResolution.id.desc())
                .first()
            )

    def get_resolution_by_generated_path_id(
        self,
        *,
        generated_path_id: str,
    ) -> RouteResolution | None:
        with self.database.session_scope() as session:
            return (
                session.query(RouteResolution)
                .filter(
                    RouteResolution.generated_path_id == generated_path_id,
                    RouteResolution.is_valid.is_(True),
                )
                .order_by(RouteResolution.id.desc())
                .first()
            )

    def create_endpoint_resolution(
        self,
        *,
        previous_physical_node_id: str,
        route_strategy: str,
        route_nonce: int,
        route_path_id: str,
        kyber_private_key_pem: str,
        kyber_public_key_pem: str,
    ) -> RouteResolution:
        with self.database.session_scope() as session:
            resolution = RouteResolution(
                local_role="final_endpoint",
                route_strategy=route_strategy,
                status="pending_validation",
                route_nonce=route_nonce,
                route_path_id=route_path_id,
                previous_physical_node_id=previous_physical_node_id,
                kyber_private_key_pem=kyber_private_key_pem,
                kyber_public_key_pem=kyber_public_key_pem,
                is_valid=True,
                metadata_json=None,
            )
            session.add(resolution)
            session.flush()
            session.refresh(resolution)
            return resolution

    def get_endpoint_resolution_by_path_id(
        self,
        *,
        route_path_id: str,
    ) -> RouteResolution | None:
        with self.database.session_scope() as session:
            return (
                session.query(RouteResolution)
                .filter(
                    RouteResolution.local_role == "final_endpoint",
                    RouteResolution.route_path_id == route_path_id,
                    RouteResolution.is_valid.is_(True),
                )
                .order_by(RouteResolution.id.desc())
                .first()
            )

    def get_endpoint_resolution_by_final_path_id(
        self,
        *,
        final_path_id: str,
    ) -> RouteResolution | None:
        with self.database.session_scope() as session:
            return (
                session.query(RouteResolution)
                .filter(
                    RouteResolution.local_role == "final_endpoint",
                    RouteResolution.final_path_id == final_path_id,
                    RouteResolution.is_valid.is_(True),
                )
                .order_by(RouteResolution.id.desc())
                .first()
            )

    def create_initiator_resolution(
        self,
        *,
        first_hop_physical_node_id: str,
        initial_path_id: str,
        final_physical_node_public_key: str,
        expected_round_trip_ttl_ms: int | None = None,
        route_strategy: str | None = None,
        route_nonce: int | None = None,
        local_virtual_node_id: str | None = None,
        final_path_id: str | None = None,
        virtual_node_signature: str | None = None,
    ) -> RouteResolution:
        with self.database.session_scope() as session:
            resolution = RouteResolution(
                local_role="initiator",
                route_strategy=route_strategy,
                status="pending_kem_offer",
                route_nonce=route_nonce,
                initial_path_id=initial_path_id,
                first_hop_physical_node_id=first_hop_physical_node_id,
                local_virtual_node_id=local_virtual_node_id,
                final_physical_node_public_key=final_physical_node_public_key,
                final_path_id=final_path_id,
                virtual_node_signature=virtual_node_signature,
                is_valid=True,
                metadata_json=_dump_metadata(
                    {
                        "expected_round_trip_ttl_ms": expected_round_trip_ttl_ms,
                    }
                ),
            )
            session.add(resolution)
            session.flush()
            session.refresh(resolution)
            return resolution

    def mark_initiator_resolution_ping_started(
        self,
        *,
        initial_path_id: str,
        ping_id: str,
        started_at_monotonic_ms: float,
    ) -> RouteResolution | None:
        with self.database.session_scope() as session:
            resolution = (
                session.query(RouteResolution)
                .filter(
                    RouteResolution.local_role == "initiator",
                    RouteResolution.initial_path_id == initial_path_id,
                    RouteResolution.is_valid.is_(True),
                )
                .order_by(RouteResolution.id.desc())
                .first()
            )
            if resolution is None:
                return None

            metadata = _load_metadata(resolution.metadata_json)
            metadata["last_ping_id"] = ping_id
            metadata["last_ping_started_at_monotonic_ms"] = started_at_monotonic_ms
            resolution.metadata_json = _dump_metadata(metadata)
            session.flush()
            session.refresh(resolution)
            return resolution

    def get_initiator_resolution_by_initial_path_id(
        self,
        *,
        initial_path_id: str,
    ) -> RouteResolution | None:
        with self.database.session_scope() as session:
            return (
                session.query(RouteResolution)
                .filter(
                    RouteResolution.local_role == "initiator",
                    RouteResolution.initial_path_id == initial_path_id,
                    RouteResolution.is_valid.is_(True),
                )
                .order_by(RouteResolution.id.desc())
                .first()
            )

    def get_pending_initiator_resolution_for_local_virtual_node(
        self,
        *,
        local_virtual_node_id: str,
    ) -> RouteResolution | None:
        pending_statuses = (
            "pending_kem_offer",
            "pending_final_validation",
        )
        with self.database.session_scope() as session:
            return (
                session.query(RouteResolution)
                .filter(
                    RouteResolution.local_role == "initiator",
                    RouteResolution.local_virtual_node_id == local_virtual_node_id,
                    RouteResolution.status.in_(pending_statuses),
                    RouteResolution.is_valid.is_(True),
                )
                .order_by(RouteResolution.id.desc())
                .first()
            )

    def get_any_pending_initiator_resolution(self) -> RouteResolution | None:
        pending_statuses = (
            "pending_kem_offer",
            "pending_final_validation",
        )
        with self.database.session_scope() as session:
            return (
                session.query(RouteResolution)
                .filter(
                    RouteResolution.local_role == "initiator",
                    RouteResolution.status.in_(pending_statuses),
                    RouteResolution.is_valid.is_(True),
                )
                .order_by(RouteResolution.id.asc())
                .first()
            )

    def invalidate_initiator_resolution(
        self,
        *,
        initial_path_id: str,
        reason: str,
    ) -> RouteResolution | None:
        with self.database.session_scope() as session:
            resolution = (
                session.query(RouteResolution)
                .filter(
                    RouteResolution.local_role == "initiator",
                    RouteResolution.initial_path_id == initial_path_id,
                    RouteResolution.is_valid.is_(True),
                )
                .order_by(RouteResolution.id.desc())
                .first()
            )
            if resolution is None:
                return None

            metadata = _load_metadata(resolution.metadata_json)
            metadata["invalidated_reason"] = reason
            metadata["invalidated_at"] = datetime.now(timezone.utc).isoformat()
            resolution.status = "invalid"
            resolution.is_valid = False
            resolution.metadata_json = _dump_metadata(metadata)
            session.flush()
            session.refresh(resolution)
            return resolution

    def get_active_initiator_resolution_for_local_virtual_node(
        self,
        *,
        local_virtual_node_id: str,
    ) -> RouteResolution | None:
        with self.database.session_scope() as session:
            return (
                session.query(RouteResolution)
                .filter(
                    RouteResolution.local_role == "initiator",
                    RouteResolution.local_virtual_node_id == local_virtual_node_id,
                    RouteResolution.status == "active",
                    RouteResolution.final_path_id.is_not(None),
                    RouteResolution.is_valid.is_(True),
                )
                .order_by(RouteResolution.id.desc())
                .first()
            )

    def update_initiator_resolution_validation_payload(
        self,
        *,
        initial_path_id: str,
        local_virtual_node_id: str,
        final_path_id: str,
        virtual_node_signature: str,
        shared_secret_hex: str,
    ) -> RouteResolution | None:
        with self.database.session_scope() as session:
            resolution = (
                session.query(RouteResolution)
                .filter(
                    RouteResolution.local_role == "initiator",
                    RouteResolution.initial_path_id == initial_path_id,
                    RouteResolution.is_valid.is_(True),
                )
                .order_by(RouteResolution.id.desc())
                .first()
            )
            if resolution is None:
                return None

            resolution.local_virtual_node_id = local_virtual_node_id
            resolution.final_path_id = final_path_id
            resolution.virtual_node_signature = virtual_node_signature
            resolution.shared_secret_hex = shared_secret_hex
            resolution.status = "pending_final_validation"
            session.flush()
            session.refresh(resolution)
            return resolution

    def mark_initiator_resolution_active(
        self,
        *,
        initial_path_id: str,
        final_path_id: str,
        virtual_node_signature: str,
        physical_node_signature: str,
        public_route_acceptance_signature: str,
    ) -> RouteResolution | None:
        with self.database.session_scope() as session:
            resolution = (
                session.query(RouteResolution)
                .filter(
                    RouteResolution.local_role == "initiator",
                    RouteResolution.initial_path_id == initial_path_id,
                    RouteResolution.is_valid.is_(True),
                )
                .order_by(RouteResolution.id.desc())
                .first()
            )
            if resolution is None:
                return None

            resolution.final_path_id = final_path_id
            resolution.virtual_node_signature = virtual_node_signature
            resolution.physical_node_signature = physical_node_signature
            resolution.public_route_acceptance_signature = public_route_acceptance_signature
            resolution.status = "active"
            session.flush()
            session.refresh(resolution)
            return resolution

    def update_endpoint_resolution_validation_context(
        self,
        *,
        route_path_id: str,
        shared_secret_hex: str,
        final_path_id: str,
        remote_virtual_node_public_key: str,
        virtual_node_signature: str,
        physical_node_signature: str,
        public_route_acceptance_signature: str,
        remote_virtual_node_id: str,
        expected_round_trip_ttl_ms: int,
        ping_id: str,
        ping_sent_at_monotonic_ms: float,
    ) -> RouteResolution | None:
        with self.database.session_scope() as session:
            resolution = (
                session.query(RouteResolution)
                .filter(
                    RouteResolution.local_role == "final_endpoint",
                    RouteResolution.route_path_id == route_path_id,
                    RouteResolution.is_valid.is_(True),
                )
                .order_by(RouteResolution.id.desc())
                .first()
            )
            if resolution is None:
                return None

            resolution.shared_secret_hex = shared_secret_hex
            resolution.final_path_id = final_path_id
            resolution.virtual_node_signature = virtual_node_signature
            resolution.physical_node_signature = physical_node_signature
            resolution.public_route_acceptance_signature = public_route_acceptance_signature
            resolution.status = "pending_ping_validation"

            metadata = _load_metadata(resolution.metadata_json)
            metadata["remote_virtual_node_id"] = remote_virtual_node_id
            metadata["remote_virtual_node_public_key"] = remote_virtual_node_public_key
            metadata["expected_round_trip_ttl_ms"] = expected_round_trip_ttl_ms
            metadata["last_ping_id"] = ping_id
            metadata["last_ping_sent_at_monotonic_ms"] = ping_sent_at_monotonic_ms
            resolution.metadata_json = _dump_metadata(metadata)
            session.flush()
            session.refresh(resolution)
            return resolution

    def mark_endpoint_resolution_active(
        self,
        *,
        route_path_id: str,
    ) -> RouteResolution | None:
        with self.database.session_scope() as session:
            resolution = (
                session.query(RouteResolution)
                .filter(
                    RouteResolution.local_role == "final_endpoint",
                    RouteResolution.route_path_id == route_path_id,
                    RouteResolution.is_valid.is_(True),
                )
                .order_by(RouteResolution.id.desc())
                .first()
            )
            if resolution is None:
                return None

            resolution.status = "active"
            session.flush()
            session.refresh(resolution)
            return resolution

    def build_drt_publish_request_from_endpoint_resolution(
        self,
        *,
        route_path_id: str,
        physical_node_public_key: str,
        rtt_physical_node_signature: str,
        observed_round_trip_ms: int,
        expires_at: str,
    ) -> dict[str, str] | None:
        resolution = self.get_endpoint_resolution_by_path_id(route_path_id=route_path_id)
        if resolution is None:
            return None

        metadata = _load_metadata(resolution.metadata_json)
        pk_virtual_node = metadata.get("remote_virtual_node_public_key")
        if not isinstance(pk_virtual_node, str) or not pk_virtual_node:
            return None
        virtual_node_id = metadata.get("remote_virtual_node_id")
        if not isinstance(virtual_node_id, str) or not virtual_node_id:
            virtual_node_id = sha512_hex(pk_virtual_node.encode("utf-8"))
        if not isinstance(resolution.virtual_node_signature, str) or not resolution.virtual_node_signature:
            return None
        if not isinstance(resolution.physical_node_signature, str) or not resolution.physical_node_signature:
            return None
        if not isinstance(resolution.final_path_id, str) or not resolution.final_path_id:
            return None

        now = datetime.now(timezone.utc)
        record_payload = DrtRecordPayload(
            pk_virtual_node=pk_virtual_node,
            route_entries=[
                DrtRouteEntryRecord(
                    pk_physical_node=physical_node_public_key,
                    virtual_node_signature=resolution.virtual_node_signature,
                    final_path_id=resolution.final_path_id,
                    entry_point_virtual_node_signature=resolution.virtual_node_signature,
                    entry_point_physical_node_signature=resolution.physical_node_signature,
                    physical_node_signature=resolution.physical_node_signature,
                    expires_at=expires_at,
                    rtt=observed_round_trip_ms,
                    rtt_physical_node_signature=rtt_physical_node_signature,
                )
            ],
            last_update=now.isoformat(),
        )
        return {
            "namespace": "drt",
            "logical_key": virtual_node_id,
            "record_json": serialize_record(record_payload),
            "expires_at": expires_at,
        }


def _load_metadata(metadata_json: str | None) -> dict[str, object]:
    if not metadata_json:
        return {}

    try:
        payload = json.loads(metadata_json)
    except json.JSONDecodeError:
        return {}

    return payload if isinstance(payload, dict) else {}


def _dump_metadata(metadata: dict[str, object]) -> str | None:
    if not metadata:
        return None
    return json.dumps(metadata, separators=(",", ":"), sort_keys=True)
