from __future__ import annotations

from datetime import datetime, timezone
import json

from sqlalchemy import func, or_, select

from crypto import generate_dilithium_key_pair, sha512_hex
from dht import DpntRecordPayload, serialize_record
from storage import DatabaseManager, get_database
from storage.models import (
    LocalPhysicalNodeIdentity,
    LocalVirtualNodeIdentity,
    NodeEndpoint,
    PhysicalNodeInfoExchangeState,
    RemotePhysicalNodeIdentity,
    RemoteVirtualNodeIdentity,
    RttInfo,
)

from .models import (
    PhysicalNodeIdentityResult,
    RemotePhysicalNodeExchangeCandidate,
    RemotePhysicalNodeEndpointResult,
    RemotePhysicalNodePingCandidate,
    RemotePhysicalNodeRouteCandidate,
    RemotePhysicalNodeValidationCandidate,
    VirtualNodeIdentityCreateInput,
)


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _merge_notes_json(
    existing_notes_json: str | None,
    new_notes_json: str | None,
) -> str | None:
    existing_notes = _load_json_object(existing_notes_json)
    new_notes = _load_json_object(new_notes_json)
    merged_notes = dict(existing_notes)

    for key, value in new_notes.items():
        if value is None:
            continue
        if isinstance(value, str) and not value:
            continue
        if isinstance(value, list) and not value:
            continue
        merged_notes[key] = value

    if not merged_notes:
        return None

    return json.dumps(merged_notes, separators=(",", ":"))


def _load_json_object(payload: str | None) -> dict[str, object]:
    if not payload:
        return {}

    try:
        parsed = json.loads(payload)
    except json.JSONDecodeError:
        return {}

    if not isinstance(parsed, dict):
        return {}

    return parsed


class IdentityService:
    """Gerencia identidades locais do node fisico e dos nodes virtuais."""

    def __init__(self, database: DatabaseManager | None = None) -> None:
        self.database = database or get_database()

    def ensure_local_physical_node(self) -> LocalPhysicalNodeIdentity:
        existing_node = self.get_local_physical_node()
        if existing_node is not None:
            return existing_node

        key_pair = generate_dilithium_key_pair()
        public_key = key_pair.public_key_pem
        private_key_encrypted = self._protect_private_key(key_pair.private_key_pem)
        physical_node_id = self._build_node_id(public_key)

        with self.database.session_scope() as session:
            physical_node = LocalPhysicalNodeIdentity(
                id=physical_node_id,
                public_key=public_key,
                private_key_encrypted=private_key_encrypted,
                key_algorithm="ml-dsa-65",
                status="active",
            )
            session.add(physical_node)
            session.flush()
            session.refresh(physical_node)
            return physical_node

    def get_local_physical_node(self) -> LocalPhysicalNodeIdentity | None:
        with self.database.session_scope() as session:
            query = select(LocalPhysicalNodeIdentity).order_by(LocalPhysicalNodeIdentity.created_at)
            return session.scalar(query)

    def get_local_physical_node_result(self) -> PhysicalNodeIdentityResult | None:
        node = self.get_local_physical_node()
        if node is None:
            return None

        return PhysicalNodeIdentityResult(
            id=node.id,
            public_key=node.public_key,
            private_key_pem=node.private_key_encrypted,
            key_algorithm=node.key_algorithm,
            status=node.status,
            created_at=node.created_at,
            updated_at=node.updated_at,
        )

    def create_local_virtual_node(
        self,
        data: VirtualNodeIdentityCreateInput,
    ) -> LocalVirtualNodeIdentity:
        key_pair = generate_dilithium_key_pair()
        public_key = key_pair.public_key_pem
        private_key_encrypted = self._protect_private_key(key_pair.private_key_pem)
        virtual_node_id = self._build_node_id(public_key)

        with self.database.session_scope() as session:
            virtual_node = LocalVirtualNodeIdentity(
                id=virtual_node_id,
                public_key=public_key,
                private_key_encrypted=private_key_encrypted,
                kind=data.kind,
                owner_physical_node_id=data.owner_physical_node_id,
                expires_at=data.expires_at,
                is_active=data.is_active,
                metadata_json=data.metadata_json,
            )
            session.add(virtual_node)
            session.flush()
            session.refresh(virtual_node)
            return virtual_node

    def get_local_virtual_node_by_id(self, virtual_node_id: str) -> LocalVirtualNodeIdentity | None:
        with self.database.session_scope() as session:
            return session.get(LocalVirtualNodeIdentity, virtual_node_id)

    def list_local_virtual_nodes(
        self,
        *,
        only_active: bool = False,
    ) -> list[LocalVirtualNodeIdentity]:
        with self.database.session_scope() as session:
            query = select(LocalVirtualNodeIdentity).order_by(LocalVirtualNodeIdentity.created_at)
            if only_active:
                query = query.where(LocalVirtualNodeIdentity.is_active.is_(True))
            return list(session.scalars(query).all())

    def get_remote_physical_node_by_id(
        self,
        node_id: str,
    ) -> RemotePhysicalNodeIdentity | None:
        with self.database.session_scope() as session:
            return session.get(RemotePhysicalNodeIdentity, node_id)

    def get_remote_virtual_node_by_id(
        self,
        node_id: str,
    ) -> RemoteVirtualNodeIdentity | None:
        with self.database.session_scope() as session:
            return session.get(RemoteVirtualNodeIdentity, node_id)

    def upsert_remote_virtual_node(
        self,
        *,
        node_id: str,
        public_key: str,
        kind: str = "default",
        status: str = "active",
        expires_at: datetime | None = None,
        metadata_json: str | None = None,
    ) -> RemoteVirtualNodeIdentity:
        now = utc_now()

        with self.database.session_scope() as session:
            remote_node = session.get(RemoteVirtualNodeIdentity, node_id)
            if remote_node is None:
                remote_node = RemoteVirtualNodeIdentity(
                    id=node_id,
                    public_key=public_key,
                    kind=kind,
                    status=status,
                    first_seen_at=now,
                    last_seen_at=now,
                    expires_at=expires_at,
                    metadata_json=metadata_json,
                )
                session.add(remote_node)
            else:
                remote_node.public_key = public_key
                remote_node.kind = kind
                remote_node.status = status
                remote_node.last_seen_at = now
                remote_node.expires_at = expires_at
                remote_node.metadata_json = _merge_notes_json(
                    remote_node.metadata_json,
                    metadata_json,
                )

            session.flush()
            session.refresh(remote_node)
            return remote_node

    def list_remote_physical_nodes_for_validation(
        self,
        *,
        limit: int = 1,
        failed_before: datetime | None = None,
    ) -> list[RemotePhysicalNodeValidationCandidate]:
        with self.database.session_scope() as session:
            query = (
                select(RemotePhysicalNodeIdentity)
                .join(
                    NodeEndpoint,
                    NodeEndpoint.physical_node_hash_id == RemotePhysicalNodeIdentity.id,
                )
                .where(RemotePhysicalNodeIdentity.last_validated_at.is_(None))
                .where(RemotePhysicalNodeIdentity.status != "active")
                .where(NodeEndpoint.transport.is_not(None))
                .where(NodeEndpoint.host.is_not(None))
                .where(NodeEndpoint.port.is_not(None))
            )

            if failed_before is not None:
                query = query.where(
                    or_(
                        NodeEndpoint.last_failure_at.is_(None),
                        NodeEndpoint.last_failure_at <= failed_before,
                    )
                )

            query = query.distinct().order_by(func.random()).limit(limit)
            remote_nodes = list(session.scalars(query).all())

        candidates: list[RemotePhysicalNodeValidationCandidate] = []
        for remote_node in remote_nodes:
            candidates.append(
                RemotePhysicalNodeValidationCandidate(
                    node_id=remote_node.id,
                    public_key=remote_node.public_key,
                )
            )
        return candidates

    def list_remote_physical_node_endpoints(
        self,
        node_id: str,
    ) -> list[RemotePhysicalNodeEndpointResult]:
        with self.database.session_scope() as session:
            query = (
                select(NodeEndpoint)
                .where(NodeEndpoint.physical_node_hash_id == node_id)
                .order_by(
                    NodeEndpoint.priority.asc(),
                    NodeEndpoint.last_success_at.desc(),
                    NodeEndpoint.id.asc(),
                )
            )
            endpoints = list(session.scalars(query).all())

        return [
            RemotePhysicalNodeEndpointResult(
                transport=endpoint.transport,
                host=endpoint.host,
                port=endpoint.port,
                priority=endpoint.priority,
            )
            for endpoint in endpoints
        ]

    def list_remote_physical_nodes_for_info_exchange(
        self,
        *,
        limit: int = 10,
    ) -> list[RemotePhysicalNodeExchangeCandidate]:
        with self.database.session_scope() as session:
            query = (
                select(RemotePhysicalNodeIdentity)
                .where(RemotePhysicalNodeIdentity.status == "active")
                .where(RemotePhysicalNodeIdentity.last_validated_at.is_not(None))
                .order_by(func.random())
                .limit(limit)
            )
            remote_nodes = list(session.scalars(query).all())

        return [
            RemotePhysicalNodeExchangeCandidate(node_id=remote_node.id)
            for remote_node in remote_nodes
        ]

    def list_remote_physical_nodes_for_ping(
        self,
        *,
        limit: int = 10,
    ) -> list[RemotePhysicalNodePingCandidate]:
        with self.database.session_scope() as session:
            query = (
                select(RemotePhysicalNodeIdentity)
                .join(
                    NodeEndpoint,
                    NodeEndpoint.physical_node_hash_id == RemotePhysicalNodeIdentity.id,
                )
                .where(RemotePhysicalNodeIdentity.status == "active")
                .where(RemotePhysicalNodeIdentity.last_validated_at.is_not(None))
                .where(NodeEndpoint.transport.is_not(None))
                .where(NodeEndpoint.host.is_not(None))
                .where(NodeEndpoint.port.is_not(None))
                .distinct()
                .order_by(func.random())
                .limit(limit)
            )
            remote_nodes = list(session.scalars(query).all())

        return [RemotePhysicalNodePingCandidate(node_id=remote_node.id) for remote_node in remote_nodes]

    def list_remote_physical_nodes_for_random_walk_ttl(
        self,
        *,
        limit: int = 32,
    ) -> list[RemotePhysicalNodeRouteCandidate]:
        with self.database.session_scope() as session:
            query = (
                select(RemotePhysicalNodeIdentity, RttInfo)
                .join(RttInfo, RttInfo.remote_physical_node_id == RemotePhysicalNodeIdentity.id)
                .where(RemotePhysicalNodeIdentity.status == "active")
                .where(RemotePhysicalNodeIdentity.last_validated_at.is_not(None))
                .order_by(func.random())
                .limit(limit)
            )
            rows = session.execute(query).all()

        return [
            RemotePhysicalNodeRouteCandidate(
                node_id=remote_node.id,
                public_key=remote_node.public_key,
                average_rtt_ms=float(rtt_info.average_rtt_ms),
            )
            for remote_node, rtt_info in rows
        ]

    def upsert_remote_physical_node(
        self,
        *,
        node_id: str,
        public_key: str,
        protocol_version: str | None,
        status: str,
        endpoints: list[dict[str, object]],
        mark_validated: bool = False,
        display_name: str | None = None,
        reachability_class: str | None = None,
        relay_capable: bool = False,
        hole_punch_capable: bool = False,
        notes_json: str | None = None,
    ) -> RemotePhysicalNodeIdentity:
        now = utc_now()

        with self.database.session_scope() as session:
            remote_node = session.get(RemotePhysicalNodeIdentity, node_id)
            if remote_node is None:
                remote_node = RemotePhysicalNodeIdentity(
                    id=node_id,
                    public_key=public_key,
                    display_name=display_name,
                    reachability_class=reachability_class,
                    relay_capable=relay_capable,
                    hole_punch_capable=hole_punch_capable,
                    protocol_version=protocol_version,
                    status=status,
                    last_seen_at=now,
                    last_validated_at=now if mark_validated else None,
                    notes_json=notes_json,
                )
                session.add(remote_node)
            else:
                remote_node.public_key = public_key
                remote_node.display_name = display_name
                remote_node.reachability_class = reachability_class
                remote_node.relay_capable = relay_capable
                remote_node.hole_punch_capable = hole_punch_capable
                remote_node.protocol_version = protocol_version
                remote_node.last_seen_at = now
                remote_node.notes_json = _merge_notes_json(remote_node.notes_json, notes_json)
                if mark_validated:
                    remote_node.status = status
                    remote_node.last_validated_at = now
                elif remote_node.status != "active":
                    remote_node.status = status

            for endpoint_data in endpoints:
                transport = endpoint_data["transport"]
                host = endpoint_data["host"]
                port = endpoint_data["port"]
                priority = endpoint_data.get("priority", 0)

                endpoint = session.scalar(
                    select(NodeEndpoint).where(
                        NodeEndpoint.physical_node_hash_id == node_id,
                        NodeEndpoint.transport == transport,
                        NodeEndpoint.host == host,
                        NodeEndpoint.port == port,
                    )
                )
                if endpoint is None:
                    endpoint = NodeEndpoint(
                        physical_node_hash_id=node_id,
                        transport=transport,
                        host=host,
                        port=port,
                        priority=priority if isinstance(priority, int) else 0,
                        is_active=True,
                        last_success_at=now,
                        metadata_json=None,
                    )
                    session.add(endpoint)
                else:
                    endpoint.priority = priority if isinstance(priority, int) else endpoint.priority
                    endpoint.is_active = True
                    endpoint.last_success_at = now

            session.flush()
            session.refresh(remote_node)
            return remote_node

    def upsert_discovered_remote_physical_node(
        self,
        *,
        node_id: str,
        public_key: str,
        protocol_version: str | None,
        endpoints: list[dict[str, object]],
        status: str = "discovered",
        display_name: str | None = None,
        reachability_class: str | None = None,
        relay_capable: bool = False,
        hole_punch_capable: bool = False,
        notes_json: str | None = None,
    ) -> RemotePhysicalNodeIdentity:
        now = utc_now()

        with self.database.session_scope() as session:
            remote_node = session.get(RemotePhysicalNodeIdentity, node_id)
            if remote_node is None:
                remote_node = RemotePhysicalNodeIdentity(
                    id=node_id,
                    public_key=public_key,
                    display_name=display_name,
                    reachability_class=reachability_class,
                    relay_capable=relay_capable,
                    hole_punch_capable=hole_punch_capable,
                    protocol_version=protocol_version,
                    status=status,
                    last_seen_at=now,
                    last_validated_at=None,
                    notes_json=notes_json,
                )
                session.add(remote_node)
            else:
                remote_node.public_key = public_key
                remote_node.display_name = display_name
                remote_node.reachability_class = reachability_class
                remote_node.relay_capable = relay_capable
                remote_node.hole_punch_capable = hole_punch_capable
                remote_node.protocol_version = protocol_version
                remote_node.last_seen_at = now
                remote_node.notes_json = _merge_notes_json(remote_node.notes_json, notes_json)
                if remote_node.status != "active":
                    remote_node.status = status

            for endpoint_data in endpoints:
                transport = endpoint_data["transport"]
                host = endpoint_data["host"]
                port = endpoint_data["port"]
                priority = endpoint_data.get("priority", 0)

                endpoint = session.scalar(
                    select(NodeEndpoint).where(
                        NodeEndpoint.physical_node_hash_id == node_id,
                        NodeEndpoint.transport == transport,
                        NodeEndpoint.host == host,
                        NodeEndpoint.port == port,
                    )
                )
                if endpoint is None:
                    endpoint = NodeEndpoint(
                        physical_node_hash_id=node_id,
                        transport=transport,
                        host=host,
                        port=port,
                        priority=priority if isinstance(priority, int) else 0,
                        is_active=False,
                        last_success_at=None,
                        metadata_json=None,
                    )
                    session.add(endpoint)
                else:
                    endpoint.priority = priority if isinstance(priority, int) else endpoint.priority
                    endpoint.is_active = False

            session.flush()
            session.refresh(remote_node)
            return remote_node

    def mark_remote_physical_node_validated(
        self,
        *,
        node_id: str,
        transport: str,
        host: str,
        port: int,
    ) -> RemotePhysicalNodeIdentity | None:
        now = utc_now()

        with self.database.session_scope() as session:
            remote_node = session.get(RemotePhysicalNodeIdentity, node_id)
            if remote_node is None:
                return None

            remote_node.status = "active"
            remote_node.last_seen_at = now
            remote_node.last_validated_at = now

            endpoint = session.scalar(
                select(NodeEndpoint).where(
                    NodeEndpoint.physical_node_hash_id == node_id,
                    NodeEndpoint.transport == transport,
                    NodeEndpoint.host == host,
                    NodeEndpoint.port == port,
                )
            )
            if endpoint is not None:
                endpoint.is_active = True
                endpoint.last_success_at = now

            session.flush()
            session.refresh(remote_node)
            return remote_node

    def mark_remote_physical_node_validation_failure(
        self,
        *,
        node_id: str,
        transport: str,
        host: str,
        port: int,
    ) -> RemotePhysicalNodeIdentity | None:
        now = utc_now()

        with self.database.session_scope() as session:
            remote_node = session.get(RemotePhysicalNodeIdentity, node_id)
            if remote_node is None:
                return None

            endpoint = session.scalar(
                select(NodeEndpoint).where(
                    NodeEndpoint.physical_node_hash_id == node_id,
                    NodeEndpoint.transport == transport,
                    NodeEndpoint.host == host,
                    NodeEndpoint.port == port,
                )
            )
            if endpoint is not None:
                endpoint.is_active = False
                endpoint.last_failure_at = now
                endpoint.failure_count += 1

            session.flush()
            session.refresh(remote_node)
            return remote_node

    def was_physical_node_info_exchange_requested_after(
        self,
        *,
        remote_physical_node_id: str,
        threshold: datetime,
    ) -> bool:
        with self.database.session_scope() as session:
            state = session.scalar(
                select(PhysicalNodeInfoExchangeState).where(
                    PhysicalNodeInfoExchangeState.remote_physical_node_id == remote_physical_node_id
                )
            )
            if state is None or state.last_request_sent_at is None:
                return False

            return state.last_request_sent_at >= threshold

    def mark_physical_node_info_exchange_request_sent(
        self,
        *,
        remote_physical_node_id: str,
    ) -> None:
        now = utc_now()

        with self.database.session_scope() as session:
            state = self._get_or_create_exchange_state(session, remote_physical_node_id)
            state.last_request_sent_at = now
            state.last_exchange_at = now
            session.flush()

    def mark_physical_node_info_exchange_response_received(
        self,
        *,
        remote_physical_node_id: str,
    ) -> None:
        now = utc_now()

        with self.database.session_scope() as session:
            state = self._get_or_create_exchange_state(session, remote_physical_node_id)
            state.last_response_received_at = now
            state.last_exchange_at = now
            session.flush()

    def mark_physical_node_info_exchange_announce_received(
        self,
        *,
        remote_physical_node_id: str,
    ) -> None:
        now = utc_now()

        with self.database.session_scope() as session:
            state = self._get_or_create_exchange_state(session, remote_physical_node_id)
            state.last_announce_received_at = now
            state.last_exchange_at = now
            session.flush()

    def upsert_rtt_info(
        self,
        *,
        remote_physical_node_id: str,
        observed_rtt_ms: float,
    ) -> RttInfo | None:
        if observed_rtt_ms < 0:
            raise ValueError("O RTT observado nao pode ser negativo.")

        now = utc_now()

        with self.database.session_scope() as session:
            remote_node = session.get(RemotePhysicalNodeIdentity, remote_physical_node_id)
            if remote_node is None:
                return None

            rtt_info = session.scalar(
                select(RttInfo).where(RttInfo.remote_physical_node_id == remote_physical_node_id)
            )
            if rtt_info is None:
                rtt_info = RttInfo(
                    remote_physical_node_id=remote_physical_node_id,
                    min_rtt_ms=observed_rtt_ms,
                    max_rtt_ms=observed_rtt_ms,
                    average_rtt_ms=observed_rtt_ms,
                    observed_count=1,
                    last_observed_at=now,
                    metadata_json=None,
                )
                session.add(rtt_info)
            else:
                current_total = rtt_info.average_rtt_ms * rtt_info.observed_count
                new_count = rtt_info.observed_count + 1
                rtt_info.min_rtt_ms = min(rtt_info.min_rtt_ms, observed_rtt_ms)
                rtt_info.max_rtt_ms = max(rtt_info.max_rtt_ms, observed_rtt_ms)
                rtt_info.average_rtt_ms = (current_total + observed_rtt_ms) / new_count
                rtt_info.observed_count = new_count
                rtt_info.last_observed_at = now

            session.flush()
            session.refresh(rtt_info)
            return rtt_info

    def build_dpnt_publish_request_for_remote_physical_node(
        self,
        *,
        node_id: str,
    ) -> dict[str, str] | None:
        remote_node = self.get_remote_physical_node_by_id(node_id)
        if remote_node is None:
            return None

        endpoints = self.list_remote_physical_node_endpoints(node_id)
        if not endpoints:
            return None

        notes = _load_json_object(remote_node.notes_json)
        dpnt_signature = notes.get("dpnt_signature")
        dpnt_feature_flags = notes.get("dpnt_feature_flags", [])
        if (
            not isinstance(dpnt_signature, str)
            or not dpnt_signature
            or remote_node.last_validated_at is None
        ):
            return None

        feature_flags = (
            [item for item in dpnt_feature_flags if isinstance(item, str)]
            if isinstance(dpnt_feature_flags, list)
            else []
        )
        local_last_validated_at = remote_node.last_validated_at.isoformat()
        payload = DpntRecordPayload(
            pk_physical_node=remote_node.public_key,
            endpoints=[
                {
                    "transport": endpoint.transport,
                    "host": endpoint.host,
                    "port": endpoint.port,
                    "priority": endpoint.priority,
                }
                for endpoint in endpoints
            ],
            transport_methods=sorted({endpoint.transport for endpoint in endpoints}),
            reachability_class=remote_node.reachability_class or "unknown",
            relay_capable=remote_node.relay_capable,
            hole_punch_capable=remote_node.hole_punch_capable,
            protocol_version=remote_node.protocol_version or "1",
            feature_flags=feature_flags,
            last_validated_at=local_last_validated_at,
            status=remote_node.status,
            signature=dpnt_signature,
        )
        return {
            "namespace": "dpnt",
            "logical_key": remote_node.id,
            "record_json": serialize_record(payload),
            "expires_at": None,
        }

    @staticmethod
    def _build_node_id(public_key: str) -> str:
        return sha512_hex(public_key.encode("utf-8"))

    @staticmethod
    def _protect_private_key(private_key_pem: str) -> str:
        return private_key_pem

    @staticmethod
    def _get_or_create_exchange_state(
        session,
        remote_physical_node_id: str,
    ) -> PhysicalNodeInfoExchangeState:
        state = session.scalar(
            select(PhysicalNodeInfoExchangeState).where(
                PhysicalNodeInfoExchangeState.remote_physical_node_id == remote_physical_node_id
            )
        )
        if state is not None:
            return state

        state = PhysicalNodeInfoExchangeState(
            remote_physical_node_id=remote_physical_node_id,
            last_exchange_at=None,
            last_request_sent_at=None,
            last_response_received_at=None,
            last_announce_received_at=None,
            metadata_json=None,
        )
        session.add(state)
        session.flush()
        return state


def _load_json_object(raw_json: str | None) -> dict[str, object]:
    if not raw_json:
        return {}

    try:
        payload = json.loads(raw_json)
    except json.JSONDecodeError:
        return {}

    return payload if isinstance(payload, dict) else {}
