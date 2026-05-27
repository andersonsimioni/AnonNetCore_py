from __future__ import annotations

import json
from datetime import datetime, timezone

from sqlalchemy import func, or_, select

from common import compact_json_text, load_json_object, utc_now
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


def _merge_notes_json(
    existing_notes_json: str | None,
    new_notes_json: str | None,
) -> str | None:
    existing_notes = load_json_object(existing_notes_json)
    new_notes = load_json_object(new_notes_json)
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


def _normalize_endpoint_metadata_json(endpoint_data: dict[str, object]) -> str | None:
    metadata = endpoint_data.get("metadata")
    if isinstance(metadata, dict) and metadata:
        return compact_json_text(metadata)

    metadata_json = endpoint_data.get("metadata_json")
    if isinstance(metadata_json, str) and metadata_json:
        parsed = load_json_object(metadata_json)
        if parsed:
            return compact_json_text(parsed)

    return None


class IdentityService:
    """Gerencia identidades locais do node fisico e dos nodes virtuais."""

    def __init__(self, database: DatabaseManager | None = None) -> None:
        self.database = database or get_database()
        self.endpoint_failure_threshold = 3

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

    @staticmethod
    def _get_local_physical_node_id(session) -> str | None:
        return session.scalar(select(LocalPhysicalNodeIdentity.id).order_by(LocalPhysicalNodeIdentity.created_at))

    @classmethod
    def _exclude_local_physical_node(cls, session, query):
        local_node_id = cls._get_local_physical_node_id(session)
        if local_node_id is None:
            return query
        return query.where(RemotePhysicalNodeIdentity.id != local_node_id)

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

    def list_remote_virtual_nodes(
        self,
        *,
        status: str | None = None,
    ) -> list[RemoteVirtualNodeIdentity]:
        with self.database.session_scope() as session:
            query = select(RemoteVirtualNodeIdentity).order_by(
                RemoteVirtualNodeIdentity.last_seen_at.desc(),
                RemoteVirtualNodeIdentity.id.asc(),
            )
            if status:
                query = query.where(RemoteVirtualNodeIdentity.status == status)
            return list(session.scalars(query).all())

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

            query = self._exclude_local_physical_node(session, query)
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
        *,
        only_active: bool = False,
    ) -> list[RemotePhysicalNodeEndpointResult]:
        with self.database.session_scope() as session:
            query = (
                select(NodeEndpoint)
                .where(NodeEndpoint.physical_node_hash_id == node_id)
            )
            if only_active:
                query = query.where(NodeEndpoint.is_active.is_(True))

            query = query.order_by(
                NodeEndpoint.is_active.desc(),
                NodeEndpoint.priority.asc(),
                NodeEndpoint.failure_count.asc(),
                NodeEndpoint.last_success_at.desc(),
                NodeEndpoint.id.asc(),
            )
            endpoints = list(session.scalars(query).all())

        return [
            RemotePhysicalNodeEndpointResult(
                transport=endpoint.transport,
                host=endpoint.host,
                port=endpoint.port,
                priority=endpoint.priority,
                metadata_json=endpoint.metadata_json,
            )
            for endpoint in endpoints
        ]

    def find_remote_physical_node_id_by_endpoint(
        self,
        *,
        transport: str,
        host: str,
        port: int,
    ) -> str | None:
        with self.database.session_scope() as session:
            endpoint = session.scalar(
                select(NodeEndpoint)
                .where(NodeEndpoint.transport == transport)
                .where(NodeEndpoint.host == host)
                .where(NodeEndpoint.port == port)
                .where(NodeEndpoint.is_active.is_(True))
                .order_by(NodeEndpoint.last_success_at.desc(), NodeEndpoint.id.asc())
            )
            return endpoint.physical_node_hash_id if endpoint is not None else None

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
            query = self._exclude_local_physical_node(session, query)
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
            query = self._exclude_local_physical_node(session, query)
            remote_nodes = list(session.scalars(query).all())

        return [RemotePhysicalNodePingCandidate(node_id=remote_node.id) for remote_node in remote_nodes]

    def list_remote_physical_nodes_for_random_walk_ttl(
        self,
        *,
        limit: int = 32,
    ) -> list[RemotePhysicalNodeRouteCandidate]:
        fallback_rtt_ms = 40.0
        with self.database.session_scope() as session:
            query = (
                select(RemotePhysicalNodeIdentity, RttInfo)
                .outerjoin(RttInfo, RttInfo.remote_physical_node_id == RemotePhysicalNodeIdentity.id)
                .join(
                    NodeEndpoint,
                    NodeEndpoint.physical_node_hash_id == RemotePhysicalNodeIdentity.id,
                )
                .where(RemotePhysicalNodeIdentity.status == "active")
                .where(RemotePhysicalNodeIdentity.last_validated_at.is_not(None))
                .where(NodeEndpoint.is_active.is_(True))
                .where(NodeEndpoint.transport.is_not(None))
                .where(NodeEndpoint.host.is_not(None))
                .where(NodeEndpoint.port.is_not(None))
                .distinct()
                .order_by(func.random())
                .limit(limit)
            )
            query = self._exclude_local_physical_node(session, query)
            rows = session.execute(query).all()

        return [
            RemotePhysicalNodeRouteCandidate(
                node_id=remote_node.id,
                public_key=remote_node.public_key,
                average_rtt_ms=float(rtt_info.average_rtt_ms if rtt_info is not None else fallback_rtt_ms),
            )
            for remote_node, rtt_info in rows
        ]

    def build_remote_physical_node_route_diagnostics(self) -> dict[str, object]:
        """Resume os filtros que um PN precisa passar para virar candidato de rota."""

        with self.database.session_scope() as session:
            total_remote_nodes_query = select(func.count()).select_from(RemotePhysicalNodeIdentity)
            discovered_nodes_query = (
                select(func.count())
                .select_from(RemotePhysicalNodeIdentity)
                .where(RemotePhysicalNodeIdentity.status != "active")
            )
            active_nodes_query = (
                select(func.count())
                .select_from(RemotePhysicalNodeIdentity)
                .where(RemotePhysicalNodeIdentity.status == "active")
            )
            validated_nodes_query = (
                select(func.count())
                .select_from(RemotePhysicalNodeIdentity)
                .where(RemotePhysicalNodeIdentity.status == "active")
                .where(RemotePhysicalNodeIdentity.last_validated_at.is_not(None))
            )
            active_endpoint_nodes_query = (
                select(func.count(func.distinct(RemotePhysicalNodeIdentity.id)))
                .select_from(RemotePhysicalNodeIdentity)
                .join(
                    NodeEndpoint,
                    NodeEndpoint.physical_node_hash_id == RemotePhysicalNodeIdentity.id,
                )
                .where(RemotePhysicalNodeIdentity.status == "active")
                .where(RemotePhysicalNodeIdentity.last_validated_at.is_not(None))
                .where(NodeEndpoint.is_active.is_(True))
                .where(NodeEndpoint.transport.is_not(None))
                .where(NodeEndpoint.host.is_not(None))
                .where(NodeEndpoint.port.is_not(None))
            )
            route_ready_nodes_query = (
                select(func.count(func.distinct(RemotePhysicalNodeIdentity.id)))
                .select_from(RemotePhysicalNodeIdentity)
                .join(
                    NodeEndpoint,
                    NodeEndpoint.physical_node_hash_id == RemotePhysicalNodeIdentity.id,
                )
                .where(RemotePhysicalNodeIdentity.status == "active")
                .where(RemotePhysicalNodeIdentity.last_validated_at.is_not(None))
                .where(NodeEndpoint.is_active.is_(True))
                .where(NodeEndpoint.transport.is_not(None))
                .where(NodeEndpoint.host.is_not(None))
                .where(NodeEndpoint.port.is_not(None))
            )
            rtt_known_nodes_query = (
                select(func.count(func.distinct(RemotePhysicalNodeIdentity.id)))
                .select_from(RemotePhysicalNodeIdentity)
                .join(RttInfo, RttInfo.remote_physical_node_id == RemotePhysicalNodeIdentity.id)
                .where(RemotePhysicalNodeIdentity.status == "active")
                .where(RemotePhysicalNodeIdentity.last_validated_at.is_not(None))
            )
            endpoint_rows_query = select(
                NodeEndpoint.physical_node_hash_id,
                NodeEndpoint.transport,
                NodeEndpoint.host,
                NodeEndpoint.port,
                NodeEndpoint.is_active,
                NodeEndpoint.failure_count,
            )

            total_remote_nodes = session.scalar(
                self._exclude_local_physical_node(session, total_remote_nodes_query)
            ) or 0
            discovered_nodes = session.scalar(
                self._exclude_local_physical_node(session, discovered_nodes_query)
            ) or 0
            active_nodes = session.scalar(
                self._exclude_local_physical_node(session, active_nodes_query)
            ) or 0
            validated_nodes = session.scalar(
                self._exclude_local_physical_node(session, validated_nodes_query)
            ) or 0
            active_endpoint_nodes = session.scalar(
                self._exclude_local_physical_node(session, active_endpoint_nodes_query)
            ) or 0
            route_ready_nodes = session.scalar(
                self._exclude_local_physical_node(session, route_ready_nodes_query)
            ) or 0
            rtt_known_nodes = session.scalar(
                self._exclude_local_physical_node(session, rtt_known_nodes_query)
            ) or 0
            local_node_id = self._get_local_physical_node_id(session)
            if local_node_id is not None:
                endpoint_rows_query = endpoint_rows_query.where(
                    NodeEndpoint.physical_node_hash_id != local_node_id
                )
            endpoint_rows_query = endpoint_rows_query.order_by(
                NodeEndpoint.last_success_at.desc(),
                NodeEndpoint.id.desc(),
            ).limit(8)
            endpoint_rows = list(session.execute(endpoint_rows_query).all())

        return {
            "total_remote_nodes": int(total_remote_nodes),
            "discovered_nodes": int(discovered_nodes),
            "active_nodes": int(active_nodes),
            "validated_nodes": int(validated_nodes),
            "active_endpoint_nodes": int(active_endpoint_nodes),
            "route_ready_nodes": int(route_ready_nodes),
            "rtt_known_nodes": int(rtt_known_nodes),
            "recent_endpoints": [
                {
                    "node_id": node_id,
                    "transport": transport,
                    "host": host,
                    "port": port,
                    "is_active": is_active,
                    "failure_count": failure_count,
                }
                for node_id, transport, host, port, is_active, failure_count in endpoint_rows
            ],
        }

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
                remote_node.display_name = display_name or remote_node.display_name
                remote_node.reachability_class = reachability_class or remote_node.reachability_class
                remote_node.relay_capable = relay_capable
                remote_node.hole_punch_capable = hole_punch_capable
                remote_node.protocol_version = protocol_version or remote_node.protocol_version
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
                metadata_json = _normalize_endpoint_metadata_json(endpoint_data)

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
                        metadata_json=metadata_json,
                    )
                    session.add(endpoint)
                else:
                    endpoint.priority = priority if isinstance(priority, int) else endpoint.priority
                    endpoint.is_active = True
                    endpoint.last_success_at = now
                    endpoint.failure_count = 0
                    endpoint.metadata_json = metadata_json or endpoint.metadata_json

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
        local_status = "discovered" if status == "active" else status

        with self.database.session_scope() as session:
            remote_node = session.get(RemotePhysicalNodeIdentity, node_id)
            node_was_validated = False
            if remote_node is None:
                remote_node = RemotePhysicalNodeIdentity(
                    id=node_id,
                    public_key=public_key,
                    display_name=display_name,
                    reachability_class=reachability_class,
                    relay_capable=relay_capable,
                    hole_punch_capable=hole_punch_capable,
                    protocol_version=protocol_version,
                    status=local_status,
                    last_seen_at=now,
                    last_validated_at=None,
                    notes_json=notes_json,
                )
                session.add(remote_node)
            else:
                node_was_validated = remote_node.status == "active" and remote_node.last_validated_at is not None
                remote_node.public_key = public_key
                remote_node.display_name = display_name or remote_node.display_name
                remote_node.reachability_class = reachability_class or remote_node.reachability_class
                remote_node.relay_capable = relay_capable
                remote_node.hole_punch_capable = hole_punch_capable
                remote_node.protocol_version = protocol_version or remote_node.protocol_version
                remote_node.last_seen_at = now
                remote_node.notes_json = _merge_notes_json(remote_node.notes_json, notes_json)
                if remote_node.status != "active":
                    remote_node.status = local_status

            for endpoint_data in endpoints:
                transport = endpoint_data["transport"]
                host = endpoint_data["host"]
                port = endpoint_data["port"]
                priority = endpoint_data.get("priority", 0)
                metadata_json = _normalize_endpoint_metadata_json(endpoint_data)

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
                        metadata_json=metadata_json,
                    )
                    session.add(endpoint)
                else:
                    endpoint.priority = priority if isinstance(priority, int) else endpoint.priority
                    endpoint.metadata_json = metadata_json or endpoint.metadata_json
                    if not node_was_validated:
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
                endpoint.failure_count = 0

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
                endpoint.last_failure_at = now
                endpoint.failure_count += 1
                endpoint.is_active = endpoint.failure_count < self.endpoint_failure_threshold

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

            last_request_sent_at = state.last_request_sent_at
            if last_request_sent_at.tzinfo is None:
                last_request_sent_at = last_request_sent_at.replace(tzinfo=timezone.utc)
            if threshold.tzinfo is None:
                threshold = threshold.replace(tzinfo=timezone.utc)

            return last_request_sent_at >= threshold

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

        endpoints = self.list_remote_physical_node_endpoints(node_id, only_active=True)
        if not endpoints:
            return None

        notes = load_json_object(remote_node.notes_json)
        dpnt_signature = notes.get("dpnt_signature")
        dpnt_feature_flags = notes.get("dpnt_feature_flags", [])
        dpnt_reachability_class = notes.get("dpnt_reachability_class")
        dpnt_relay_capable = notes.get("dpnt_relay_capable")
        dpnt_hole_punch_capable = notes.get("dpnt_hole_punch_capable")
        dpnt_protocol_version = notes.get("dpnt_protocol_version")
        dpnt_status = notes.get("dpnt_status")
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
                    "metadata": load_json_object(endpoint.metadata_json),
                }
                for endpoint in endpoints
            ],
            transport_methods=sorted({endpoint.transport for endpoint in endpoints}),
            reachability_class=(
                dpnt_reachability_class
                if isinstance(dpnt_reachability_class, str) and dpnt_reachability_class
                else remote_node.reachability_class or "unknown"
            ),
            relay_capable=(
                dpnt_relay_capable
                if isinstance(dpnt_relay_capable, bool)
                else remote_node.relay_capable
            ),
            hole_punch_capable=(
                dpnt_hole_punch_capable
                if isinstance(dpnt_hole_punch_capable, bool)
                else remote_node.hole_punch_capable
            ),
            protocol_version=(
                dpnt_protocol_version
                if isinstance(dpnt_protocol_version, str) and dpnt_protocol_version
                else remote_node.protocol_version or "1"
            ),
            feature_flags=feature_flags,
            last_validated_at=local_last_validated_at,
            status=(
                dpnt_status
                if isinstance(dpnt_status, str) and dpnt_status
                else remote_node.status
            ),
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
