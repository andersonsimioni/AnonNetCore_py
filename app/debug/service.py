from __future__ import annotations

from datetime import datetime, timezone
import json
from typing import Any

from sqlalchemy import func, select

from core.components import EngineBoundComponent
from storage.models import (
    ContentAdvertisement,
    ContentObject,
    DhtRecord,
    LocalVirtualNodeIdentity,
    NodeEndpoint,
    RemotePhysicalNodeIdentity,
    RemoteVirtualNodeIdentity,
    RouteResolution,
    RttInfo,
)


class DebugSnapshotService(EngineBoundComponent):
    """Monta uma visao de leitura sobre o estado local do core."""

    def build_state(self) -> dict[str, object]:
        local_node = self.engine.services.identity_service.get_local_physical_node_result()
        return {
            "captured_at": datetime.now(timezone.utc).isoformat(),
            "node": {
                "name": self.engine.get_runtime_node_name(),
                "physical_node_id": local_node.id if local_node else None,
                "listen_host": self.engine.services.config.physical_listen_host,
                "listen_port": self.engine.services.config.physical_tcp_listen_port,
                "advertised_tcp_host": self.engine.get_advertised_tcp_host(),
                "advertised_tcp_port": self.engine.get_advertised_tcp_port(),
            },
            "bootstrap": self._build_bootstrap_state(),
            "peers": self._build_peer_state(),
            "sessions": self._build_session_state(),
            "virtual_nodes": self._build_virtual_node_state(),
            "routes": self._build_route_state(),
            "dht": self._build_dht_state(),
            "content": self._build_content_state(),
            "runtimes": self._build_runtime_state(),
            "problems": self._build_problem_list(),
        }

    def _build_bootstrap_state(self) -> dict[str, object]:
        result = self.engine.bootstrap_result
        endpoints = []
        if result is not None:
            endpoints = [
                {
                    "transport": endpoint.transport,
                    "host": endpoint.host,
                    "port": endpoint.port,
                    "source": endpoint.source,
                }
                for endpoint in result.all_endpoints
            ]
        return {"endpoint_count": len(endpoints), "endpoints": endpoints}

    def _build_peer_state(self) -> dict[str, object]:
        route_diagnostics = (
            self.engine.services.identity_service.build_remote_physical_node_route_diagnostics()
        )
        with self.engine.services.database.session_scope() as session:
            rows = session.execute(
                select(RemotePhysicalNodeIdentity, RttInfo)
                .outerjoin(RttInfo, RttInfo.remote_physical_node_id == RemotePhysicalNodeIdentity.id)
                .order_by(
                    RemotePhysicalNodeIdentity.status.asc(),
                    RemotePhysicalNodeIdentity.last_seen_at.desc(),
                    RemotePhysicalNodeIdentity.id.asc(),
                )
                .limit(200)
            ).all()
            endpoint_rows = session.execute(
                select(NodeEndpoint)
                .order_by(
                    NodeEndpoint.physical_node_hash_id.asc(),
                    NodeEndpoint.is_active.desc(),
                    NodeEndpoint.priority.asc(),
                    NodeEndpoint.failure_count.asc(),
                )
                .limit(500)
            ).scalars().all()

        endpoints_by_node: dict[str, list[dict[str, object]]] = {}
        for endpoint in endpoint_rows:
            endpoints_by_node.setdefault(endpoint.physical_node_hash_id, []).append(
                {
                    "transport": endpoint.transport,
                    "host": endpoint.host,
                    "port": endpoint.port,
                    "priority": endpoint.priority,
                    "is_active": endpoint.is_active,
                    "failure_count": endpoint.failure_count,
                    "last_success_at": _iso(endpoint.last_success_at),
                    "last_failure_at": _iso(endpoint.last_failure_at),
                }
            )

        peers = [
            {
                "node_id": peer.id,
                "status": peer.status,
                "last_seen_at": _iso(peer.last_seen_at),
                "last_validated_at": _iso(peer.last_validated_at),
                "endpoint_count": len(endpoints_by_node.get(peer.id, [])),
                "active_endpoint_count": len(
                    [endpoint for endpoint in endpoints_by_node.get(peer.id, []) if endpoint["is_active"]]
                ),
                "endpoints": endpoints_by_node.get(peer.id, []),
                "rtt": None
                if rtt is None
                else {
                    "min_ms": rtt.min_rtt_ms,
                    "avg_ms": rtt.average_rtt_ms,
                    "max_ms": rtt.max_rtt_ms,
                    "observed_count": rtt.observed_count,
                    "last_observed_at": _iso(rtt.last_observed_at),
                },
            }
            for peer, rtt in rows
        ]
        return {"diagnostics": route_diagnostics, "items": peers}

    def _build_session_state(self) -> dict[str, object]:
        sessions = self.engine.services.session_manager.list_sessions()
        items = [
            {
                "session_id": session.session_id,
                "scope": session.session_scope,
                "state": session.session_state,
                "handshake_state": session.handshake_state,
                "direction": session.direction,
                "initiator_side": session.initiator_side,
                "local_identity_id": session.local_identity_id,
                "remote_identity_id": session.remote_identity_id,
                "remote_host": session.remote_host,
                "remote_port": session.remote_port,
                "transport": session.transport,
                "bound_route_id": session.bound_route_id,
                "last_activity_at": _iso(session.last_activity_at),
                "last_keepalive_sent_at": _iso(session.last_keepalive_sent_at),
                "keepalive_deadline": _iso(session.keepalive_deadline),
                "close_reason": session.close_reason,
            }
            for session in sessions
        ]
        return {
            "total": len(items),
            "active": len([item for item in items if item["state"] == "active"]),
            "physical_active": len(
                [item for item in items if item["scope"] == "physical" and item["state"] == "active"]
            ),
            "virtual_active": len(
                [item for item in items if item["scope"] == "virtual" and item["state"] == "active"]
            ),
            "items": items,
        }

    def _build_virtual_node_state(self) -> dict[str, object]:
        with self.engine.services.database.session_scope() as session:
            local_nodes = session.scalars(
                select(LocalVirtualNodeIdentity).order_by(LocalVirtualNodeIdentity.created_at.asc())
            ).all()
            remote_nodes = session.scalars(
                select(RemoteVirtualNodeIdentity).order_by(
                    RemoteVirtualNodeIdentity.last_seen_at.desc(),
                    RemoteVirtualNodeIdentity.id.asc(),
                )
            ).all()

        return {
            "local": [
                {
                    "id": node.id,
                    "kind": node.kind,
                    "owner_physical_node_id": node.owner_physical_node_id,
                    "is_active": node.is_active,
                    "created_at": _iso(node.created_at),
                    "updated_at": _iso(node.updated_at),
                    "expires_at": _iso(node.expires_at),
                    "metadata": _json_object(node.metadata_json),
                }
                for node in local_nodes
            ],
            "remote": [
                {
                    "id": node.id,
                    "kind": node.kind,
                    "status": node.status,
                    "first_seen_at": _iso(node.first_seen_at),
                    "last_seen_at": _iso(node.last_seen_at),
                    "expires_at": _iso(node.expires_at),
                    "metadata": _json_object(node.metadata_json),
                }
                for node in remote_nodes
            ],
        }

    def _build_route_state(self) -> dict[str, object]:
        with self.engine.services.database.session_scope() as session:
            rows = session.scalars(
                select(RouteResolution)
                .order_by(RouteResolution.id.desc())
                .limit(300)
            ).all()

        items = [
            {
                "id": route.id,
                "local_role": route.local_role,
                "strategy": route.route_strategy,
                "status": route.status,
                "is_valid": route.is_valid,
                "local_virtual_node_id": route.local_virtual_node_id,
                "initial_path_id": route.initial_path_id,
                "route_path_id": route.route_path_id,
                "received_path_id": route.received_path_id,
                "generated_path_id": route.generated_path_id,
                "final_path_id": route.final_path_id,
                "from_physical_node_id": route.from_physical_node_id,
                "to_physical_node_id": route.to_physical_node_id,
                "previous_physical_node_id": route.previous_physical_node_id,
                "first_hop_physical_node_id": route.first_hop_physical_node_id,
                "has_shared_secret": bool(route.shared_secret_hex),
                "metadata": _json_object(route.metadata_json),
            }
            for route in rows
        ]
        return {
            "total": len(items),
            "active": len([item for item in items if item["status"] == "active" and item["is_valid"]]),
            "pending": len(
                [
                    item
                    for item in items
                    if item["is_valid"] and str(item["status"]).startswith("pending")
                ]
            ),
            "items": items,
        }

    def _build_dht_state(self) -> dict[str, object]:
        with self.engine.services.database.session_scope() as session:
            namespace_rows = session.execute(
                select(DhtRecord.namespace, func.count(DhtRecord.id))
                .group_by(DhtRecord.namespace)
                .order_by(DhtRecord.namespace.asc())
            ).all()
            unique_namespace_rows = session.execute(
                select(DhtRecord.namespace, func.count(func.distinct(DhtRecord.key)))
                .group_by(DhtRecord.namespace)
                .order_by(DhtRecord.namespace.asc())
            ).all()
            duplicate_key_count = session.scalar(
                select(func.count())
                .select_from(
                    select(DhtRecord.key)
                    .group_by(DhtRecord.key)
                    .having(func.count(DhtRecord.id) > 1)
                    .subquery()
                )
            ) or 0
            dht_keys = session.scalars(
                select(DhtRecord.key)
                .distinct()
                .order_by(DhtRecord.key.asc())
                .limit(5000)
            ).all()
            records = session.scalars(
                select(DhtRecord)
                .order_by(DhtRecord.updated_at.desc(), DhtRecord.id.desc())
                .limit(200)
            ).all()

        return {
            "counts_by_namespace": {
                namespace: int(count)
                for namespace, count in namespace_rows
            },
            "unique_counts_by_namespace": {
                namespace: int(count)
                for namespace, count in unique_namespace_rows
            },
            "total_rows": int(sum(count for _, count in namespace_rows)),
            "total_unique_keys": len(dht_keys),
            "duplicate_key_count": int(duplicate_key_count),
            "keys": list(dht_keys),
            "recent_records": [
                {
                    "key": record.key,
                    "namespace": record.namespace,
                    "logical_key": record.logical_key,
                    "source": record.source,
                    "record_json_size": len(record.record_json or ""),
                    "created_at": _iso(record.created_at),
                    "updated_at": _iso(record.updated_at),
                    "expires_at": _iso(record.expires_at),
                    "last_validated_at": _iso(record.last_validated_at),
                }
                for record in records
            ],
        }

    def _build_content_state(self) -> dict[str, object]:
        downloads = self.engine.services.content_transfer_service.list_download_states()
        with self.engine.services.database.session_scope() as session:
            content_count = session.scalar(
                select(func.count()).select_from(ContentObject).where(ContentObject.is_deleted.is_(False))
            ) or 0
            advertisement_count = session.scalar(
                select(func.count())
                .select_from(ContentAdvertisement)
                .where(ContentAdvertisement.is_active.is_(True))
            ) or 0
            items = session.scalars(
                select(ContentObject)
                .where(ContentObject.is_deleted.is_(False))
                .order_by(ContentObject.updated_at.desc(), ContentObject.id.desc())
                .limit(50)
            ).all()

        return {
            "content_count": int(content_count),
            "active_advertisement_count": int(advertisement_count),
            "downloads": [
                {
                    "session_id": state.session_id,
                    "content_id": state.content_id,
                    "size_bytes": state.size_bytes,
                    "next_start_byte": state.next_start_byte,
                    "status": state.status,
                    "error_message": state.error_message,
                    "partial_path": str(state.partial_path),
                    "final_path": str(state.final_path),
                }
                for state in downloads
            ],
            "recent_content": [
                {
                    "content_hash": item.content_hash,
                    "title": item.title,
                    "content_type": item.content_type,
                    "size_bytes": item.size_bytes,
                    "storage_path": item.storage_path,
                    "updated_at": _iso(item.updated_at),
                    "last_access_at": _iso(item.last_access_at),
                }
                for item in items
            ],
        }

    def _build_runtime_state(self) -> dict[str, object]:
        runtime_services = self.engine.services.runtime_services
        if runtime_services is None:
            return {}

        runtime_names = (
            "dht_maintenance",
            "physical_node_info_exchange",
            "physical_ping",
            "physical_node_validation",
            "virtual_route_maintenance",
            "session",
        )
        return {
            name: self._describe_runtime(getattr(runtime_services, name))
            for name in runtime_names
        }

    @staticmethod
    def _describe_runtime(runtime: object) -> dict[str, object]:
        task = getattr(runtime, "_task", None)
        stop_event = getattr(runtime, "_stop_event", None)
        return {
            "running": task is not None and not task.done(),
            "task_done": None if task is None else task.done(),
            "stop_requested": bool(stop_event.is_set()) if stop_event is not None else None,
            "interval_seconds": getattr(runtime, "_loop_interval_seconds", None),
        }

    def _build_problem_list(self) -> list[dict[str, object]]:
        problems: list[dict[str, object]] = []
        route_diagnostics = (
            self.engine.services.identity_service.build_remote_physical_node_route_diagnostics()
        )
        if route_diagnostics.get("route_ready_nodes", 0) == 0:
            problems.append(
                {
                    "severity": "warning",
                    "area": "peers",
                "message": "No remote physical node is ready to build routes.",
                }
            )

        sessions = self.engine.services.session_manager.list_sessions()
        closed_sessions = [session for session in sessions if session.session_state == "closed"]
        if closed_sessions:
            problems.append(
                {
                    "severity": "info",
                    "area": "sessions",
                    "message": f"{len(closed_sessions)} sessoes fechadas em memoria.",
                }
            )

        with self.engine.services.database.session_scope() as session:
            stale_pending_routes = session.scalar(
                select(func.count())
                .select_from(RouteResolution)
                .where(RouteResolution.is_valid.is_(True))
                .where(RouteResolution.status.in_(("pending_kem_offer", "pending_final_validation")))
            ) or 0
        if stale_pending_routes:
            problems.append(
                {
                    "severity": "info",
                    "area": "routes",
                    "message": f"{int(stale_pending_routes)} rotas pendentes.",
                }
            )
        duplicate_dht_keys = self._count_duplicate_dht_keys()
        if duplicate_dht_keys:
            problems.append(
                {
                    "severity": "warning",
                    "area": "dht",
                    "message": f"{duplicate_dht_keys} chaves DHT possuem registros locais duplicados.",
                }
            )
        return problems

    def _count_duplicate_dht_keys(self) -> int:
        with self.engine.services.database.session_scope() as session:
            return int(
                session.scalar(
                    select(func.count())
                    .select_from(
                        select(DhtRecord.key)
                        .group_by(DhtRecord.key)
                        .having(func.count(DhtRecord.id) > 1)
                        .subquery()
                    )
                )
                or 0
            )


def _iso(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)


def _json_object(raw_json: str | None) -> dict[str, object]:
    if not raw_json:
        return {}
    try:
        payload = json.loads(raw_json)
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, dict) else {}
