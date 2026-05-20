from __future__ import annotations

from hashlib import sha512
from typing import TYPE_CHECKING

from storage import DatabaseManager, get_database
from storage.models import LocalPhysicalNodeIdentity, NodeEndpoint, RemotePhysicalNodeIdentity

if TYPE_CHECKING:
    from core.config import CoreConfig


class DhtService:
    """Operacoes utilitarias da DHT focadas em chave, distancia e responsabilidade."""

    def __init__(
        self,
        config: "CoreConfig | None" = None,
        database: DatabaseManager | None = None,
    ) -> None:
        if config is None:
            from core.config import CoreConfig

            config = CoreConfig()

        self.database = database or get_database()
        self.config = config

    @staticmethod
    def build_key(
        namespace: str,
        logical_key: str,
    ) -> str:
        if not namespace:
            raise ValueError("namespace e obrigatorio.")
        if not logical_key:
            raise ValueError("logical_key e obrigatorio.")

        raw_key = f"{namespace}|{logical_key}"
        return sha512(raw_key.encode("utf-8")).hexdigest()

    @staticmethod
    def xor_distance_hex(left_hex: str, right_hex: str) -> int:
        if not left_hex or not right_hex:
            raise ValueError("left_hex e right_hex sao obrigatorios.")

        return int(left_hex, 16) ^ int(right_hex, 16)

    def select_k_closest_nodes(self, key_hex: str) -> dict[str, object]:
        if not key_hex:
            raise ValueError("key_hex e obrigatorio.")

        node_candidates = self._deduplicate_node_candidates(self._load_node_candidates())
        ranked_nodes = self._rank_nodes_by_distance(key_hex, node_candidates)
        replication_factor = self.config.dht_replication_factor
        selected_nodes = ranked_nodes[:replication_factor]
        self._attach_remote_endpoints(selected_nodes)

        return {
            "key_hex": key_hex,
            "replication_factor": replication_factor,
            "local_node_is_responsible": any(node["is_local"] for node in selected_nodes),
            "nodes": selected_nodes,
        }

    def _load_node_candidates(self) -> list[dict[str, object]]:
        with self.database.session_scope() as session:
            local_nodes = list(
                session.query(LocalPhysicalNodeIdentity)
                .filter(LocalPhysicalNodeIdentity.status == "active")
                .all()
            )
            remote_nodes = list(
                session.query(RemotePhysicalNodeIdentity)
                .join(
                    NodeEndpoint,
                    NodeEndpoint.physical_node_hash_id == RemotePhysicalNodeIdentity.id,
                )
                .filter(RemotePhysicalNodeIdentity.status == "active")
                .filter(RemotePhysicalNodeIdentity.last_validated_at.is_not(None))
                .filter(NodeEndpoint.is_active.is_(True))
                .filter(NodeEndpoint.transport.is_not(None))
                .filter(NodeEndpoint.host.is_not(None))
                .filter(NodeEndpoint.port.is_not(None))
                .distinct()
                .all()
            )

        candidates: list[dict[str, object]] = []
        for local_node in local_nodes:
            candidates.append(
                {
                    "node_id": local_node.id,
                    "is_local": True,
                    "public_key": local_node.public_key,
                }
            )

        for remote_node in remote_nodes:
            candidates.append(
                {
                    "node_id": remote_node.id,
                    "is_local": False,
                    "public_key": remote_node.public_key,
                }
            )

        return candidates

    @staticmethod
    def _deduplicate_node_candidates(
        node_candidates: list[dict[str, object]],
    ) -> list[dict[str, object]]:
        """Mantem um unico candidato por node_id e prefere a identidade local."""

        candidates_by_node_id: dict[str, dict[str, object]] = {}
        for node_candidate in node_candidates:
            node_id = node_candidate.get("node_id")
            if not isinstance(node_id, str) or not node_id:
                continue

            current_candidate = candidates_by_node_id.get(node_id)
            if current_candidate is None:
                candidates_by_node_id[node_id] = node_candidate
                continue

            if node_candidate.get("is_local") is True:
                candidates_by_node_id[node_id] = node_candidate

        return list(candidates_by_node_id.values())

    def _rank_nodes_by_distance(
        self,
        key_hex: str,
        node_candidates: list[dict[str, object]],
    ) -> list[dict[str, object]]:
        ranked_nodes: list[dict[str, object]] = []
        for node_candidate in node_candidates:
            node_id = node_candidate["node_id"]
            distance_int = self.xor_distance_hex(key_hex, node_id)
            ranked_nodes.append(
                {
                    "node_id": node_id,
                    "is_local": node_candidate["is_local"],
                    "public_key": node_candidate["public_key"],
                    "distance_int": distance_int,
                    "distance_hex": format(distance_int, "0128x"),
                }
            )

        ranked_nodes.sort(key=lambda node: node["distance_int"])
        return ranked_nodes

    def _attach_remote_endpoints(self, selected_nodes: list[dict[str, object]]) -> None:
        remote_node_ids = [
            node["node_id"]
            for node in selected_nodes
            if node["is_local"] is False
        ]
        if not remote_node_ids:
            return

        with self.database.session_scope() as session:
            endpoints = list(
                session.query(NodeEndpoint)
                .filter(NodeEndpoint.physical_node_hash_id.in_(remote_node_ids))
                .filter(NodeEndpoint.is_active.is_(True))
                .order_by(
                    NodeEndpoint.physical_node_hash_id.asc(),
                    NodeEndpoint.priority.asc(),
                    NodeEndpoint.last_success_at.desc(),
                )
                .all()
            )

        endpoints_by_node_id: dict[str, list[dict[str, object]]] = {}
        for endpoint in endpoints:
            endpoints_by_node_id.setdefault(endpoint.physical_node_hash_id, []).append(
                {
                    "transport": endpoint.transport,
                    "host": endpoint.host,
                    "port": endpoint.port,
                    "priority": endpoint.priority,
                }
            )

        for node in selected_nodes:
            if node["is_local"] is True:
                node["endpoints"] = []
                continue

            node["endpoints"] = endpoints_by_node_id.get(node["node_id"], [])
