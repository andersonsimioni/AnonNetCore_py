from __future__ import annotations

import asyncio
import random

from transport import OutboundMessage, TransportEndpoint

from ...protocols import DhtProtocolHandler


class PhysicalDhtClient:
    """Cliente ativo do protocolo DHT na camada physical."""

    def __init__(self, engine) -> None:
        self.engine = engine
        self._pending_results: dict[str, asyncio.Future[dict[str, object]]] = {}
        self._response_timeout_seconds = (
            self.engine.services.config.dht_client_response_timeout_seconds
        )
        self._max_hops = self.engine.services.config.dht_client_max_hops

    async def publish(
        self,
        *,
        namespace: str,
        logical_key: str,
        record_json: str,
        expires_at: str | None = None,
    ) -> dict[str, object]:
        visited_node_ids: set[str] = set()
        key_hex = self.engine.services.dht_service.build_key(namespace, logical_key)
        current_session = await self._select_initial_session(key_hex)
        if current_session is None:
            return self._publish_locally_if_responsible(
                key_hex=key_hex,
                namespace=namespace,
                logical_key=logical_key,
                record_json=record_json,
                expires_at=expires_at,
            )

        for _ in range(self._max_hops):
            visited_node_ids.add(current_session.remote_identity_id)
            result = await self._request_publish_once(
                session_id=current_session.session_id,
                namespace=namespace,
                logical_key=logical_key,
                record_json=record_json,
                expires_at=expires_at,
            )
            if result.get("status") == "stored":
                result["visited_node_ids"] = sorted(visited_node_ids)
                return result

            if result.get("status") != "closest_nodes":
                result["visited_node_ids"] = sorted(visited_node_ids)
                return result

            current_session = await self._advance_to_next_session(
                responsible_nodes=result.get("responsible_nodes", []),
                visited_node_ids=visited_node_ids,
            )
            if current_session is None:
                return {
                    "status": "not_routable",
                    "key": result.get("key"),
                    "responsible_nodes": result.get("responsible_nodes", []),
                    "visited_node_ids": sorted(visited_node_ids),
                }

        return {
            "status": "max_hops_reached",
            "key": key_hex,
            "visited_node_ids": sorted(visited_node_ids),
        }

    async def query(
        self,
        *,
        namespace: str,
        logical_key: str,
    ) -> dict[str, object]:
        visited_node_ids: set[str] = set()
        key_hex = self.engine.services.dht_service.build_key(namespace, logical_key)
        current_session = await self._select_initial_session(key_hex)
        if current_session is None:
            return self._query_locally_if_responsible(
                key_hex=key_hex,
            )

        for _ in range(self._max_hops):
            visited_node_ids.add(current_session.remote_identity_id)
            result = await self._request_query_once(
                session_id=current_session.session_id,
                namespace=namespace,
                logical_key=logical_key,
            )
            if result.get("status") == "found":
                result["visited_node_ids"] = sorted(visited_node_ids)
                return result

            if result.get("status") == "not_found":
                result["visited_node_ids"] = sorted(visited_node_ids)
                return result

            if result.get("status") != "closest_nodes":
                result["visited_node_ids"] = sorted(visited_node_ids)
                return result

            current_session = await self._advance_to_next_session(
                responsible_nodes=result.get("responsible_nodes", []),
                visited_node_ids=visited_node_ids,
            )
            if current_session is None:
                return {
                    "status": "not_routable",
                    "key": result.get("key"),
                    "responsible_nodes": result.get("responsible_nodes", []),
                    "visited_node_ids": sorted(visited_node_ids),
                }

        return {
            "status": "max_hops_reached",
            "key": key_hex,
            "visited_node_ids": sorted(visited_node_ids),
        }

    def complete_result(
        self,
        *,
        response_to_message_id: str,
        result_data: dict[str, object],
    ) -> None:
        future = self._pending_results.pop(response_to_message_id, None)
        if future is None or future.done():
            return

        future.set_result(result_data)

    async def _request_publish_once(
        self,
        *,
        session_id: str,
        namespace: str,
        logical_key: str,
        record_json: str,
        expires_at: str | None,
    ) -> dict[str, object]:
        session = self._get_active_session(session_id)
        endpoint = self._build_remote_endpoint(session)

        header = self.engine.build_message_header(
            message_type="DHT_PUBLISH",
            physical_session_id=session.session_id,
        )
        payload = DhtProtocolHandler.build_publish_payload(
            header=header,
            namespace=namespace,
            logical_key=logical_key,
            record_json=record_json,
            expires_at=expires_at,
        )
        return await self._send_and_wait_result(
            message_id=header["message_id"],
            transport_name=endpoint.transport_name,
            payload=payload,
            remote_endpoint=endpoint,
        )

    async def _request_query_once(
        self,
        *,
        session_id: str,
        namespace: str,
        logical_key: str,
    ) -> dict[str, object]:
        session = self._get_active_session(session_id)
        endpoint = self._build_remote_endpoint(session)

        header = self.engine.build_message_header(
            message_type="DHT_QUERY",
            physical_session_id=session.session_id,
        )
        payload = DhtProtocolHandler.build_query_payload(
            header=header,
            namespace=namespace,
            logical_key=logical_key,
        )
        return await self._send_and_wait_result(
            message_id=header["message_id"],
            transport_name=endpoint.transport_name,
            payload=payload,
            remote_endpoint=endpoint,
        )

    async def _send_and_wait_result(
        self,
        *,
        message_id: str,
        transport_name: str,
        payload: bytes,
        remote_endpoint: TransportEndpoint,
    ) -> dict[str, object]:
        loop = asyncio.get_running_loop()
        future: asyncio.Future[dict[str, object]] = loop.create_future()
        self._pending_results[message_id] = future

        try:
            await self.engine.send_packet(
                OutboundMessage(
                    transport_name=transport_name,
                    payload=payload,
                    remote_endpoint=remote_endpoint,
                )
            )
            return await asyncio.wait_for(future, timeout=self._response_timeout_seconds)
        finally:
            self._pending_results.pop(message_id, None)

    async def _advance_to_next_session(
        self,
        *,
        responsible_nodes: list[object],
        visited_node_ids: set[str],
    ):
        self._remember_responsible_nodes(responsible_nodes)

        for responsible_node in responsible_nodes:
            if not isinstance(responsible_node, dict):
                continue

            node_id = responsible_node.get("node_id")
            if not isinstance(node_id, str) or not node_id:
                continue
            if node_id in visited_node_ids:
                continue
            if responsible_node.get("is_local") is True:
                continue

            existing_session = self.engine.services.session_manager.get_active_physical_session_by_remote_node_id(node_id)
            if existing_session is not None:
                return existing_session

            try:
                session_id = await self.engine.services.protocol_clients.physical.session.start_session(
                    remote_physical_node_id=node_id,
                )
            except Exception:
                continue

            session = self.engine.services.session_manager.get_session_by_session_id(session_id)
            if session is not None and session.session_state == "active":
                return session

        return None

    async def _select_initial_session(self, key_hex: str):
        closest_nodes_result = self.engine.services.dht_service.select_k_closest_nodes(key_hex)
        responsible_nodes = list(closest_nodes_result.get("nodes", []))
        remote_candidates = [
            node
            for node in responsible_nodes
            if isinstance(node, dict) and node.get("is_local") is not True
        ]
        random.shuffle(remote_candidates)

        for remote_candidate in remote_candidates:
            node_id = remote_candidate.get("node_id")
            if not isinstance(node_id, str) or not node_id:
                continue

            existing_session = self.engine.services.session_manager.get_active_physical_session_by_remote_node_id(node_id)
            if existing_session is not None:
                return existing_session

            try:
                session_id = await self.engine.services.protocol_clients.physical.session.start_session(
                    remote_physical_node_id=node_id,
                )
            except Exception:
                continue

            session = self.engine.services.session_manager.get_session_by_session_id(session_id)
            if session is not None and session.session_state == "active":
                return session

        return None

    def _publish_locally_if_responsible(
        self,
        *,
        key_hex: str,
        namespace: str,
        logical_key: str,
        record_json: str,
        expires_at: str | None,
    ) -> dict[str, object]:
        closest_nodes_result = self.engine.services.dht_service.select_k_closest_nodes(key_hex)
        if not closest_nodes_result.get("local_node_is_responsible"):
            return {
                "status": "not_routable",
                "key": key_hex,
                "responsible_nodes": closest_nodes_result.get("nodes", []),
                "visited_node_ids": [],
            }

        parsed_expires_at = self._parse_optional_datetime(expires_at)
        DhtProtocolHandler._upsert_local_record(
            services=self.engine.services,
            key_hex=key_hex,
            namespace=namespace,
            logical_key=logical_key,
            record_json=record_json,
            expires_at=parsed_expires_at,
            source="dht_publish",
        )
        return {
            "status": "stored",
            "key": key_hex,
            "stored_locally": True,
            "responsible_nodes": closest_nodes_result.get("nodes", []),
            "visited_node_ids": [],
        }

    def _query_locally_if_responsible(
        self,
        *,
        key_hex: str,
    ) -> dict[str, object]:
        closest_nodes_result = self.engine.services.dht_service.select_k_closest_nodes(key_hex)
        if not closest_nodes_result.get("local_node_is_responsible"):
            return {
                "status": "not_routable",
                "key": key_hex,
                "responsible_nodes": closest_nodes_result.get("nodes", []),
                "visited_node_ids": [],
            }

        dht_record = DhtProtocolHandler._load_validated_local_record(
            services=self.engine.services,
            key_hex=key_hex,
        )
        if dht_record is None:
            return {
                "status": "not_found",
                "key": key_hex,
                "stored_locally": False,
                "responsible_nodes": closest_nodes_result.get("nodes", []),
                "visited_node_ids": [],
            }

        return {
            "status": "found",
            "key": key_hex,
            "stored_locally": True,
            "record_json": dht_record.record_json,
            "expires_at": (
                dht_record.expires_at.isoformat()
                if dht_record.expires_at is not None
                else None
            ),
            "responsible_nodes": closest_nodes_result.get("nodes", []),
            "visited_node_ids": [],
        }

    def _remember_responsible_nodes(self, responsible_nodes: list[object]) -> None:
        for responsible_node in responsible_nodes:
            if not isinstance(responsible_node, dict):
                continue
            if responsible_node.get("is_local") is True:
                continue

            node_id = responsible_node.get("node_id")
            public_key = responsible_node.get("public_key")
            endpoints = responsible_node.get("endpoints")
            if (
                not isinstance(node_id, str)
                or not node_id
                or not isinstance(public_key, str)
                or not public_key
                or not isinstance(endpoints, list)
                or not endpoints
            ):
                continue

            valid_endpoints: list[dict[str, object]] = []
            for endpoint in endpoints:
                if not isinstance(endpoint, dict):
                    continue
                transport = endpoint.get("transport")
                host = endpoint.get("host")
                port = endpoint.get("port")
                priority = endpoint.get("priority", 0)
                if isinstance(transport, str) and transport and isinstance(host, str) and host and isinstance(port, int):
                    valid_endpoints.append(
                        {
                            "transport": transport,
                            "host": host,
                            "port": port,
                            "priority": priority if isinstance(priority, int) else 0,
                        }
                    )

            if not valid_endpoints:
                continue

            self.engine.services.identity_service.upsert_discovered_remote_physical_node(
                node_id=node_id,
                public_key=public_key,
                protocol_version=None,
                endpoints=valid_endpoints,
                notes_json='{"source":"dht_result_responsible_nodes"}',
            )

    def _get_active_session(self, session_id: str):
        session = self.engine.services.session_manager.get_session_by_session_id(session_id)
        if session is None:
            raise ValueError("A physical session informada nao existe em memoria.")
        if session.session_state != "active":
            raise ValueError("A physical session informada ainda nao esta ativa.")
        return session

    @staticmethod
    def _build_remote_endpoint(session) -> TransportEndpoint:
        if not session.transport or not session.remote_host or session.remote_port is None:
            raise ValueError("A physical session nao possui endpoint remoto associado.")

        return TransportEndpoint(
            transport_name=session.transport,
            host=session.remote_host,
            port=session.remote_port,
        )

    @staticmethod
    def _parse_optional_datetime(value: str | None):
        if not isinstance(value, str) or not value:
            return None

        try:
            from datetime import datetime

            return datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
