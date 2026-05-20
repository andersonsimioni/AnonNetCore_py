from __future__ import annotations

import asyncio
import json
import random
from datetime import datetime

from transport import OutboundMessage, TransportEndpoint

from ...protocols import DhtProtocolHandler


class PhysicalDhtClient:
    """Cliente DHT: inicia a operacao e deixa os handlers rotearem hop-by-hop."""

    def __init__(self, engine) -> None:
        self.engine = engine
        self._pending_results: dict[str, asyncio.Future[dict[str, object]]] = {}
        self._response_timeout_seconds = (
            self.engine.services.config.dht_client_response_timeout_seconds
        )
        self._publish_attempt_timeout_seconds = self._response_timeout_seconds
        self._publish_attempt_count = 3
        self._query_attempt_timeout_seconds = self._response_timeout_seconds
        self._query_attempt_count = 6
        self._request_ttl = self.engine.services.config.dht_client_max_hops

    async def publish(
        self,
        *,
        namespace: str,
        logical_key: str,
        record_json: str,
        expires_at: str | None = None,
    ) -> dict[str, object]:
        key_hex = self.engine.services.dht_service.build_key(namespace, logical_key)
        self.engine.services.log_service.info(
            "physical_dht_client",
            "starting dht publish",
            namespace=namespace,
            logical_key=logical_key,
            key=key_hex,
        )
        local_publish_result = self._publish_locally_if_responsible(
            key_hex=key_hex,
            namespace=namespace,
            logical_key=logical_key,
            record_json=record_json,
            expires_at=expires_at,
        )
        stored_by = self._read_stored_by(local_publish_result)
        if local_publish_result.get("status") == "stored":
            self.engine.services.log_service.info(
                "physical_dht_client",
                "dht publish completed locally",
                namespace=namespace,
                logical_key=logical_key,
                key=key_hex,
                status=local_publish_result.get("status"),
                stored_count=local_publish_result.get("stored_count"),
                required_stored_count=local_publish_result.get("required_stored_count"),
            )
            return local_publish_result

        session = await self._select_random_initial_session()
        if session is None:
            self.engine.services.log_service.warning(
                "physical_dht_client",
                "dht publish has no active path to responsible nodes",
                namespace=namespace,
                logical_key=logical_key,
                key=key_hex,
                local_status=local_publish_result.get("status"),
                local_reason=local_publish_result.get("reason"),
                stored_count=local_publish_result.get("stored_count"),
                required_stored_count=local_publish_result.get("required_stored_count"),
            )
            return local_publish_result

        result = local_publish_result
        for attempt in range(1, self._publish_attempt_count + 1):
            result = await self._request_publish_once(
                session_id=session.session_id,
                namespace=namespace,
                logical_key=logical_key,
                record_json=record_json,
                expires_at=expires_at,
                stored_by=stored_by,
                timeout_seconds=self._publish_attempt_timeout_seconds,
            )
            stored_by = self._merge_stored_by(stored_by, self._read_stored_by(result))
            if result.get("status") == "stored":
                break

            self.engine.services.log_service.warning(
                "physical_dht_client",
                "dht publish attempt did not finish with full storage",
                namespace=namespace,
                logical_key=logical_key,
                key=key_hex,
                attempt=attempt,
                max_attempts=self._publish_attempt_count,
                status=result.get("status"),
                reason=result.get("reason"),
                stored_count=result.get("stored_count"),
                required_stored_count=result.get("required_stored_count"),
                stored_by=stored_by,
            )
            if attempt == self._publish_attempt_count:
                break

            next_session = await self._select_random_initial_session()
            if next_session is None:
                break
            session = next_session

        self.engine.services.log_service.info(
            "physical_dht_client",
            "dht publish completed through network",
            namespace=namespace,
            logical_key=logical_key,
            key=key_hex,
            status=result.get("status"),
            stored_count=result.get("stored_count"),
            required_stored_count=result.get("required_stored_count"),
            stored_by=result.get("stored_by"),
        )
        return result

    async def query(
        self,
        *,
        namespace: str,
        logical_key: str,
    ) -> dict[str, object]:
        key_hex = self.engine.services.dht_service.build_key(namespace, logical_key)
        self.engine.services.log_service.info(
            "physical_dht_client",
            "starting dht query",
            namespace=namespace,
            logical_key=logical_key,
            key=key_hex,
        )

        result = self._query_locally_if_responsible(key_hex=key_hex)
        if result.get("status") == "found":
            self.engine.services.log_service.info(
                "physical_dht_client",
                "dht query completed locally",
                namespace=namespace,
                logical_key=logical_key,
                key=key_hex,
                status=result.get("status"),
            )
            return result

        for attempt in range(1, self._query_attempt_count + 1):
            session = await self._select_random_initial_session()
            if session is None:
                self.engine.services.log_service.warning(
                    "physical_dht_client",
                    "dht query has no active path to responsible nodes",
                    namespace=namespace,
                    logical_key=logical_key,
                    key=key_hex,
                    attempt=attempt,
                    max_attempts=self._query_attempt_count,
                    local_status=result.get("status"),
                    local_reason=result.get("reason"),
                )
                break

            result = await self._request_query_once(
                session_id=session.session_id,
                namespace=namespace,
                logical_key=logical_key,
                timeout_seconds=self._query_attempt_timeout_seconds,
            )
            self.engine.services.log_service.info(
                "physical_dht_client",
                "dht query attempt completed",
                namespace=namespace,
                logical_key=logical_key,
                key=key_hex,
                attempt=attempt,
                max_attempts=self._query_attempt_count,
                status=result.get("status"),
                reason=result.get("reason"),
                has_record=bool(result.get("record_json")),
                responsible_nodes=result.get("responsible_nodes"),
            )
            if result.get("status") == "found":
                break

        self.engine.services.log_service.info(
            "physical_dht_client",
            "dht query completed through network",
            namespace=namespace,
            logical_key=logical_key,
            key=key_hex,
            status=result.get("status"),
            reason=result.get("reason"),
            has_record=bool(result.get("record_json")),
            responsible_nodes=result.get("responsible_nodes"),
        )
        return result

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
        self.engine.services.log_service.debug(
            "physical_dht_client",
            "completed pending dht result future",
            response_to_message_id=response_to_message_id,
            status=result_data.get("status"),
            key=result_data.get("key"),
            stored_count=result_data.get("stored_count"),
            required_stored_count=result_data.get("required_stored_count"),
        )

    async def _request_publish_once(
        self,
        *,
        session_id: str,
        namespace: str,
        logical_key: str,
        record_json: str,
        expires_at: str | None,
        stored_by: list[str],
        timeout_seconds: float | None = None,
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
            ttl=self._request_ttl,
            stored_by=stored_by,
        )
        return await self._send_and_wait_result(
            message_id=header["message_id"],
            transport_name=endpoint.transport_name,
            payload=payload,
            remote_endpoint=endpoint,
            timeout_seconds=timeout_seconds,
        )

    async def _request_query_once(
        self,
        *,
        session_id: str,
        namespace: str,
        logical_key: str,
        timeout_seconds: float | None = None,
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
            ttl=self._request_ttl,
        )
        return await self._send_and_wait_result(
            message_id=header["message_id"],
            transport_name=endpoint.transport_name,
            payload=payload,
            remote_endpoint=endpoint,
            timeout_seconds=timeout_seconds,
        )

    async def _send_and_wait_result(
        self,
        *,
        message_id: str,
        transport_name: str,
        payload: bytes,
        remote_endpoint: TransportEndpoint,
        timeout_seconds: float | None = None,
    ) -> dict[str, object]:
        loop = asyncio.get_running_loop()
        future: asyncio.Future[dict[str, object]] = loop.create_future()
        self._pending_results[message_id] = future
        wait_timeout_seconds = timeout_seconds or self._response_timeout_seconds

        try:
            await self.engine.send_packet(
                OutboundMessage(
                    transport_name=transport_name,
                    payload=payload,
                    remote_endpoint=remote_endpoint,
                )
            )
            self.engine.services.log_service.debug(
                "physical_dht_client",
                "sent dht request and waiting final result",
                message_id=message_id,
                transport=transport_name,
                host=remote_endpoint.host,
                port=remote_endpoint.port,
                payload_size_bytes=len(payload),
                timeout_seconds=wait_timeout_seconds,
            )
            return await asyncio.wait_for(future, timeout=wait_timeout_seconds)
        except asyncio.TimeoutError:
            self.engine.services.log_service.warning(
                "physical_dht_client",
                "dht request timed out waiting final result",
                message_id=message_id,
                timeout_seconds=wait_timeout_seconds,
                transport=transport_name,
                host=remote_endpoint.host,
                port=remote_endpoint.port,
            )
            return {
                "status": "timeout",
                "reason": "dht_final_result_timeout",
                "timeout_seconds": wait_timeout_seconds,
            }
        except Exception as error:
            self.engine.services.log_service.warning(
                "physical_dht_client",
                "failed to send dht request",
                message_id=message_id,
                transport=transport_name,
                host=remote_endpoint.host,
                port=remote_endpoint.port,
                error_type=type(error).__name__,
                error=repr(error),
            )
            return {
                "status": "send_failed",
                "reason": "dht_request_send_failed",
                "error": repr(error),
            }
        finally:
            self._pending_results.pop(message_id, None)

    async def _select_random_initial_session(self):
        active_sessions = [
            session
            for session in self.engine.services.session_manager.list_active_physical_sessions()
            if not self._is_observed_only_physical_session(session)
        ]
        if active_sessions:
            session = random.choice(active_sessions)
            self.engine.services.log_service.debug(
                "physical_dht_client",
                "selected random active session for dht request",
                remote_physical_node_id=session.remote_identity_id,
                session_id=session.session_id,
            )
            return session

        candidates = self.engine.services.identity_service.list_remote_physical_nodes_for_ping(
            limit=16,
        )
        random.shuffle(candidates)
        self.engine.services.log_service.debug(
            "physical_dht_client",
            "opening random initial dht session from known candidates",
            candidate_count=len(candidates),
        )
        for candidate in candidates:
            try:
                session_id = await self.engine.services.protocol_clients.physical.session.start_session(
                    remote_physical_node_id=candidate.node_id,
                )
            except Exception as error:
                self.engine.services.log_service.warning(
                    "physical_dht_client",
                    "failed to open random initial dht session",
                    remote_physical_node_id=candidate.node_id,
                    error=repr(error),
                )
                continue

            session = self.engine.services.session_manager.get_session_by_session_id(session_id)
            if session is not None and session.session_state == "active":
                self.engine.services.log_service.info(
                    "physical_dht_client",
                    "opened random initial dht session",
                    remote_physical_node_id=candidate.node_id,
                    session_id=session.session_id,
                )
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
        responsible_nodes = closest_nodes_result.get("nodes", [])
        required_stored_count = len(responsible_nodes)
        if not closest_nodes_result.get("local_node_is_responsible"):
            self.engine.services.log_service.debug(
                "physical_dht_client",
                "local node is not responsible for dht publish",
                key=key_hex,
                responsible_count=required_stored_count,
            )
            return {
                "status": "not_routable",
                "key": key_hex,
                "responsible_nodes": [],
                "stored_by": [],
                "stored_count": 0,
                "required_stored_count": required_stored_count,
            }

        local_node = self.engine.services.identity_service.get_local_physical_node_result()
        if local_node is None:
            return {
                "status": "not_routable",
                "reason": "local_physical_node_not_initialized",
                "key": key_hex,
                "responsible_nodes": [],
                "stored_by": [],
                "stored_count": 0,
                "required_stored_count": required_stored_count,
            }

        DhtProtocolHandler._upsert_local_record(
            services=self.engine.services,
            key_hex=key_hex,
            namespace=namespace,
            logical_key=logical_key,
            record_json=record_json,
            expires_at=self._parse_optional_datetime(expires_at),
            source="dht_publish",
        )
        self.engine.services.log_service.info(
            "physical_dht_client",
            "stored dht record locally via publish fallback",
            key=key_hex,
            stored_count=1,
            required_stored_count=required_stored_count,
            responsible_count=required_stored_count,
        )
        status = "stored" if required_stored_count <= 1 else "partially_stored"
        return {
            "status": status,
            "key": key_hex,
            "stored_locally": True,
            "responsible_nodes": [],
            "stored_by": [local_node.id],
            "stored_count": 1,
            "required_stored_count": required_stored_count,
        }

    def _query_locally_if_responsible(
        self,
        *,
        key_hex: str,
    ) -> dict[str, object]:
        closest_nodes_result = self.engine.services.dht_service.select_k_closest_nodes(key_hex)
        if not closest_nodes_result.get("local_node_is_responsible"):
            self.engine.services.log_service.warning(
                "physical_dht_client",
                "local query fallback is not responsible",
                key=key_hex,
            )
            return {
                "status": "not_routable",
                "key": key_hex,
                "responsible_nodes": [],
            }

        dht_record = DhtProtocolHandler._load_validated_local_record(
            services=self.engine.services,
            key_hex=key_hex,
        )
        if dht_record is None:
            self.engine.services.log_service.info(
                "physical_dht_client",
                "local query fallback did not find record",
                key=key_hex,
            )
            return {
                "status": "not_found",
                "key": key_hex,
                "stored_locally": False,
                "responsible_nodes": [],
            }

        self.engine.services.log_service.info(
            "physical_dht_client",
            "local query fallback found record",
            key=key_hex,
        )
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
            "responsible_nodes": [],
        }

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
            return datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None

    @staticmethod
    def _read_stored_by(result: dict[str, object]) -> list[str]:
        value = result.get("stored_by")
        if not isinstance(value, list):
            return []

        return [item for item in value if isinstance(item, str) and item]

    @staticmethod
    def _merge_stored_by(current: list[str], incoming: list[str]) -> list[str]:
        merged: list[str] = []
        seen: set[str] = set()
        for node_id in [*current, *incoming]:
            if node_id in seen:
                continue
            seen.add(node_id)
            merged.append(node_id)
        return merged

    @staticmethod
    def _is_observed_only_physical_session(session) -> bool:
        if not session.metadata_json:
            return False

        try:
            metadata = json.loads(session.metadata_json)
        except json.JSONDecodeError:
            return False

        if not isinstance(metadata, dict):
            return False
        return metadata.get("physical_endpoint_source") == "observed"
