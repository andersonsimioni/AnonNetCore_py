from __future__ import annotations

import asyncio
import json
import random
from datetime import datetime

from common import load_json_object
from transport import OutboundMessage, TransportEndpoint

from ...protocols import DhtProtocolHandler


class PhysicalDhtClient:
    """DHT client: starts requests and lets handlers route them hop-by-hop."""

    def __init__(self, engine) -> None:
        self.engine = engine
        self._pending_results: dict[str, asyncio.Future[dict[str, object]]] = {}
        self._response_timeout_seconds = (
            self.engine.services.config.dht_request_timeout_seconds
        )
        self._publish_attempt_timeout_seconds = self._response_timeout_seconds
        self._publish_attempt_count = 3
        self._query_attempt_timeout_seconds = self._response_timeout_seconds
        self._query_attempt_count = 6
        self._request_ttl = self.engine.services.config.dht_request_max_forward_hops

    async def publish(
        self,
        *,
        namespace: str,
        logical_key: str,
        record_json: str,
        expires_at: str | None = None,
        trace_context: dict[str, object] | None = None,
    ) -> dict[str, object]:
        key_hex = self.engine.services.dht_service.build_key(namespace, logical_key)
        record_json = self.engine.services.dht_service.attach_record_payload_pow_nonces(
            namespace=namespace,
            key_hex=key_hex,
            record_json=record_json,
            difficulty_bits=self.engine.services.config.network_pow_difficulty_bits,
        )
        pow_nonce = self.engine.services.dht_service.find_publish_pow_nonce(
            key_hex=key_hex,
            record_json=record_json,
            difficulty_bits=self.engine.services.config.network_pow_difficulty_bits,
        )
        pow_details = self.engine.services.dht_service.build_publish_pow_details(
            key_hex=key_hex,
            record_json=record_json,
            nonce=pow_nonce,
            difficulty_bits=self.engine.services.config.network_pow_difficulty_bits,
        )
        self.engine.services.log_service.info(
            "physical_dht_client",
            "starting dht publish",
            namespace=namespace,
            logical_key=logical_key,
            key=key_hex,
            pow_nonce=pow_nonce,
            pow_difficulty_bits=self.engine.services.config.network_pow_difficulty_bits,
            pow_canonical_hash=pow_details["canonical_hash"],
            pow_proof_hash_prefix=pow_details["proof_hash_prefix"],
            record_json_size=len(record_json),
            semantic_payload_pow=True,
            trace_context=trace_context,
        )
        local_publish_result = self._publish_locally_if_responsible(
            key_hex=key_hex,
            namespace=namespace,
            logical_key=logical_key,
            record_json=record_json,
            expires_at=expires_at,
            pow_nonce=pow_nonce,
        )
        stored_by = self._read_stored_by(local_publish_result)
        required_stored_count = self._read_required_stored_count(local_publish_result)
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
                trace_context=trace_context,
            )
            return local_publish_result

        attempted_remote_node_ids: set[str] = set()
        session = await self._select_closest_known_session(
            key_hex=key_hex,
            attempted_remote_node_ids=attempted_remote_node_ids,
        )
        if session is None:
            session = await self._select_random_initial_session(
                ignored_remote_node_ids=attempted_remote_node_ids,
            )
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
                trace_context=trace_context,
            )
            return local_publish_result

        result = local_publish_result
        for attempt in range(1, self._publish_attempt_count + 1):
            if isinstance(session.remote_identity_id, str) and session.remote_identity_id:
                attempted_remote_node_ids.add(session.remote_identity_id)
            result = await self._request_publish_once(
                session_id=session.session_id,
                namespace=namespace,
                logical_key=logical_key,
                record_json=record_json,
                expires_at=expires_at,
                stored_by=stored_by,
                required_stored_count=required_stored_count,
                pow_nonce=pow_nonce,
                timeout_seconds=self._publish_attempt_timeout_seconds,
                trace_context=trace_context,
            )
            stored_by = self._merge_stored_by(stored_by, self._read_stored_by(result))
            result = self._normalize_publish_result(
                result=result,
                stored_by=stored_by,
                required_stored_count=required_stored_count,
            )
            if len(stored_by) >= required_stored_count:
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
                trace_context=trace_context,
            )
            if attempt == self._publish_attempt_count:
                break

            next_session = await self._select_random_initial_session(
                ignored_remote_node_ids=attempted_remote_node_ids,
            )
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
            trace_context=trace_context,
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

        attempted_remote_node_ids: set[str] = set()
        for attempt in range(1, self._query_attempt_count + 1):
            session = await self._select_initial_query_session(
                key_hex=key_hex,
                attempted_remote_node_ids=attempted_remote_node_ids,
            )
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

            if isinstance(session.remote_identity_id, str) and session.remote_identity_id:
                attempted_remote_node_ids.add(session.remote_identity_id)

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
            self._add_responsible_node_ids_to_attempts(
                result=result,
                attempted_remote_node_ids=attempted_remote_node_ids,
            )

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
        if future is None:
            self.engine.services.log_service.warning(
                "physical_dht_client",
                "received dht result without pending future",
                response_to_message_id=response_to_message_id,
                status=result_data.get("status"),
                key=result_data.get("key"),
                stored_count=result_data.get("stored_count"),
                required_stored_count=result_data.get("required_stored_count"),
                pending_result_count=len(self._pending_results),
                trace_context=result_data.get("trace_context"),
            )
            return
        if future.done():
            self.engine.services.log_service.warning(
                "physical_dht_client",
                "received dht result for already completed future",
                response_to_message_id=response_to_message_id,
                status=result_data.get("status"),
                key=result_data.get("key"),
                stored_count=result_data.get("stored_count"),
                required_stored_count=result_data.get("required_stored_count"),
                trace_context=result_data.get("trace_context"),
            )
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
        required_stored_count: int,
        pow_nonce: int,
        timeout_seconds: float | None = None,
        trace_context: dict[str, object] | None = None,
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
            required_stored_count=required_stored_count,
            pow_nonce=pow_nonce,
            trace_context=trace_context,
        )
        self.engine.services.log_service.debug(
            "physical_dht_client",
            "sending dht publish request",
            namespace=namespace,
            logical_key=logical_key,
            message_id=header["message_id"],
            session_id=session.session_id,
            remote_physical_node_id=session.remote_identity_id,
            transport=endpoint.transport_name,
            host=endpoint.host,
            port=endpoint.port,
            stored_by=stored_by,
            stored_count=len(stored_by),
            required_stored_count=required_stored_count,
            ttl=self._request_ttl,
            trace_context=trace_context,
        )
        return await self._send_and_wait_result(
            message_id=header["message_id"],
            transport_name=endpoint.transport_name,
            payload=payload,
            remote_endpoint=endpoint,
            timeout_seconds=timeout_seconds,
            trace_context=trace_context,
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
        trace_context: dict[str, object] | None = None,
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
                trace_context=trace_context,
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
                pending_result_count=len(self._pending_results),
                trace_context=trace_context,
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
                trace_context=trace_context,
            )
            return {
                "status": "send_failed",
                "reason": "dht_request_send_failed",
                "error": repr(error),
            }
        finally:
            self._pending_results.pop(message_id, None)

    async def _select_initial_query_session(
        self,
        *,
        key_hex: str,
        attempted_remote_node_ids: set[str],
    ):
        session = await self._select_closest_known_session(
            key_hex=key_hex,
            attempted_remote_node_ids=attempted_remote_node_ids,
        )
        if session is not None:
            return session

        return await self._select_random_initial_session(
            ignored_remote_node_ids=attempted_remote_node_ids,
        )

    def _add_responsible_node_ids_to_attempts(
        self,
        *,
        result: dict[str, object],
        attempted_remote_node_ids: set[str],
    ) -> None:
        responsible_nodes = result.get("responsible_nodes")
        if not isinstance(responsible_nodes, list):
            return

        for node in responsible_nodes:
            if not isinstance(node, dict):
                continue
            node_id = node.get("node_id")
            if isinstance(node_id, str) and node_id:
                attempted_remote_node_ids.add(node_id)

    async def _select_closest_known_session(
        self,
        *,
        key_hex: str,
        attempted_remote_node_ids: set[str],
    ):
        closest_nodes_result = self.engine.services.dht_service.select_k_closest_nodes(key_hex)
        responsible_nodes = [
            node
            for node in closest_nodes_result.get("nodes", [])
            if node.get("is_local") is not True
        ]
        if not responsible_nodes:
            return None

        self.engine.services.log_service.debug(
            "physical_dht_client",
            "trying closest known nodes for dht request",
            key=key_hex,
            candidate_count=len(responsible_nodes),
            attempted_count=len(attempted_remote_node_ids),
        )
        for node in responsible_nodes:
            node_id = node.get("node_id")
            if not isinstance(node_id, str) or not node_id:
                continue
            if node_id in attempted_remote_node_ids:
                continue

            existing_session = self._get_preferred_active_physical_session(node_id)
            if existing_session is not None and not self._is_observed_only_physical_session(existing_session):
                self.engine.services.log_service.debug(
                    "physical_dht_client",
                    "selected active closest known session for dht request",
                    key=key_hex,
                    remote_physical_node_id=node_id,
                    session_id=existing_session.session_id,
                    transport=existing_session.transport,
                )
                return existing_session

            try:
                session_id = await self.engine.services.protocol_clients.physical.session.start_session(
                    remote_physical_node_id=node_id,
                )
            except Exception as error:
                attempted_remote_node_ids.add(node_id)
                self.engine.services.log_service.warning(
                    "physical_dht_client",
                    "failed to open closest known dht request session",
                    key=key_hex,
                    remote_physical_node_id=node_id,
                    error_type=type(error).__name__,
                    error=repr(error),
                )
                continue

            session = self.engine.services.session_manager.get_session_by_session_id(session_id)
            if session is not None and session.session_state == "active":
                self.engine.services.log_service.info(
                    "physical_dht_client",
                    "opened closest known dht request session",
                    key=key_hex,
                    remote_physical_node_id=node_id,
                    session_id=session.session_id,
                )
                return session

        return None

    async def _select_random_initial_session(
        self,
        *,
        ignored_remote_node_ids: set[str] | None = None,
    ):
        ignored_remote_node_ids = ignored_remote_node_ids or set()
        active_sessions = [
            session
            for session in self.engine.services.session_manager.list_active_physical_sessions()
            if not self._is_observed_only_physical_session(session)
            and session.remote_identity_id not in ignored_remote_node_ids
        ]
        if active_sessions:
            preferred_score = min(
                self._transport_preference(session.transport)
                for session in active_sessions
            )
            preferred_sessions = [
                candidate
                for candidate in active_sessions
                if self._transport_preference(candidate.transport) == preferred_score
            ]
            session = random.choice(preferred_sessions)
            self.engine.services.log_service.debug(
                "physical_dht_client",
                "selected random active session for dht request",
                remote_physical_node_id=session.remote_identity_id,
                session_id=session.session_id,
                transport=session.transport,
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
            if candidate.node_id in ignored_remote_node_ids:
                continue
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

    def _get_preferred_active_physical_session(self, remote_physical_node_id: str):
        sessions = [
            session
            for session in self.engine.services.session_manager.list_active_physical_sessions()
            if session.remote_identity_id == remote_physical_node_id
            and not self._is_observed_only_physical_session(session)
        ]
        if not sessions:
            return None
        return min(sessions, key=lambda session: self._transport_preference(session.transport))

    @staticmethod
    def _transport_preference(transport_name: str | None) -> int:
        if transport_name == "tcp":
            return 0
        if transport_name == "relay_tcp":
            return 1
        if transport_name == "udp":
            return 2
        return 3

    def _publish_locally_if_responsible(
        self,
        *,
        key_hex: str,
        namespace: str,
        logical_key: str,
        record_json: str,
        expires_at: str | None,
        pow_nonce: int,
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

        pow_details = self.engine.services.dht_service.build_publish_pow_details(
            key_hex=key_hex,
            record_json=record_json,
            nonce=pow_nonce,
            difficulty_bits=self.engine.services.config.network_pow_difficulty_bits,
        )
        if not pow_details["is_valid"]:
            self.engine.services.log_service.warning(
                "physical_dht_client",
                "local dht publish proof of work is invalid",
                key=key_hex,
                pow_nonce=pow_nonce,
                pow_difficulty_bits=self.engine.services.config.network_pow_difficulty_bits,
                pow_canonical_hash=pow_details["canonical_hash"],
                pow_proof_hash_prefix=pow_details["proof_hash_prefix"],
                record_json_size=len(record_json),
            )
            return {
                "status": "not_routable",
                "reason": "invalid_dht_publish_pow",
                "key": key_hex,
                "responsible_nodes": [],
                "stored_by": [],
                "stored_count": 0,
                "required_stored_count": required_stored_count,
            }

        if not self.engine.services.dht_service.validate_record_payload_pow(
            namespace=namespace,
            key_hex=key_hex,
            record_json=record_json,
            difficulty_bits=self.engine.services.config.network_pow_difficulty_bits,
        ):
            self.engine.services.log_service.warning(
                "physical_dht_client",
                "local dht publish semantic payload proof of work is invalid",
                key=key_hex,
                namespace=namespace,
                logical_key=logical_key,
                pow_difficulty_bits=self.engine.services.config.network_pow_difficulty_bits,
                record_json_size=len(record_json),
            )
            return {
                "status": "not_routable",
                "reason": "invalid_dht_payload_pow",
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
            raise ValueError("The provided physical session does not exist in memory.")
        if session.session_state != "active":
            raise ValueError("The provided physical session is not active yet.")
        return session

    @staticmethod
    def _build_remote_endpoint(session) -> TransportEndpoint:
        if not session.transport or not session.remote_host or session.remote_port is None:
            raise ValueError("The physical session has no associated remote endpoint.")

        return TransportEndpoint(
            transport_name=session.transport,
            host=session.remote_host,
            port=session.remote_port,
            metadata=load_json_object(session.metadata_json),
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
    def _read_required_stored_count(result: dict[str, object]) -> int:
        value = result.get("required_stored_count")
        if isinstance(value, int) and value > 0:
            return value
        return max(1, len(PhysicalDhtClient._read_stored_by(result)))

    @staticmethod
    def _normalize_publish_result(
        *,
        result: dict[str, object],
        stored_by: list[str],
        required_stored_count: int,
    ) -> dict[str, object]:
        normalized = dict(result)
        normalized["stored_by"] = stored_by
        normalized["stored_count"] = len(stored_by)
        normalized["required_stored_count"] = required_stored_count

        if len(stored_by) >= required_stored_count:
            normalized["status"] = "stored"
            normalized.pop("reason", None)
        elif normalized.get("status") == "stored":
            normalized["status"] = "partially_stored"
            normalized["reason"] = "publish_stored_by_below_original_required_count"

        return normalized

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
        return load_json_object(session.metadata_json).get("physical_endpoint_source") == "observed"
