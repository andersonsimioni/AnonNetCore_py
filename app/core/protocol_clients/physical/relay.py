from __future__ import annotations

import asyncio
from collections import defaultdict, deque

from common import canonical_payload_hex, compact_json_bytes
from crypto import dilithium_sign_hex
from sessions import build_remote_endpoint_from_session
from transport import OutboundMessage

from ...protocols.physical.relay import build_register_signature_payload


class PhysicalRelayClient:
    """Cliente para registrar nodes privados e trocar dados por relay fisico."""

    def __init__(self, engine) -> None:
        self.engine = engine
        self._response_timeout_seconds = 10.0
        self._pending_responses: dict[str, asyncio.Future[dict[str, object]]] = {}
        self._inbound_data_by_channel: dict[str, deque[dict[str, object]]] = defaultdict(deque)
        self._pending_data: dict[str, asyncio.Future[dict[str, object]]] = {}

    async def register_local_node_at_relay(self, *, relay_physical_node_id: str) -> dict[str, object]:
        local_node = self.engine.services.identity_service.get_local_physical_node_result()
        if local_node is None:
            raise ValueError("The local physical identity has not been initialized yet.")

        challenge = await self._send_request_and_wait(
            relay_physical_node_id=relay_physical_node_id,
            message_type="PHYSICAL_RELAY_REGISTER_REQUEST",
            payload={"target_physical_node_id": local_node.id},
        )
        signature_payload = build_register_signature_payload(
            relay_physical_node_id=_read_string(challenge, "relay_physical_node_id"),
            target_physical_node_id=local_node.id,
            challenge_nonce=_read_string(challenge, "challenge_nonce"),
            expires_at=_read_string(challenge, "expires_at"),
            relay_endpoint=_read_dict(challenge, "relay_endpoint"),
        )
        signature_hex = dilithium_sign_hex(
            canonical_payload_hex(signature_payload),
            local_node.private_key_pem,
        )
        result = await self._send_request_and_wait(
            relay_physical_node_id=relay_physical_node_id,
            message_type="PHYSICAL_RELAY_REGISTER_PROOF",
            payload={
                "target_physical_node_id": local_node.id,
                "target_public_key": local_node.public_key,
                "relay_physical_node_id": signature_payload["relay_physical_node_id"],
                "challenge_nonce": signature_payload["challenge_nonce"],
                "expires_at": signature_payload["expires_at"],
                "signature_hex": signature_hex,
            },
        )
        self.engine.services.log_service.info(
            "physical_relay_client",
            "local physical node registered at relay",
            relay_physical_node_id=relay_physical_node_id,
            target_physical_node_id=local_node.id,
            relay_endpoint=result.get("relay_endpoint"),
        )
        return result

    async def open_channel(
        self,
        *,
        relay_physical_node_id: str,
        target_physical_node_id: str,
    ) -> dict[str, object]:
        result = await self._send_request_and_wait(
            relay_physical_node_id=relay_physical_node_id,
            message_type="PHYSICAL_RELAY_OPEN",
            payload={"target_physical_node_id": target_physical_node_id},
        )
        if result.get("status") == "failed":
            raise RuntimeError(str(result.get("reason") or "relay_open_failed"))
        self.engine.services.log_service.info(
            "physical_relay_client",
            "relay channel opened",
            relay_physical_node_id=relay_physical_node_id,
            target_physical_node_id=target_physical_node_id,
            relay_channel_id=result.get("relay_channel_id"),
        )
        return result

    async def send_channel_data(
        self,
        *,
        relay_physical_node_id: str,
        relay_channel_id: str,
        data: dict[str, object],
    ) -> None:
        await self._send_to_relay_session(
            relay_physical_node_id=relay_physical_node_id,
            message_type="PHYSICAL_RELAY_DATA",
            payload={
                "relay_channel_id": relay_channel_id,
                "data": data,
            },
        )
        self.engine.services.log_service.debug(
            "physical_relay_client",
            "sent relay channel data",
            relay_physical_node_id=relay_physical_node_id,
            relay_channel_id=relay_channel_id,
            data_keys=sorted(data.keys()),
        )

    async def send_transport_packet(
        self,
        *,
        relay_physical_node_id: str,
        target_physical_node_id: str,
        payload: bytes,
        relay_endpoint,
        relay_channel_id: str | None = None,
    ) -> None:
        if relay_channel_id is None:
            channel = await self.open_channel(
                relay_physical_node_id=relay_physical_node_id,
                target_physical_node_id=target_physical_node_id,
            )
            relay_channel_id = _read_string(channel, "relay_channel_id")
        await self.send_channel_data(
            relay_physical_node_id=relay_physical_node_id,
            relay_channel_id=relay_channel_id,
            data={
                "payload_hex": payload.hex(),
                "relay_physical_node_id": relay_physical_node_id,
                "relay_host": relay_endpoint.host,
                "relay_port": relay_endpoint.port,
            },
        )
        self.engine.services.log_service.debug(
            "physical_relay_client",
            "sent transport packet through relay",
            relay_physical_node_id=relay_physical_node_id,
            target_physical_node_id=target_physical_node_id,
            relay_channel_id=relay_channel_id,
            payload_size_bytes=len(payload),
        )

    async def close_channel(
        self,
        *,
        relay_physical_node_id: str,
        relay_channel_id: str,
        reason: str = "local_closed",
    ) -> None:
        await self._send_to_relay_session(
            relay_physical_node_id=relay_physical_node_id,
            message_type="PHYSICAL_RELAY_CLOSE",
            payload={
                "relay_channel_id": relay_channel_id,
                "reason": reason,
            },
        )

    async def receive_channel_data(
        self,
        *,
        relay_channel_id: str,
        timeout_seconds: float = 10.0,
    ) -> dict[str, object]:
        queued = self._inbound_data_by_channel[relay_channel_id]
        if queued:
            return queued.popleft()

        loop = asyncio.get_running_loop()
        future: asyncio.Future[dict[str, object]] = loop.create_future()
        self._pending_data[relay_channel_id] = future
        try:
            return await asyncio.wait_for(future, timeout=timeout_seconds)
        finally:
            self._pending_data.pop(relay_channel_id, None)

    def complete_response(
        self,
        *,
        response_to_message_id: str,
        message_type: str,
        payload: dict[str, object],
    ) -> None:
        future = self._pending_responses.pop(response_to_message_id, None)
        if future is None or future.done():
            self.engine.services.log_service.debug(
                "physical_relay_client",
                "received relay response for unknown request",
                response_to_message_id=response_to_message_id,
                message_type=message_type,
            )
            return
        future.set_result(payload)

    def handle_inbound_data(
        self,
        *,
        relay_channel_id: str,
        payload: dict[str, object],
    ) -> None:
        data = payload.get("data")
        normalized_data = data if isinstance(data, dict) else {"data": data}
        future = self._pending_data.pop(relay_channel_id, None)
        if future is not None and not future.done():
            future.set_result(normalized_data)
            return
        self._inbound_data_by_channel[relay_channel_id].append(normalized_data)

    async def _send_request_and_wait(
        self,
        *,
        relay_physical_node_id: str,
        message_type: str,
        payload: dict[str, object],
    ) -> dict[str, object]:
        loop = asyncio.get_running_loop()
        future: asyncio.Future[dict[str, object]] = loop.create_future()
        header = self.engine.build_message_header(message_type=message_type)
        self._pending_responses[header["message_id"]] = future
        try:
            await self._send_to_relay_session(
                relay_physical_node_id=relay_physical_node_id,
                message_type=message_type,
                payload=payload,
                header=header,
            )
            return await asyncio.wait_for(future, timeout=self._response_timeout_seconds)
        finally:
            self._pending_responses.pop(header["message_id"], None)

    async def _send_to_relay_session(
        self,
        *,
        relay_physical_node_id: str,
        message_type: str,
        payload: dict[str, object],
        header: dict[str, object] | None = None,
    ) -> dict[str, object]:
        session_id = await self.engine.services.protocol_clients.physical.session.start_session(
            remote_physical_node_id=relay_physical_node_id,
        )
        session = self.engine.services.session_manager.get_session_by_session_id(session_id)
        if session is None:
            raise RuntimeError("The physical session with the relay is not active.")

        endpoint = build_remote_endpoint_from_session(session)
        header = dict(header or self.engine.build_message_header(message_type=message_type))
        header["physical_session_id"] = session.session_id
        await self.engine.send_packet(
            OutboundMessage(
                transport_name=endpoint.transport_name,
                payload=compact_json_bytes({"header": header, "payload": payload}),
                remote_endpoint=endpoint,
            )
        )
        return header


def _read_string(payload: dict[str, object], field_name: str) -> str:
    value = payload.get(field_name)
    if isinstance(value, str) and value:
        return value
    raise ValueError(f"Campo obrigatorio ausente no relay response: {field_name}")


def _read_dict(payload: dict[str, object], field_name: str) -> dict[str, object]:
    value = payload.get(field_name)
    if isinstance(value, dict):
        return value
    raise ValueError(f"Campo obrigatorio ausente no relay response: {field_name}")
