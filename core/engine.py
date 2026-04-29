from __future__ import annotations

import json
from uuid import uuid4

from bootstrap import BootstrapResolutionResult
from crypto import aes_decrypt_hex, aes_encrypt_hex
from .models import PacketContext, PacketProcessingResult, ProtocolEnvelope
from .router import MessageRouter
from .services import EngineServices
from transport import OutboundMessage, TransportEndpoint, TransportPacket


class CoreEngine:
    """Nucleo generico para identificar e processar pacotes recebidos."""

    _PLAINTEXT_SESSION_MESSAGE_TYPES = {
        "PHYSICAL_SESSION_INIT",
        "PHYSICAL_SESSION_INIT_OK",
        "PHYSICAL_SESSION_KEY_CONFIRM",
        "PHYSICAL_SESSION_READY",
    }

    def __init__(self) -> None:
        self.services = EngineServices()
        self.services.ensure_defaults()
        self.services.bind_engine(self)
        self.services.transport.set_inbound_packet_handler(self.handle_transport_packet)
        self.bootstrap_result: BootstrapResolutionResult | None = None
        self.message_router = MessageRouter()
        self._message_sequence = 0

    async def start(self) -> None:
        self.services.database.create_schema()
        self.services.identity_service.ensure_local_physical_node()
        self.bootstrap_result = await self._run_bootstrap()
        await self.services.transport.start()
        if self.services.runtime_services is not None:
            await self.services.runtime_services.start()

    async def stop(self) -> None:
        if self.services.runtime_services is not None:
            await self.services.runtime_services.stop()
        await self.services.transport.stop()

    async def send_packet(self, message: OutboundMessage) -> None:
        prepared_message = self._prepare_outbound_message(message)
        await self.services.transport.send(prepared_message)

    async def handle_transport_packet(self, packet: TransportPacket) -> None:
        context = PacketContext(
            transport_name=packet.transport_name,
            payload=packet.payload,
            remote_host=packet.remote_endpoint.host,
            remote_port=packet.remote_endpoint.port,
            local_host=packet.local_endpoint.host if packet.local_endpoint else None,
            local_port=packet.local_endpoint.port if packet.local_endpoint else None,
            received_at=packet.received_at,
            metadata=packet.metadata,
        )
        result = await self.process_received_packet(context)
        if result.response_payload is not None:
            await self.send_packet(
                OutboundMessage(
                    transport_name=packet.transport_name,
                    payload=result.response_payload,
                    remote_endpoint=packet.remote_endpoint,
                )
            )
        await self._execute_processing_result_actions(result)

    async def process_received_packet(self, context: PacketContext) -> PacketProcessingResult:
        try:
            envelope = self._decode_json_packet(context)
        except ValueError as error:
            return PacketProcessingResult(
                protocol_name="json",
                handled=False,
                message_type=None,
                metadata={
                    "reason": "invalid_json_packet",
                    "transport_name": context.transport_name,
                    "error": str(error),
                },
            )

        session_validation = self._validate_message_policy(envelope)
        if session_validation is not None:
            return session_validation

        return await self.message_router.route(envelope, context, self.services)

    async def _run_bootstrap(self) -> BootstrapResolutionResult:
        return await self.services.bootstrap_service.load_bootstrap_targets()

    def build_message_header(
        self,
        *,
        message_type: str,
        physical_session_id: str | None = None,
        virtual_session_id: str | None = None,
    ) -> dict[str, object]:
        self._message_sequence += 1
        return {
            "version": 1,
            "message_type": message_type,
            "message_id": str(uuid4()),
            "message_sequence": self._message_sequence,
            "physical_session_id": physical_session_id,
            "virtual_session_id": virtual_session_id,
        }

    async def forward_message_to_remote_physical_node(
        self,
        *,
        remote_physical_node_id: str,
        message_type: str,
        payload: dict[str, object],
    ) -> None:
        session = await self._ensure_active_physical_session(remote_physical_node_id)
        endpoint = self._build_session_remote_endpoint(session)
        header = self.build_message_header(
            message_type=message_type,
            physical_session_id=session.session_id,
        )
        packet_bytes = self._build_json_packet_bytes(
            header=header,
            payload=payload,
        )
        await self.send_packet(
            OutboundMessage(
                transport_name=endpoint.transport_name,
                payload=packet_bytes,
                remote_endpoint=endpoint,
            )
        )

    def _prepare_outbound_message(self, message: OutboundMessage) -> OutboundMessage:
        packet = self._load_json_packet(message.payload)
        if packet is None:
            return message

        header = packet.get("header")
        payload = packet.get("payload")
        if not isinstance(header, dict) or not isinstance(payload, dict):
            return message

        session_id = header.get("physical_session_id")
        message_type = header.get("message_type")
        if not isinstance(session_id, str) or not isinstance(message_type, str):
            return message

        if message_type in self._PLAINTEXT_SESSION_MESSAGE_TYPES:
            return message

        session = self.services.session_manager.get_session_by_session_id(session_id)
        if session is None or session.session_state != "active" or not session.shared_secret_hex:
            return message

        plaintext_hex = self._encode_payload_hex(payload)
        encrypted_payload = aes_encrypt_hex(plaintext_hex, session.shared_secret_hex)
        protected_packet = {
            "header": {
                **header,
                "payload_encrypted": True,
            },
            "payload": {
                "ciphertext_hex": encrypted_payload.payload_hex,
            },
        }
        self.services.session_manager.touch_session(session_id)
        return OutboundMessage(
            transport_name=message.transport_name,
            payload=json.dumps(protected_packet, separators=(",", ":"), sort_keys=True).encode("utf-8"),
            remote_endpoint=message.remote_endpoint,
            local_endpoint=message.local_endpoint,
            metadata=message.metadata,
        )

    async def _execute_processing_result_actions(
        self,
        result: PacketProcessingResult,
    ) -> None:
        action = result.metadata.get("action")
        if not isinstance(action, str) or not action:
            return

        if action == "forward_message":
            await self._execute_forward_message_action(result.metadata)

    async def _execute_forward_message_action(
        self,
        metadata: dict[str, object],
    ) -> None:
        remote_physical_node_id = metadata.get("target_remote_physical_node_id")
        message_type = metadata.get("forward_message_type")
        payload = metadata.get("forward_payload")

        if not isinstance(remote_physical_node_id, str) or not remote_physical_node_id:
            return
        if not isinstance(message_type, str) or not message_type:
            return
        if not isinstance(payload, dict):
            return

        await self.forward_message_to_remote_physical_node(
            remote_physical_node_id=remote_physical_node_id,
            message_type=message_type,
            payload=payload,
        )

    async def _ensure_active_physical_session(
        self,
        remote_physical_node_id: str,
    ):
        active_session = self.services.session_manager.get_active_physical_session_by_remote_node_id(
            remote_physical_node_id
        )
        if active_session is not None:
            return active_session

        session_id = await self.services.protocol_clients.physical.session.start_session(
            remote_physical_node_id=remote_physical_node_id,
        )
        session = self.services.session_manager.get_session_by_session_id(session_id)
        if session is None:
            raise RuntimeError("A physical session foi criada, mas nao esta disponivel em memoria.")
        return session

    @staticmethod
    def _build_session_remote_endpoint(session) -> TransportEndpoint:
        if not session.transport or not session.remote_host or session.remote_port is None:
            raise ValueError("A physical session nao possui endpoint remoto associado.")

        return TransportEndpoint(
            transport_name=session.transport,
            host=session.remote_host,
            port=session.remote_port,
        )

    def _validate_message_policy(
        self,
        envelope: ProtocolEnvelope,
    ) -> PacketProcessingResult | None:
        definition = self.message_router.get_definition(envelope.message_type)
        if definition is None:
            return None

        if definition.requires_physical_session:
            session_id = envelope.header.get("physical_session_id")
            if not isinstance(session_id, str) or not session_id:
                return PacketProcessingResult(
                    protocol_name=envelope.protocol_name,
                    handled=False,
                    message_type=envelope.message_type,
                    metadata={
                        "reason": "physical_session_required",
                        "layer": definition.layer,
                    },
                )

            session = self.services.session_manager.get_session_by_session_id(session_id)
            if session is None:
                return PacketProcessingResult(
                    protocol_name=envelope.protocol_name,
                    handled=False,
                    message_type=envelope.message_type,
                    metadata={
                        "reason": "physical_session_not_found",
                        "layer": definition.layer,
                    },
                )

            if session.session_state != "active":
                return PacketProcessingResult(
                    protocol_name=envelope.protocol_name,
                    handled=False,
                    message_type=envelope.message_type,
                    metadata={
                        "reason": "physical_session_not_active",
                        "layer": definition.layer,
                    },
                )

        return None

    def _decode_json_packet(self, context: PacketContext) -> ProtocolEnvelope:
        packet = self._load_json_packet(context.payload)
        if packet is None:
            raise ValueError("O payload recebido nao contem um pacote JSON valido.")

        if not isinstance(packet, dict):
            raise ValueError("O pacote JSON precisa ser um objeto.")

        header = packet.get("header")
        if not isinstance(header, dict):
            raise ValueError("O campo 'header' precisa ser um objeto JSON.")

        payload = packet.get("payload")
        if payload is None:
            payload = {}

        message_type = header.get("message_type")
        if message_type is not None and not isinstance(message_type, str):
            raise ValueError("O campo 'header.message_type' precisa ser uma string.")

        payload = self._restore_inbound_payload(header, payload)
        return ProtocolEnvelope(
            protocol_name="json",
            message_type=message_type,
            payload=payload,
            raw_payload=context.payload,
            header=header,
        )

    def _restore_inbound_payload(
        self,
        header: dict[str, object],
        payload: object,
    ) -> object:
        payload_encrypted = header.get("payload_encrypted")
        if payload_encrypted is not True:
            return payload

        session_id = header.get("physical_session_id")
        if not isinstance(session_id, str) or not session_id:
            raise ValueError("Pacote cifrado sem physical_session_id.")

        session = self.services.session_manager.get_session_by_session_id(session_id)
        if session is None or session.session_state != "active" or not session.shared_secret_hex:
            raise ValueError("Pacote cifrado recebido para uma sessao fisica inativa ou desconhecida.")

        if not isinstance(payload, dict):
            raise ValueError("O payload cifrado precisa ser um objeto JSON.")

        ciphertext_hex = payload.get("ciphertext_hex")
        if not isinstance(ciphertext_hex, str) or not ciphertext_hex:
            raise ValueError("O payload cifrado nao contem ciphertext_hex valido.")

        plaintext_json = bytes.fromhex(
            aes_decrypt_hex(ciphertext_hex, session.shared_secret_hex)
        ).decode("utf-8")
        restored_payload = json.loads(plaintext_json)
        self.services.session_manager.touch_session(session_id)
        return restored_payload

    @staticmethod
    def _encode_payload_hex(payload: dict[str, object]) -> str:
        return json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8").hex()

    @staticmethod
    def _build_json_packet_bytes(
        *,
        header: dict[str, object],
        payload: dict[str, object],
    ) -> bytes:
        return json.dumps(
            {
                "header": header,
                "payload": payload,
            },
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")

    @staticmethod
    def _load_json_packet(raw_payload: bytes) -> object | None:
        try:
            return json.loads(raw_payload.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            return None
