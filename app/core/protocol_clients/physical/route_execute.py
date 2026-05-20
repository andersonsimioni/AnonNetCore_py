from __future__ import annotations

import json

from crypto import aes_encrypt_hex


class RouteExecuteClient:
    """Envia payloads por uma rota ja construida."""

    def __init__(self, engine) -> None:
        self.engine = engine

    async def send_from_initiator(
        self,
        *,
        initial_path_id: str,
        virtual_envelope: dict[str, object],
        virtual_envelope_ciphered: bool,
        virtual_session_id: str | None = None,
    ) -> dict[str, object]:
        initiator_resolution = self.engine.services.route_service.get_initiator_resolution_by_initial_path_id(
            initial_path_id=initial_path_id,
        )
        if initiator_resolution is None:
            raise ValueError("A rota informada nao existe no estado local do initiator.")
        if not initiator_resolution.first_hop_physical_node_id:
            raise ValueError("A rota informada nao possui first hop associado.")

        route_data_payload = self._build_route_data_payload(
            virtual_session_id=virtual_session_id,
            virtual_envelope=virtual_envelope,
            virtual_envelope_ciphered=virtual_envelope_ciphered,
        )
        await self.engine.forward_message_to_remote_physical_node(
            remote_physical_node_id=initiator_resolution.first_hop_physical_node_id,
            message_type="ROUTE_DATA",
            payload={
                "path_id": initial_path_id,
                **route_data_payload,
            },
        )
        return {
            "initial_path_id": initial_path_id,
            "first_hop_physical_node_id": initiator_resolution.first_hop_physical_node_id,
            "virtual_session_id": virtual_session_id,
            "virtual_envelope_ciphered": virtual_envelope_ciphered,
        }

    async def send_from_local_route(
        self,
        *,
        local_route_path_id: str,
        virtual_envelope: dict[str, object],
        virtual_envelope_ciphered: bool,
        virtual_session_id: str | None = None,
    ) -> dict[str, object]:
        route_target = self._resolve_local_route_target(local_route_path_id)
        route_data_payload = self._build_route_data_payload(
            virtual_session_id=virtual_session_id,
            virtual_envelope=virtual_envelope,
            virtual_envelope_ciphered=virtual_envelope_ciphered,
        )
        await self.engine.forward_message_to_remote_physical_node(
            remote_physical_node_id=route_target["target_remote_physical_node_id"],
            message_type="ROUTE_DATA",
            payload={
                "path_id": route_target["path_id"],
                **route_data_payload,
            },
        )
        return {
            "local_route_path_id": local_route_path_id,
            "path_id": route_target["path_id"],
            "target_remote_physical_node_id": route_target["target_remote_physical_node_id"],
            "virtual_session_id": virtual_session_id,
            "virtual_envelope_ciphered": virtual_envelope_ciphered,
        }

    async def send_to_entry_point(
        self,
        *,
        entry_point_physical_node_id: str,
        route_path_id: str,
        virtual_envelope: dict[str, object],
        virtual_envelope_ciphered: bool,
        virtual_session_id: str | None = None,
    ) -> dict[str, object]:
        local_entry_point_target = self._resolve_local_entry_point_target(
            entry_point_physical_node_id=entry_point_physical_node_id,
            final_path_id=route_path_id,
        )
        if local_entry_point_target is not None:
            route_data_payload = self._build_route_data_payload(
                virtual_session_id=virtual_session_id,
                virtual_envelope=virtual_envelope,
                virtual_envelope_ciphered=virtual_envelope_ciphered,
            )
            await self.engine.forward_message_to_remote_physical_node(
                remote_physical_node_id=local_entry_point_target["target_remote_physical_node_id"],
                message_type="ROUTE_DATA",
                payload={
                    "path_id": local_entry_point_target["path_id"],
                    **route_data_payload,
                },
            )
            self.engine.services.log_service.debug(
                "route_execute_client",
                "sent route data through local entry point",
                entry_point_physical_node_id=entry_point_physical_node_id,
                final_path_id=route_path_id,
                route_path_id=local_entry_point_target["path_id"],
                target_remote_physical_node_id=local_entry_point_target["target_remote_physical_node_id"],
                virtual_session_id=virtual_session_id,
            )
            return {
                "entry_point_physical_node_id": entry_point_physical_node_id,
                "path_id": local_entry_point_target["path_id"],
                "virtual_session_id": virtual_session_id,
                "virtual_envelope_ciphered": virtual_envelope_ciphered,
            }

        route_data_payload = self._build_route_data_payload(
            virtual_session_id=virtual_session_id,
            virtual_envelope=virtual_envelope,
            virtual_envelope_ciphered=virtual_envelope_ciphered,
        )
        await self.engine.forward_message_to_remote_physical_node(
            remote_physical_node_id=entry_point_physical_node_id,
            message_type="ROUTE_DATA",
            payload={
                "path_id": route_path_id,
                **route_data_payload,
            },
        )
        return {
            "entry_point_physical_node_id": entry_point_physical_node_id,
            "path_id": route_path_id,
            "virtual_session_id": virtual_session_id,
            "virtual_envelope_ciphered": virtual_envelope_ciphered,
        }

    def _build_route_data_payload(
        self,
        *,
        virtual_session_id: str | None,
        virtual_envelope: dict[str, object],
        virtual_envelope_ciphered: bool,
    ) -> dict[str, object]:
        if not virtual_envelope_ciphered:
            return {
                "virtual_session_id": virtual_session_id,
                "virtual_envelope_ciphered": False,
                "virtual_envelope": virtual_envelope,
            }

        if not virtual_session_id:
            raise ValueError("virtual_session_id e obrigatorio para enviar virtual_envelope cifrado.")

        session = self.engine.services.session_manager.get_session_by_session_id(virtual_session_id)
        if session is None or session.session_state != "active" or not session.shared_secret_hex:
            raise ValueError("A virtual session informada nao esta ativa para cifrar o envelope.")

        plaintext_hex = json.dumps(
            virtual_envelope,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8").hex()
        encrypted_virtual_envelope = aes_encrypt_hex(plaintext_hex, session.shared_secret_hex)
        self.engine.services.session_manager.touch_session(virtual_session_id)
        return {
            "virtual_session_id": virtual_session_id,
            "virtual_envelope_ciphered": True,
            "virtual_envelope": encrypted_virtual_envelope.payload_hex,
        }

    def _resolve_local_route_target(
        self,
        local_route_path_id: str,
    ) -> dict[str, str]:
        initiator_resolution = self.engine.services.route_service.get_initiator_resolution_by_initial_path_id(
            initial_path_id=local_route_path_id,
        )
        if initiator_resolution is not None and initiator_resolution.first_hop_physical_node_id:
            return {
                "path_id": local_route_path_id,
                "target_remote_physical_node_id": initiator_resolution.first_hop_physical_node_id,
            }

        endpoint_resolution = self.engine.services.route_service.get_endpoint_resolution_by_path_id(
            route_path_id=local_route_path_id,
        )
        if endpoint_resolution is not None and endpoint_resolution.previous_physical_node_id:
            return {
                "path_id": local_route_path_id,
                "target_remote_physical_node_id": endpoint_resolution.previous_physical_node_id,
            }

        raise ValueError("A rota local informada nao existe ou nao possui proximo hop associado.")

    def _resolve_local_entry_point_target(
        self,
        *,
        entry_point_physical_node_id: str,
        final_path_id: str,
    ) -> dict[str, str] | None:
        local_node = self.engine.services.identity_service.get_local_physical_node_result()
        if local_node is None or local_node.id != entry_point_physical_node_id:
            return None

        endpoint_resolution = self.engine.services.route_service.get_endpoint_resolution_by_final_path_id(
            final_path_id=final_path_id,
        )
        if endpoint_resolution is None:
            return None
        if not endpoint_resolution.route_path_id or not endpoint_resolution.previous_physical_node_id:
            return None

        return {
            "path_id": endpoint_resolution.route_path_id,
            "target_remote_physical_node_id": endpoint_resolution.previous_physical_node_id,
        }
