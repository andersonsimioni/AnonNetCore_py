from __future__ import annotations


class RouteExecuteClient:
    """Envia payloads por uma rota ja construida."""

    def __init__(self, engine) -> None:
        self.engine = engine

    async def send_from_initiator(
        self,
        *,
        initial_path_id: str,
        payload: object,
    ) -> dict[str, object]:
        initiator_resolution = self.engine.services.route_service.get_initiator_resolution_by_initial_path_id(
            initial_path_id=initial_path_id,
        )
        if initiator_resolution is None:
            raise ValueError("A rota informada nao existe no estado local do initiator.")
        if not initiator_resolution.first_hop_physical_node_id:
            raise ValueError("A rota informada nao possui first hop associado.")

        await self.engine.forward_message_to_remote_physical_node(
            remote_physical_node_id=initiator_resolution.first_hop_physical_node_id,
            message_type="ROUTE_DATA",
            payload={
                "path_id": initial_path_id,
                "payload": payload,
            },
        )
        return {
            "initial_path_id": initial_path_id,
            "first_hop_physical_node_id": initiator_resolution.first_hop_physical_node_id,
        }
