from __future__ import annotations

from storage import get_database
from storage.models import PathIdMapping


class RouteStateService:
    """Persiste estado operacional local das rotas fisicas."""

    def __init__(self) -> None:
        self.database = get_database()

    def create_path_id_mapping(
        self,
        *,
        from_physical_node_id: str,
        to_physical_node_id: str,
        received_path_id: str,
        generated_path_id: str,
    ) -> PathIdMapping:
        with self.database.session_scope() as session:
            mapping = PathIdMapping(
                from_physical_node_id=from_physical_node_id,
                to_physical_node_id=to_physical_node_id,
                received_path_id=received_path_id,
                generated_path_id=generated_path_id,
                is_valid=True,
                metadata_json=None,
            )
            session.add(mapping)
            session.flush()
            session.refresh(mapping)
            return mapping
