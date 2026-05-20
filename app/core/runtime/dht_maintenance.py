from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, timezone

from dht import (
    DdtRecordPayload,
    DpntRecordPayload,
    DptRecordPayload,
    DrtRecordPayload,
    DttRecordPayload,
    parse_record,
    serialize_record,
    validate_and_merge,
)
from sqlalchemy import func
from storage.models import DhtRecord


class DhtMaintenanceRuntime:
    """Mantem registros DHT locais validados e presentes nos K responsaveis."""

    def __init__(self, engine) -> None:
        self.engine = engine
        self._task: asyncio.Task[None] | None = None
        self._stop_event = asyncio.Event()
        self._loop_interval_seconds = (
            self.engine.services.config.dht_maintenance_runtime_interval_seconds
        )
        self._publish_backoff_seconds = (
            self.engine.services.config.dht_maintenance_publish_backoff_seconds
        )
        self._last_publish_by_record_key: dict[str, float] = {}

    async def start(self) -> None:
        if self._task is not None and not self._task.done():
            return

        self._stop_event = asyncio.Event()
        self._task = asyncio.create_task(self._run_loop(), name="dht-maintenance-runtime")

    async def stop(self) -> None:
        if self._task is None:
            return

        self._stop_event.set()
        try:
            await self._task
        finally:
            self._task = None

    async def _run_loop(self) -> None:
        while not self._stop_event.is_set():
            await self._run_once()
            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=self._loop_interval_seconds)
            except TimeoutError:
                continue

    async def _run_once(self) -> None:
        await self._validate_local_records()
        await self._replicate_records_to_current_responsible_nodes()
        await self._handoff_records_no_longer_responsible()

    async def _validate_local_records(self) -> None:
        """Valida registros DHT locais antes de disponibiliza-los para a rede."""
        services = self.engine.services
        with services.database.session_scope() as session:
            fragment = (
                session.query(DhtRecord)
                .filter(DhtRecord.last_validated_at.is_(None))
                .order_by(DhtRecord.created_at.asc(), DhtRecord.id.asc())
                .first()
            )
            if fragment is None:
                return None

            services.log_service.debug(
                "dht_maintenance_runtime",
                "validating local dht fragment",
                key=fragment.key,
                namespace=fragment.namespace,
                logical_key=fragment.logical_key,
                fragment_id=fragment.id,
            )
            parent = (
                session.query(DhtRecord)
                .filter(DhtRecord.key == fragment.key)
                .filter(DhtRecord.last_validated_at.is_not(None))
                .order_by(DhtRecord.last_validated_at.desc(), DhtRecord.updated_at.desc())
                .first()
            )

            try:
                fragment_payload = parse_record(fragment.namespace, fragment.record_json)
                parent_payload = (
                    parse_record(parent.namespace, parent.record_json)
                    if parent is not None
                    else self._build_seed_parent_payload(fragment.namespace, fragment_payload)
                )

                merged_payload = validate_and_merge(
                    fragment.namespace,
                    fragment.key,
                    parent_payload,
                    fragment_payload,
                )
            except Exception as error:
                services.log_service.warning(
                    "dht_maintenance_runtime",
                    "failed to validate local dht fragment",
                    key=fragment.key,
                    namespace=fragment.namespace,
                    logical_key=fragment.logical_key,
                    fragment_id=fragment.id,
                    error=str(error),
                )
                session.delete(fragment)
                return None

            now = datetime.now(timezone.utc)
            merged_record_json = serialize_record(merged_payload)

            if parent is None:
                parent = DhtRecord(
                    key=fragment.key,
                    namespace=fragment.namespace,
                    logical_key=fragment.logical_key,
                    record_json=merged_record_json,
                    source=fragment.source,
                    last_validated_at=now,
                    expires_at=self._select_preferred_expires_at(None, fragment.expires_at),
                )
                session.add(parent)
            else:
                parent.record_json = merged_record_json
                parent.source = fragment.source
                parent.last_validated_at = now
                parent.expires_at = self._select_preferred_expires_at(
                    parent.expires_at,
                    fragment.expires_at,
                )

            session.delete(fragment)
            services.log_service.info(
                "dht_maintenance_runtime",
                "validated and merged local dht fragment",
                key=fragment.key,
                namespace=fragment.namespace,
                logical_key=fragment.logical_key,
                created_parent=parent.id if parent.id is not None else None,
            )

    async def _replicate_records_to_current_responsible_nodes(self) -> None:
        """Replica registros em que este node ainda faz parte dos K responsaveis atuais."""
        await self._repair_record_placement(
            local_is_responsible=True,
            placement_reason="current_responsible_replication",
        )

    async def _handoff_records_no_longer_responsible(self) -> None:
        """Repassa registros locais quando este node saiu dos K responsaveis atuais."""
        await self._repair_record_placement(
            local_is_responsible=False,
            placement_reason="handoff_no_longer_responsible",
        )

    async def _repair_record_placement(
        self,
        *,
        local_is_responsible: bool,
        placement_reason: str,
    ) -> None:
        """Publica um registro validado para os K responsaveis atuais da chave."""
        services = self.engine.services
        selected_record = self._select_publishable_record_by_responsibility(
            local_is_responsible=local_is_responsible,
        )
        if selected_record is None:
            return None

        services.log_service.debug(
            "dht_maintenance_runtime",
            "repairing dht record placement",
            key=selected_record.key,
            namespace=selected_record.namespace,
            logical_key=selected_record.logical_key,
            placement_reason=placement_reason,
            local_is_responsible=local_is_responsible,
            responsible_count=selected_record.responsible_count,
        )
        try:
            publish_result = await self._publish_record_to_current_responsible_nodes(
                selected_record
            )
        except Exception as error:
            self._remember_publish(selected_record)
            services.log_service.warning(
                "dht_maintenance_runtime",
                "failed to repair dht record placement",
                key=selected_record.key,
                namespace=selected_record.namespace,
                logical_key=selected_record.logical_key,
                placement_reason=placement_reason,
                error_type=type(error).__name__,
                error=repr(error),
            )
            return

        self._remember_publish(selected_record)
        publish_status = publish_result.get("status")
        stored_count = publish_result.get("stored_count")
        required_stored_count = publish_result.get("required_stored_count")
        if publish_status != "stored":
            services.log_service.warning(
                "dht_maintenance_runtime",
                "dht record placement repair incomplete",
                key=selected_record.key,
                namespace=selected_record.namespace,
                logical_key=selected_record.logical_key,
                placement_reason=placement_reason,
                status=publish_status,
                stored_count=stored_count,
                required_stored_count=required_stored_count,
                reason=publish_result.get("reason"),
            )
            return

        services.log_service.info(
            "dht_maintenance_runtime",
            "finished dht record placement repair",
            key=selected_record.key,
            namespace=selected_record.namespace,
            logical_key=selected_record.logical_key,
            placement_reason=placement_reason,
            status=publish_status,
            stored_count=stored_count,
            required_stored_count=required_stored_count,
        )

    def _build_seed_parent_payload(self, namespace: str, fragment_payload):
        normalized_namespace = namespace.lower()

        if normalized_namespace == "dpnt":
            if not isinstance(fragment_payload, DpntRecordPayload):
                raise ValueError("Payload DPNT invalido para parent inicial.")
            return DpntRecordPayload(
                pk_physical_node=fragment_payload.pk_physical_node,
                endpoints=[],
                transport_methods=[],
                reachability_class="",
                relay_capable=False,
                hole_punch_capable=False,
                protocol_version="",
                feature_flags=[],
                last_validated_at="",
                status="",
                signature="",
            )

        if normalized_namespace == "drt":
            if not isinstance(fragment_payload, DrtRecordPayload):
                raise ValueError("Payload DRT invalido para parent inicial.")
            return DrtRecordPayload(
                pk_virtual_node=fragment_payload.pk_virtual_node,
                route_entries=[],
                last_update="",
            )

        if normalized_namespace == "ddt":
            if not isinstance(fragment_payload, DdtRecordPayload):
                raise ValueError("Payload DDT invalido para parent inicial.")
            return DdtRecordPayload(
                title=fragment_payload.title,
                type=fragment_payload.type,
                tags=list(fragment_payload.tags),
                holders=[],
            )

        if normalized_namespace == "dtt":
            if not isinstance(fragment_payload, DttRecordPayload):
                raise ValueError("Payload DTT invalido para parent inicial.")
            return DttRecordPayload(entries=[])

        if normalized_namespace == "dpt":
            if not isinstance(fragment_payload, DptRecordPayload):
                raise ValueError("Payload DPT invalido para parent inicial.")
            return DptRecordPayload(
                pk_virtual_node_owner=fragment_payload.pk_virtual_node_owner,
                title=fragment_payload.title,
                type=fragment_payload.type,
                last_modified="",
                target_ref="",
                signature="",
            )

        raise ValueError(f"Namespace DHT nao suportado para parent inicial: {namespace}")

    @staticmethod
    def _select_preferred_expires_at(
        current_expires_at,
        fragment_expires_at,
    ):
        if current_expires_at is None:
            return fragment_expires_at
        if fragment_expires_at is None:
            return current_expires_at
        if fragment_expires_at > current_expires_at:
            return fragment_expires_at
        return current_expires_at

    def _select_publishable_record_by_responsibility(
        self,
        *,
        local_is_responsible: bool,
    ) -> "DhtPlacementRecord | None":
        services = self.engine.services
        with services.database.session_scope() as session:
            dht_records = list(
                session.query(DhtRecord)
                .filter(DhtRecord.last_validated_at.is_not(None))
                .order_by(func.random())
                .limit(30)
                .all()
            )

            for dht_record in dht_records:
                placement = self._build_placement_record(dht_record)
                if placement.local_is_responsible != local_is_responsible:
                    continue
                if not self._is_publishable(placement):
                    continue
                return placement

        return None

    def _build_placement_record(self, dht_record: DhtRecord) -> "DhtPlacementRecord":
        responsibility = self.engine.services.dht_service.select_k_closest_nodes(
            dht_record.key
        )
        responsible_nodes = responsibility.get("nodes", [])
        return DhtPlacementRecord(
            key=dht_record.key,
            namespace=dht_record.namespace,
            logical_key=dht_record.logical_key,
            record_json=dht_record.record_json,
            expires_at=(
                dht_record.expires_at.isoformat()
                if dht_record.expires_at is not None
                else None
            ),
            local_is_responsible=bool(
                responsibility.get("local_node_is_responsible")
            ),
            responsible_count=len(responsible_nodes) if isinstance(responsible_nodes, list) else 0,
        )

    async def _publish_record_to_current_responsible_nodes(
        self,
        dht_record: "DhtPlacementRecord",
    ) -> dict[str, object]:
        if self.engine.services.protocol_clients is None:
            raise RuntimeError("Protocol clients ainda nao foram inicializados.")

        return await self.engine.services.protocol_clients.physical.dht.publish(
            namespace=dht_record.namespace,
            logical_key=dht_record.logical_key,
            record_json=dht_record.record_json,
            expires_at=dht_record.expires_at,
        )

    def _is_publishable(
        self,
        dht_record: "DhtPlacementRecord",
    ) -> bool:
        now = asyncio.get_running_loop().time()
        last_publish_at = self._last_publish_by_record_key.get(
            self._build_publish_cache_key(dht_record)
        )
        if last_publish_at is None:
            return True
        return now - last_publish_at >= self._publish_backoff_seconds

    def _remember_publish(
        self,
        dht_record: "DhtPlacementRecord",
    ) -> None:
        self._last_publish_by_record_key[self._build_publish_cache_key(dht_record)] = (
            asyncio.get_running_loop().time()
        )

    @staticmethod
    def _build_publish_cache_key(
        dht_record: "DhtPlacementRecord",
    ) -> str:
        return (
            f"{dht_record.namespace}:"
            f"{dht_record.logical_key}:"
            f"{dht_record.key}:"
            f"{dht_record.local_is_responsible}"
        )


@dataclass(slots=True, frozen=True)
class DhtPlacementRecord:
    key: str
    namespace: str
    logical_key: str
    record_json: str
    expires_at: str | None
    local_is_responsible: bool
    responsible_count: int
