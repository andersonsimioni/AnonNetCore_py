from __future__ import annotations

from datetime import datetime

try:
    from sqlalchemy import Boolean, DateTime, Float, ForeignKey, Integer, String, Text
    from sqlalchemy.orm import Mapped, mapped_column
except ModuleNotFoundError as error:
    raise ModuleNotFoundError(
        "SQLAlchemy nao esta instalado. Instale a dependencia para usar o ORM local."
    ) from error

from .base import Base, IntegerPrimaryKeyMixin


class SeenHash(IntegerPrimaryKeyMixin, Base):
    __tablename__ = "seen_hash"

    hash_value: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    hash_type: Mapped[str] = mapped_column(String(20), nullable=False, index=True)
    first_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class LocalSetting(IntegerPrimaryKeyMixin, Base):
    __tablename__ = "local_setting"

    key: Mapped[str] = mapped_column(String(255), nullable=False, unique=True, index=True)
    value: Mapped[str] = mapped_column(Text, nullable=False)
    value_type: Mapped[str] = mapped_column(String(50), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class LocalEventLog(IntegerPrimaryKeyMixin, Base):
    __tablename__ = "local_event_log"

    event_type: Mapped[str] = mapped_column(String(100), nullable=False, index=True)
    severity: Mapped[str] = mapped_column(String(20), nullable=False, index=True)
    entity_type: Mapped[str | None] = mapped_column(String(100), nullable=True)
    entity_id: Mapped[str | None] = mapped_column(String(128), nullable=True, index=True)
    message: Mapped[str] = mapped_column(Text, nullable=False)
    details_json: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class PhysicalNodeInfoExchangeState(IntegerPrimaryKeyMixin, Base):
    """Estado local do ultimo intercambio de peers fisicos com um remote physical node."""

    __tablename__ = "physical_node_info_exchange_state"

    remote_physical_node_id: Mapped[str] = mapped_column(
        ForeignKey("remote_physical_node_identity.id"),
        nullable=False,
        unique=True,
        index=True,
    )
    last_exchange_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_request_sent_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_response_received_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_announce_received_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    metadata_json: Mapped[str | None] = mapped_column(Text, nullable=True)


class RttInfo(IntegerPrimaryKeyMixin, Base):
    """Estatisticas locais de RTT observadas para um remote physical node."""

    __tablename__ = "rtt_info"

    remote_physical_node_id: Mapped[str] = mapped_column(
        ForeignKey("remote_physical_node_identity.id"),
        nullable=False,
        unique=True,
        index=True,
    )
    min_rtt_ms: Mapped[float] = mapped_column(Float, nullable=False)
    max_rtt_ms: Mapped[float] = mapped_column(Float, nullable=False)
    average_rtt_ms: Mapped[float] = mapped_column(Float, nullable=False)
    observed_count: Mapped[int] = mapped_column(Integer, nullable=False)
    last_observed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    metadata_json: Mapped[str | None] = mapped_column(Text, nullable=True)


class PathIdMapping(IntegerPrimaryKeyMixin, Base):
    """Mapeia o path_id recebido para o path_id local gerado ao encaminhar uma rota."""

    __tablename__ = "path_id_mapping"

    from_physical_node_id: Mapped[str] = mapped_column(
        ForeignKey("remote_physical_node_identity.id"),
        nullable=False,
        index=True,
    )
    to_physical_node_id: Mapped[str] = mapped_column(
        ForeignKey("remote_physical_node_identity.id"),
        nullable=False,
        index=True,
    )
    received_path_id: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    generated_path_id: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    is_valid: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    metadata_json: Mapped[str | None] = mapped_column(Text, nullable=True)
