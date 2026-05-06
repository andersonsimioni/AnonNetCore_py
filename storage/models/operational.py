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


class RouteResolution(IntegerPrimaryKeyMixin, Base):
    """Tudo o que o node local precisa para resolver uma rota em qualquer papel."""

    __tablename__ = "route_resolution"

    local_role: Mapped[str] = mapped_column(String(40), nullable=False, index=True)
    route_strategy: Mapped[str | None] = mapped_column(String(50), nullable=True, index=True)
    status: Mapped[str] = mapped_column(String(40), nullable=False, index=True, default="pending")
    route_nonce: Mapped[int | None] = mapped_column(Integer, nullable=True)
    initial_path_id: Mapped[str | None] = mapped_column(String(128), nullable=True, unique=True, index=True)
    route_path_id: Mapped[str | None] = mapped_column(String(128), nullable=True, unique=True, index=True)
    received_path_id: Mapped[str | None] = mapped_column(String(128), nullable=True, index=True)
    generated_path_id: Mapped[str | None] = mapped_column(String(128), nullable=True, index=True)
    final_path_id: Mapped[str | None] = mapped_column(String(128), nullable=True, unique=True, index=True)
    from_physical_node_id: Mapped[str | None] = mapped_column(
        ForeignKey("remote_physical_node_identity.id"),
        nullable=True,
        index=True,
    )
    to_physical_node_id: Mapped[str | None] = mapped_column(
        ForeignKey("remote_physical_node_identity.id"),
        nullable=True,
        index=True,
    )
    previous_physical_node_id: Mapped[str | None] = mapped_column(
        ForeignKey("remote_physical_node_identity.id"),
        nullable=True,
        index=True,
    )
    first_hop_physical_node_id: Mapped[str | None] = mapped_column(
        ForeignKey("remote_physical_node_identity.id"),
        nullable=True,
        index=True,
    )
    local_virtual_node_id: Mapped[str | None] = mapped_column(
        ForeignKey("local_virtual_node_identity.id"),
        nullable=True,
        index=True,
    )
    final_physical_node_public_key: Mapped[str | None] = mapped_column(Text, nullable=True)
    virtual_node_signature: Mapped[str | None] = mapped_column(Text, nullable=True)
    physical_node_signature: Mapped[str | None] = mapped_column(Text, nullable=True)
    public_route_acceptance_signature: Mapped[str | None] = mapped_column(Text, nullable=True)
    kyber_private_key_pem: Mapped[str | None] = mapped_column(Text, nullable=True)
    kyber_public_key_pem: Mapped[str | None] = mapped_column(Text, nullable=True)
    shared_secret_hex: Mapped[str | None] = mapped_column(Text, nullable=True)
    is_valid: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    metadata_json: Mapped[str | None] = mapped_column(Text, nullable=True)
