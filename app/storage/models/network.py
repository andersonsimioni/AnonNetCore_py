from __future__ import annotations

from datetime import datetime

try:
    from sqlalchemy import Boolean, DateTime, Float, ForeignKey, Integer, String, Text
    from sqlalchemy.orm import Mapped, mapped_column, relationship
except ModuleNotFoundError as error:
    raise ModuleNotFoundError(
        "SQLAlchemy is not installed. Install the dependency to use the local ORM."
    ) from error

from .base import (
    ActiveFlagMixin,
    Base,
    IntegerPrimaryKeyMixin,
    MetadataJsonMixin,
    StatusMixin,
    TimestampMixin,
)


class LocalPhysicalNodeIdentity(TimestampMixin, StatusMixin, Base):
    __tablename__ = "local_physical_node_identity"

    id: Mapped[str] = mapped_column(String(128), primary_key=True)
    public_key: Mapped[str] = mapped_column(Text, nullable=False)
    private_key_encrypted: Mapped[str] = mapped_column(Text, nullable=False)
    key_algorithm: Mapped[str] = mapped_column(String(100), nullable=False)

    virtual_nodes: Mapped[list["LocalVirtualNodeIdentity"]] = relationship(
        back_populates="owner_physical_node",
        cascade="all, delete-orphan",
    )


class LocalVirtualNodeIdentity(
    TimestampMixin,
    ActiveFlagMixin,
    MetadataJsonMixin,
    Base,
):
    __tablename__ = "local_virtual_node_identity"

    id: Mapped[str] = mapped_column(String(128), primary_key=True)
    public_key: Mapped[str] = mapped_column(Text, nullable=False)
    private_key_encrypted: Mapped[str] = mapped_column(Text, nullable=False)
    kind: Mapped[str] = mapped_column(String(50), nullable=False)
    owner_physical_node_id: Mapped[str] = mapped_column(
        ForeignKey("local_physical_node_identity.id"),
        nullable=False,
        index=True,
    )
    expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    owner_physical_node: Mapped[LocalPhysicalNodeIdentity] = relationship(
        back_populates="virtual_nodes"
    )
    content_advertisements: Mapped[list["ContentAdvertisement"]] = relationship(
        back_populates="advertiser_virtual_node"
    )


class RemotePhysicalNodeIdentity(
    TimestampMixin,
    StatusMixin,
    Base,
):
    __tablename__ = "remote_physical_node_identity"

    id: Mapped[str] = mapped_column(String(128), primary_key=True)
    public_key: Mapped[str] = mapped_column(Text, nullable=False)
    display_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    reachability_class: Mapped[str | None] = mapped_column(String(50), nullable=True)
    relay_capable: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    hole_punch_capable: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    protocol_version: Mapped[str | None] = mapped_column(String(50), nullable=True)
    last_seen_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_validated_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )
    score: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    notes_json: Mapped[str | None] = mapped_column(Text, nullable=True)

    endpoints: Mapped[list["NodeEndpoint"]] = relationship(
        back_populates="physical_node",
        cascade="all, delete-orphan",
    )


class NodeEndpoint(
    IntegerPrimaryKeyMixin,
    ActiveFlagMixin,
    MetadataJsonMixin,
    Base,
):
    __tablename__ = "node_endpoint"

    physical_node_hash_id: Mapped[str] = mapped_column(
        ForeignKey("remote_physical_node_identity.id"),
        nullable=False,
        index=True,
    )
    transport: Mapped[str] = mapped_column(String(50), nullable=False)
    host: Mapped[str] = mapped_column(String(255), nullable=False)
    port: Mapped[int] = mapped_column(Integer, nullable=False)
    priority: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    last_success_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_failure_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    failure_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    physical_node: Mapped[RemotePhysicalNodeIdentity] = relationship(back_populates="endpoints")


class BootstrapSeed(IntegerPrimaryKeyMixin, Base):
    __tablename__ = "bootstrap_seed"

    host: Mapped[str] = mapped_column(String(255), nullable=False)
    port: Mapped[int] = mapped_column(Integer, nullable=False)
    transport: Mapped[str] = mapped_column(String(50), nullable=False)
    is_enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    last_used_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_success_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_failure_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class RemoteVirtualNodeIdentity(
    TimestampMixin,
    StatusMixin,
    MetadataJsonMixin,
    Base,
):
    __tablename__ = "remote_virtual_node_identity"

    id: Mapped[str] = mapped_column(String(128), primary_key=True)
    public_key: Mapped[str] = mapped_column(Text, nullable=False)
    kind: Mapped[str] = mapped_column(String(50), nullable=False)
    first_seen_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_seen_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
