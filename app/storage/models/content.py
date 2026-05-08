from __future__ import annotations

from datetime import datetime

try:
    from sqlalchemy import Boolean, DateTime, ForeignKey, Integer, String
    from sqlalchemy.orm import Mapped, mapped_column, relationship
except ModuleNotFoundError as error:
    raise ModuleNotFoundError(
        "SQLAlchemy nao esta instalado. Instale a dependencia para usar o ORM local."
    ) from error

from .base import ActiveFlagMixin, Base, IntegerPrimaryKeyMixin, TimestampMixin


class ContentObject(IntegerPrimaryKeyMixin, TimestampMixin, Base):
    __tablename__ = "content_object"

    content_hash: Mapped[str] = mapped_column(String(128), nullable=False, unique=True, index=True)
    title: Mapped[str | None] = mapped_column(String(255), nullable=True)
    content_type: Mapped[str | None] = mapped_column(String(100), nullable=True)
    mime_type: Mapped[str | None] = mapped_column(String(255), nullable=True)
    size_bytes: Mapped[int] = mapped_column(Integer, nullable=False)
    storage_path: Mapped[str] = mapped_column(String(500), nullable=False)
    is_encrypted: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    encryption_scheme: Mapped[str | None] = mapped_column(String(100), nullable=True)
    last_access_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    is_deleted: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)

    tags: Mapped[list["ContentTag"]] = relationship(
        back_populates="content_object",
        cascade="all, delete-orphan",
    )
    advertisements: Mapped[list["ContentAdvertisement"]] = relationship(
        back_populates="content_object",
        cascade="all, delete-orphan",
    )
    replicas: Mapped[list["ContentReplica"]] = relationship(
        back_populates="content_object",
        cascade="all, delete-orphan",
    )


class ContentTag(IntegerPrimaryKeyMixin, Base):
    __tablename__ = "content_tag"

    content_object_id: Mapped[int] = mapped_column(
        ForeignKey("content_object.id"),
        nullable=False,
        index=True,
    )
    tag: Mapped[str] = mapped_column(String(255), nullable=False)
    normalized_tag: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    content_object: Mapped[ContentObject] = relationship(back_populates="tags")


class ContentAdvertisement(IntegerPrimaryKeyMixin, ActiveFlagMixin, Base):
    __tablename__ = "content_advertisement"

    content_object_id: Mapped[int] = mapped_column(
        ForeignKey("content_object.id"),
        nullable=False,
        index=True,
    )
    advertiser_virtual_node_id: Mapped[str] = mapped_column(
        ForeignKey("local_virtual_node_identity.id"),
        nullable=False,
        index=True,
    )
    published_in_ddt: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    content_object: Mapped[ContentObject] = relationship(back_populates="advertisements")
    advertiser_virtual_node: Mapped["LocalVirtualNodeIdentity"] = relationship(
        back_populates="content_advertisements"
    )


class ContentReplica(IntegerPrimaryKeyMixin, Base):
    __tablename__ = "content_replica"

    content_object_id: Mapped[int] = mapped_column(
        ForeignKey("content_object.id"),
        nullable=False,
        index=True,
    )
    retention_policy: Mapped[str | None] = mapped_column(String(100), nullable=True)
    is_pinned: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_verified_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    content_object: Mapped[ContentObject] = relationship(back_populates="replicas")
