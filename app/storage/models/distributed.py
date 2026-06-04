from __future__ import annotations

from datetime import datetime

try:
    from sqlalchemy import DateTime, Index, Integer, String, Text
    from sqlalchemy.orm import Mapped, mapped_column
except ModuleNotFoundError as error:
    raise ModuleNotFoundError(
        "SQLAlchemy is not installed. Install the dependency to use the local ORM."
    ) from error

from .base import Base, TimestampMixin

class DhtRecord(TimestampMixin, Base):
    """
    Registro generico da DHT para qualquer namespace.

    A chave primaria e derivada por:
    SHA512(namespace || logical_key)
    """

    __tablename__ = "dht_record"
    __table_args__ = (
        Index("ix_dht_record_key", "key"),
        Index("ix_dht_record_namespace", "namespace"),
        Index("ix_dht_record_namespace_logical_key", "namespace", "logical_key"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    key: Mapped[str] = mapped_column(String(128), nullable=False)
    namespace: Mapped[str] = mapped_column(String(50), nullable=False)
    logical_key: Mapped[str] = mapped_column(String(255), nullable=False)
    record_json: Mapped[str] = mapped_column(Text, nullable=False)
    source: Mapped[str | None] = mapped_column(String(100), nullable=True)
    last_validated_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
