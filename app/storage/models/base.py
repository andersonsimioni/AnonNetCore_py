from __future__ import annotations

from datetime import datetime, timezone

try:
    from sqlalchemy import Boolean, DateTime, Integer, String, Text, func
    from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
except ModuleNotFoundError as error:
    raise ModuleNotFoundError(
        "SQLAlchemy nao esta instalado. Instale a dependencia para usar o ORM local."
    ) from error


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


class Base(DeclarativeBase):
    """Base declarativa compartilhada por todos os modelos ORM."""


class TimestampMixin:
    """Campos padrao para auditoria basica em tabelas locais."""

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=utc_now,
        server_default=func.now(),
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=utc_now,
        onupdate=utc_now,
        server_default=func.now(),
    )


class IntegerPrimaryKeyMixin:
    """Chave primaria inteira para entidades puramente locais."""

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)


class StatusMixin:
    """Campo de status textual para entidades com ciclo de vida simples."""

    status: Mapped[str] = mapped_column(String(50), nullable=False)


class MetadataJsonMixin:
    """Armazena metadados flexiveis em JSON serializado como texto."""

    metadata_json: Mapped[str | None] = mapped_column(Text, nullable=True)


class ActiveFlagMixin:
    """Campo padrao para indicar atividade local."""

    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)


class SchemaMetadata(TimestampMixin, Base):
    """Tabela minima para versionamento e metadados da base local."""

    __tablename__ = "schema_metadata"

    key: Mapped[str] = mapped_column(String(100), primary_key=True)
    value: Mapped[str] = mapped_column(String(500), nullable=False)
