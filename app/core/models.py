from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from common import utc_now


@dataclass(slots=True, frozen=True)
class PacketContext:
    """Contexto de recepcao independente do transporte."""

    transport_name: str
    payload: bytes
    remote_host: str | None = None
    remote_port: int | None = None
    local_host: str | None = None
    local_port: int | None = None
    connection_id: str | None = None
    received_at: datetime = field(default_factory=utc_now)
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True, frozen=True)
class ProtocolEnvelope:
    """Representacao normalizada de um pacote identificado pelo core."""

    protocol_name: str
    message_type: str | None
    payload: Any
    raw_payload: bytes
    header: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True, frozen=True)
class PacketProcessingResult:
    """Resultado padrao do processamento de um pacote recebido."""

    protocol_name: str
    handled: bool
    message_type: str | None = None
    response_payload: bytes | None = None
    metadata: dict[str, Any] = field(default_factory=dict)
