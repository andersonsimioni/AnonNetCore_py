from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass


@dataclass(slots=True, frozen=True)
class VirtualSessionMessage:
    session_id: str
    local_virtual_node_id: str
    remote_virtual_node_id: str
    app_message_type: str
    payload: dict[str, object]
    route_path_id: str | None = None
    request_id: str | None = None


@dataclass(slots=True, frozen=True)
class VirtualSessionMessageReply:
    app_message_type: str
    payload: dict[str, object]
    request_id: str | None = None


VirtualSessionMessageHandler = Callable[
    [VirtualSessionMessage],
    VirtualSessionMessageReply | Awaitable[VirtualSessionMessageReply | None] | None,
]
