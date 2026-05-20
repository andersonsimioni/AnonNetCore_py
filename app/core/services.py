from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from bootstrap import BootstrapService
from content import ContentTransferService
from dht import DhtService
from identity import IdentityService
from log import LogService
from route import RouteService
from sessions import SessionManager
from storage import DatabaseManager, get_database
from transport import TcpTransportAdapter, TransportService

from .config import CoreConfig

if TYPE_CHECKING:
    from api import CoreApiService
    from .engine import CoreEngine
    from .protocol_clients import ProtocolClients
    from .routing_strategies import RouteStrategyRegistry
    from .runtime import RuntimeServices


@dataclass(slots=True)
class EngineServices:
    """Servicos compartilhados pela engine e pelos protocolos."""

    config: CoreConfig = field(default_factory=CoreConfig)
    database: DatabaseManager = field(default_factory=get_database)
    transport: TransportService = field(default_factory=TransportService)
    bootstrap_service: BootstrapService = field(default_factory=BootstrapService)
    content_transfer_service: ContentTransferService = field(default_factory=ContentTransferService)
    dht_service: DhtService = field(default_factory=DhtService)
    identity_service: IdentityService = field(default_factory=IdentityService)
    log_service: LogService = field(default_factory=LogService)
    route_service: RouteService = field(default_factory=RouteService)
    session_manager: SessionManager = field(default_factory=SessionManager)
    route_strategies: RouteStrategyRegistry | None = None
    protocol_clients: ProtocolClients | None = None
    runtime_services: RuntimeServices | None = None
    api_service: CoreApiService | None = None
    engine: CoreEngine | None = None
    crypto_service: Any | None = None
    extra_services: dict[str, Any] = field(default_factory=dict)

    def ensure_defaults(self) -> None:
        if not self.transport.adapters:
            self.transport.register_adapter(TcpTransportAdapter())

    def bind_engine(self, engine: CoreEngine) -> None:
        from .protocol_clients import ProtocolClients
        from .routing_strategies import RouteStrategyRegistry
        from .runtime import RuntimeServices
        from api import CoreApiService

        self.engine = engine
        self.dht_service.config = self.config
        self.identity_service.endpoint_failure_threshold = (
            self.config.physical_node_endpoint_failure_threshold
        )
        self.content_transfer_service.database = self.database
        self.content_transfer_service.configure(
            storage_dir=self.config.content_storage_dir,
            download_range_size=self.config.content_download_range_size,
        )
        self.route_strategies = RouteStrategyRegistry()
        self.protocol_clients = ProtocolClients(engine)
        self.runtime_services = RuntimeServices(engine)
        self.api_service = CoreApiService(engine)

    def get_service(self, name: str) -> Any | None:
        return self.extra_services.get(name)
