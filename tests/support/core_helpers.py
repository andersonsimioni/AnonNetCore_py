from __future__ import annotations

from pathlib import Path
import sys
import shutil
from typing import Iterable


APP_ROOT = Path(__file__).resolve().parents[2] / "app"
if str(APP_ROOT) not in sys.path:
    sys.path.insert(0, str(APP_ROOT))

from bootstrap.models import BootstrapEndpoint, DnsSeed
from core import CoreConfig, CoreEngine, EngineServices
from dht import DhtService
from identity import IdentityService
from route import RouteService
from storage import DatabaseConfig, DatabaseManager
from smokes_config import SMOKES_CONFIG


DEFAULT_CLUSTER_NODES = SMOKES_CONFIG.core_full_flow_cluster_nodes


def create_isolated_core(
    *,
    data_dir: Path,
    listen_port: int,
    listen_host: str = "0.0.0.0",
    node_reachability: str = "public",
    tcp_transport_enabled: bool = True,
    udp_enabled: bool | None = None,
    physical_udp_listen_port: int | None = None,
    log_dir: Path | None = None,
    api_port: int | None = None,
    api_websocket_port: int | None = None,
    virtual_route_expected_round_trip_ttl_ms: int = (
        SMOKES_CONFIG.route_expected_round_trip_ttl_ms(
            DEFAULT_CLUSTER_NODES
        )
    ),
    virtual_route_pending_timeout_seconds: float = (
        SMOKES_CONFIG.route_build_timeout_seconds(DEFAULT_CLUSTER_NODES)
    ),
    route_create_ok_drt_visibility_timeout_seconds: float = (
        SMOKES_CONFIG.route_ok_drt_visibility_timeout_seconds(DEFAULT_CLUSTER_NODES)
    ),
    virtual_route_min_online_routes: int = SMOKES_CONFIG.test_core_route_min_online_routes,
    random_walk_ttl_acceptance_error_ms: int = (
        SMOKES_CONFIG.route_acceptance_error_ms(DEFAULT_CLUSTER_NODES)
    ),
    dht_request_timeout_seconds: float = (
        SMOKES_CONFIG.dht_request_timeout_seconds(DEFAULT_CLUSTER_NODES)
    ),
    physical_ping_timeout_seconds: float = (
        SMOKES_CONFIG.physical_ping_timeout_seconds(DEFAULT_CLUSTER_NODES)
    ),
    virtual_session_drt_lookup_timeout_seconds: float = (
        SMOKES_CONFIG.virtual_session_drt_lookup_timeout_seconds(
            DEFAULT_CLUSTER_NODES
        )
    ),
    physical_session_handshake_timeout_seconds: float = (
        SMOKES_CONFIG.test_core_physical_session_handshake_timeout_seconds
    ),
    virtual_session_handshake_timeout_seconds: float = (
        SMOKES_CONFIG.virtual_session_handshake_timeout_seconds(
            DEFAULT_CLUSTER_NODES
        )
    ),
    bootstrap_public_endpoints: Iterable[BootstrapEndpoint] | None = None,
    bootstrap_dns_seeds: Iterable[DnsSeed] | None = None,
    reset_database: bool = False,
) -> CoreEngine:
    """Cria um CoreEngine com banco proprio para cenarios de integracao."""

    data_dir.mkdir(parents=True, exist_ok=True)
    db_path = data_dir / "anonnetcore.db"
    if reset_database and db_path.exists():
        db_path.unlink()

    database = DatabaseManager(DatabaseConfig(db_path=db_path))
    config = CoreConfig(
        node_reachability=node_reachability,
        tcp_transport_enabled=tcp_transport_enabled,
        physical_listen_host=listen_host,
        physical_tcp_listen_port=listen_port,
        physical_udp_listen_port=physical_udp_listen_port,
        log_dir=log_dir or data_dir / "logs",
    )
    if udp_enabled is not None:
        config.udp_transport_enabled = udp_enabled
    config.api_enabled = False
    if api_port is not None:
        config.api_enabled = True
        config.api_host = "127.0.0.1"
        config.api_port = api_port
    if api_websocket_port is not None:
        config.api_websocket_enabled = True
        config.api_websocket_port = api_websocket_port
    else:
        config.api_websocket_enabled = False
    config.content_storage_dir = data_dir / "content"
    config.virtual_route_maintenance_interval_seconds = (
        SMOKES_CONFIG.test_core_route_runtime_interval_seconds
    )
    config.virtual_route_min_published_routes = max(1, virtual_route_min_online_routes)
    config.virtual_route_build_timeout_seconds = virtual_route_pending_timeout_seconds
    config.default_random_walk_ttl_ms = virtual_route_expected_round_trip_ttl_ms
    config.route_create_ok_drt_visibility_timeout_seconds = (
        route_create_ok_drt_visibility_timeout_seconds
    )
    config.route_create_ok_drt_visibility_retry_seconds = (
        SMOKES_CONFIG.test_core_route_ok_drt_visibility_retry_seconds
    )
    config.random_walk_ttl_acceptance_error_ms = random_walk_ttl_acceptance_error_ms
    config.random_walk_candidate_limit = SMOKES_CONFIG.test_core_route_candidate_limit
    config.dht_request_timeout_seconds = dht_request_timeout_seconds
    config.dht_maintenance_interval_seconds = (
        SMOKES_CONFIG.test_core_dht_maintenance_interval_seconds
    )
    config.dht_republish_interval_seconds = (
        SMOKES_CONFIG.test_core_dht_republish_interval_seconds
    )
    config.physical_ping_timeout_seconds = physical_ping_timeout_seconds
    config.virtual_session_drt_lookup_timeout_seconds = (
        virtual_session_drt_lookup_timeout_seconds
    )
    config.physical_session_handshake_timeout_seconds = (
        physical_session_handshake_timeout_seconds
    )
    config.virtual_session_handshake_timeout_seconds = (
        virtual_session_handshake_timeout_seconds
    )
    config.physical_node_validation_runtime_interval_seconds = (
        SMOKES_CONFIG.test_core_physical_node_validation_interval_seconds
    )
    config.physical_node_info_exchange_interval_seconds = (
        SMOKES_CONFIG.test_core_physical_node_info_exchange_interval_seconds
    )
    config.physical_node_info_exchange_runtime_interval_seconds = (
        SMOKES_CONFIG.test_core_physical_node_info_exchange_runtime_interval_seconds
    )
    if bootstrap_public_endpoints is not None:
        config.bootstrap_public_endpoints = list(bootstrap_public_endpoints)
    if bootstrap_dns_seeds is not None:
        config.bootstrap_dns_seeds = list(bootstrap_dns_seeds)

    services = EngineServices(
        config=config,
        database=database,
        dht_service=DhtService(config=config, database=database),
        identity_service=IdentityService(database=database),
        route_service=RouteService(database=database),
    )
    return CoreEngine(services=services)


def reset_core_data_dir(data_dir: Path) -> None:
    """Remove dados anteriores de um core de teste."""

    if data_dir.exists():
        shutil.rmtree(data_dir)
    data_dir.mkdir(parents=True, exist_ok=True)


async def stop_cores(*engines: CoreEngine) -> None:
    """Stops engines while ignoring cleanup errors so the real test error stays visible."""

    for engine in engines:
        try:
            await engine.stop()
        except Exception:
            pass
