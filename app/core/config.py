from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

from bootstrap.models import BootstrapEndpoint, DnsSeed
from .network import detect_local_network_host


def build_default_bootstrap_public_endpoints() -> list[BootstrapEndpoint]:
    bootstrap_host = (
        os.getenv("ANONNET_BOOTSTRAP_HOST")
        or os.getenv("ANONNET_ADVERTISED_TCP_HOST")
        or detect_local_network_host()
    )
    return [
        BootstrapEndpoint(
            host=bootstrap_host,
            port=19001,
            source="core_config_bootstrap",
        ),
        BootstrapEndpoint(
            host=bootstrap_host,
            port=19002,
            source="core_config_bootstrap",
        ),
    ]


@dataclass(slots=True)
class CoreConfig:
    listen_host: str = "0.0.0.0"
    listen_port: int = 19001
    log_dir: str | Path = "data/local/logs"
    bootstrap_dns_seeds: list[DnsSeed] = field(default_factory=list)
    bootstrap_public_endpoints: list[BootstrapEndpoint] = field(
        default_factory=build_default_bootstrap_public_endpoints
    )
    bootstrap_warmup_seconds: float = 1.0
    bootstrap_request_retries: int = 4
    bootstrap_request_delay_seconds: float = 3.0
    physical_ping_timeout_seconds: float = 3.0
    physical_ping_runtime_interval_seconds: float = 30.0
    physical_ping_runtime_candidate_limit: int = 4
    random_walk_ttl_route_candidate_limit: int = 32
    random_walk_ttl_route_error_ms: int = 1000
    random_walk_ttl_previous_hop_fallback_rtt_ms: float = 40.0
    virtual_route_maintenance_runtime_interval_seconds: float = 30.0
    virtual_route_maintenance_route_min_online_routes: int = 5
    virtual_route_maintenance_drt_check_interval_seconds: float = 60.0
    virtual_route_maintenance_pending_route_timeout_seconds: float = 90.0
    virtual_route_maintenance_expected_round_trip_ttl_ms: int = 1000
    virtual_route_maintenance_candidate_limit: int = 16
    route_pow_difficulty_bits: int = 16
    physical_session_keepalive_seconds: int = 45
    physical_session_runtime_interval_seconds: float = 2.0
    physical_session_reliable_retry_after_seconds: float = 2.0
    virtual_session_reliable_retry_fallback_seconds: float = 5.0
    virtual_session_reliable_retry_rtt_multiplier: float = 2.0
    virtual_session_reliable_retry_min_seconds: float = 2.0
    virtual_session_reliable_retry_max_seconds: float = 30.0
    session_reliable_max_attempts: int = 5
    physical_session_handshake_timeout_seconds: float = 15.0
    physical_session_handshake_poll_interval_seconds: float = 0.25
    physical_node_validation_runtime_interval_seconds: float = 3.0
    physical_node_validation_backoff_seconds: int = 300
    physical_node_endpoint_failure_threshold: int = 3
    physical_node_info_exchange_interval_seconds: int = 120
    physical_node_info_exchange_runtime_interval_seconds: float = 5.0
    physical_node_info_exchange_max_records: int = 32
    dht_replication_factor: int = 3
    dht_maintenance_runtime_interval_seconds: float = 5.0
    dht_maintenance_publish_backoff_seconds: float = 600.0
    dht_client_response_timeout_seconds: float = 8.0
    dht_client_max_hops: int = 60
    virtual_session_drt_lookup_timeout_seconds: float = 45.0
    virtual_session_drt_lookup_retry_seconds: float = 2.0
    api_enabled: bool = True
    api_host: str = "127.0.0.1"
    api_port: int = 18080
    api_cors_allow_origin: str = "*"
    debug_api_enabled: bool = True
    api_websocket_enabled: bool = True
    api_websocket_host: str = "127.0.0.1"
    api_websocket_port: int = 18081
    api_websocket_path: str = "/v1/events"
    content_storage_dir: str | Path = "data/local/content"
    content_download_range_size: int = 64 * 1024
    content_provider_advertisement_ttl_seconds: int = 30 * 24 * 60 * 60
