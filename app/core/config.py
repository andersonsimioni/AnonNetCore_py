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


def _read_bool_env(name: str, default: bool) -> bool:
    raw_value = os.getenv(name)
    if raw_value is None:
        return default

    normalized_value = raw_value.strip().lower()
    if normalized_value in {"1", "true", "yes", "y", "on"}:
        return True
    if normalized_value in {"0", "false", "no", "n", "off"}:
        return False
    return default


@dataclass(slots=True)
class CoreConfig:
    node_reachability: str = os.getenv("ANONNET_NODE_REACHABILITY", "public")

    tcp_transport_enabled: bool = _read_bool_env("ANONNET_TCP_TRANSPORT_ENABLED", True)
    udp_transport_enabled: bool = _read_bool_env("ANONNET_UDP_TRANSPORT_ENABLED", True)
    physical_relay_enabled: bool = _read_bool_env("ANONNET_RELAY_SERVICE_ENABLED", True)

    physical_listen_host: str = "0.0.0.0"
    physical_tcp_listen_port: int = 19001
    physical_udp_listen_port: int = 19002
    physical_tcp_backlog: int = 100
    physical_tcp_idle_timeout_seconds: int = 60

    udp_max_datagram_size: int = 1200
    udp_keepalive_interval_seconds: float = 1.0
    udp_fragment_payload_size: int = 500
    udp_fragment_send_delay_seconds: float = 0.005
    udp_fragment_reassembly_timeout_seconds: float = 15.0

    log_dir: str | Path = "data/local/logs"
    runtime_stop_timeout_seconds: float = 3.0

    bootstrap_dns_seeds: list[DnsSeed] = field(default_factory=list)
    bootstrap_public_endpoints: list[BootstrapEndpoint] = field(
        default_factory=build_default_bootstrap_public_endpoints
    )
    bootstrap_request_retries: int = 3
    bootstrap_request_delay_seconds: float = 1.0

    physical_ping_timeout_seconds: float = 2.0
    physical_ping_runtime_interval_seconds: float = 10.0
    physical_ping_runtime_candidate_limit: int = 4

    random_walk_candidate_limit: int = 32
    random_walk_ttl_acceptance_error_ms: int = 1_000
    random_walk_previous_hop_fallback_rtt_ms: float = 40.0

    virtual_route_maintenance_interval_seconds: float = 5.0
    virtual_route_min_published_routes: int = 5
    virtual_route_build_timeout_seconds: float = 90.0
    route_create_ok_drt_visibility_timeout_seconds: float = 45.0
    route_create_ok_drt_visibility_retry_seconds: float = 1.0
    default_random_walk_ttl_ms: int = 500

    network_pow_difficulty_bits: int = 8

    session_keepalive_seconds: int = 20
    session_runtime_interval_seconds: float = 2.0
    physical_reliable_retry_seconds: float = 2.0
    virtual_reliable_retry_fallback_seconds: float = 5.0
    virtual_reliable_retry_rtt_multiplier: float = 2.0
    virtual_reliable_retry_min_seconds: float = 2.0
    virtual_reliable_retry_max_seconds: float = 30.0
    virtual_session_timeout_min_seconds: float = 60.0
    virtual_session_timeout_rtt_multiplier: float = 6.0
    reliable_delivery_max_attempts: int = 5
    physical_session_handshake_timeout_seconds: float = 12.0
    virtual_session_handshake_timeout_seconds: float = 60.0
    session_handshake_poll_interval_seconds: float = 0.25

    physical_node_validation_runtime_interval_seconds: float = 3.0
    physical_node_validation_backoff_seconds: int = 300
    physical_node_endpoint_failure_threshold: int = 3

    physical_node_info_exchange_interval_seconds: int = 45
    physical_node_info_exchange_runtime_interval_seconds: float = 2.0
    physical_node_info_exchange_max_records: int = 32

    physical_relay_maintenance_interval_seconds: float = 5.0
    physical_relay_candidate_limit: int = 8

    dht_replication_factor: int = 3
    dht_maintenance_interval_seconds: float = 5.0
    dht_republish_interval_seconds: float = 120.0
    dht_request_timeout_seconds: float = 60.0
    dht_request_max_forward_hops: int = 60

    virtual_session_drt_lookup_timeout_seconds: float = 20.0
    virtual_session_drt_lookup_retry_seconds: float = 1.0

    api_enabled: bool = True
    api_host: str = "127.0.0.1"
    api_port: int = 18080
    api_cors_allow_origin: str = "*"
    debug_api_enabled: bool = True
    api_websocket_enabled: bool = True
    api_websocket_port: int = 18081
    api_websocket_path: str = "/v1/events"

    content_storage_dir: str | Path = "data/local/content"
    content_download_range_size: int = 4 * 1024
    content_provider_record_ttl_seconds: int = 30 * 24 * 60 * 60
    content_provider_publish_retry_attempts: int = 4
    content_provider_publish_retry_delay_seconds: float = 2.0
