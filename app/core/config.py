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


def _read_int_env(name: str, default: int) -> int:
    raw_value = os.getenv(name)
    if raw_value is None:
        return default
    try:
        return int(raw_value)
    except ValueError:
        return default


@dataclass(slots=True)
class CoreConfig:
    node_reachability: str = "public"

    tcp_transport_enabled: bool = True
    udp_transport_enabled: bool = True
    relay_service_enabled: bool = True

    physical_listen_host: str = "0.0.0.0"
    physical_tcp_listen_port: int = 19001
    physical_udp_listen_port: int | None = None

    udp_max_datagram_size: int = 1200
    udp_chunk_payload_size: int = 512
    udp_max_frame_size: int = 1024 * 1024
    udp_reassembly_timeout_seconds: float = 10.0

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
    random_walk_ttl_acceptance_error_ms: int = 1000
    random_walk_previous_hop_fallback_rtt_ms: float = 40.0

    virtual_route_maintenance_interval_seconds: float = 5.0
    virtual_route_min_published_routes: int = 5
    virtual_route_build_timeout_seconds: float = 45.0
    route_create_ok_drt_visibility_timeout_seconds: float = 10.0
    route_create_ok_drt_visibility_retry_seconds: float = 1.0
    virtual_route_max_pending_builds_before_first_route: int = 2
    default_random_walk_ttl_ms: int = 500

    network_pow_difficulty_bits: int = _read_int_env("ANONNET_NETWORK_POW_DIFFICULTY_BITS", 16)

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
    session_handshake_timeout_seconds: float = 20.0
    session_handshake_poll_interval_seconds: float = 0.25

    physical_node_validation_runtime_interval_seconds: float = 3.0
    physical_node_validation_backoff_seconds: int = 300
    physical_node_endpoint_failure_threshold: int = 3

    physical_node_info_exchange_interval_seconds: int = 45
    physical_node_info_exchange_runtime_interval_seconds: float = 2.0
    physical_node_info_exchange_max_records: int = 32

    physical_relay_challenge_ttl_seconds: int = 60
    physical_relay_registration_ttl_seconds: int = 30 * 60
    physical_relay_channel_ttl_seconds: int = 10 * 60

    dht_replication_factor: int = 3
    dht_maintenance_interval_seconds: float = 5.0
    dht_republish_interval_seconds: float = 120.0
    dht_request_timeout_seconds: float = 5.0
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
    content_download_range_size: int = 64 * 1024
    content_provider_record_ttl_seconds: int = 30 * 24 * 60 * 60
