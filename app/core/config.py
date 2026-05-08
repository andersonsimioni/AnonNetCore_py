from __future__ import annotations

from dataclasses import dataclass, field

from bootstrap.models import BootstrapEndpoint, DnsSeed


@dataclass(slots=True)
class CoreConfig:
    listen_host: str = "0.0.0.0"
    listen_port: int = 19001
    bootstrap_dns_seeds: list[DnsSeed] = field(default_factory=list)
    bootstrap_public_endpoints: list[BootstrapEndpoint] = field(
        default_factory=lambda: [
            BootstrapEndpoint(
                host="192.168.1.19",
                port=19001,
                source="core_config_bootstrap",
            ),
            BootstrapEndpoint(
                host="192.168.1.19",
                port=19002,
                source="core_config_bootstrap",
            ),
        ]
    )
    bootstrap_warmup_seconds: float = 2.0
    bootstrap_request_retries: int = 5
    bootstrap_request_delay_seconds: float = 5
    physical_ping_timeout_seconds: float = 5.0
    physical_ping_runtime_interval_seconds: float = 10
    physical_ping_runtime_candidate_limit: int = 10
    random_walk_ttl_route_candidate_limit: int = 32
    route_pow_difficulty_bits: int = 16
    physical_session_keepalive_seconds: int = 30
    physical_session_runtime_interval_seconds: float = 1.0
    physical_session_handshake_timeout_seconds: float = 6.0
    physical_session_handshake_poll_interval_seconds: float = 0.25
    physical_node_validation_runtime_interval_seconds: float = 1.0
    physical_node_validation_backoff_seconds: int = 45
    physical_node_info_exchange_interval_seconds: int = 1
    physical_node_info_exchange_runtime_interval_seconds: float = 1.0
    physical_node_info_exchange_max_records: int = 50
    dht_replication_factor: int = 8
    dht_maintenance_runtime_interval_seconds: float = 10
    dht_client_response_timeout_seconds: float = 8.0
    dht_client_max_hops: int = 8
