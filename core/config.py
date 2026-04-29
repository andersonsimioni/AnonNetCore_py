from __future__ import annotations

from dataclasses import dataclass


@dataclass(slots=True)
class CoreConfig:
    physical_ping_timeout_seconds: float = 5.0
    physical_ping_runtime_interval_seconds: float = 25.0
    physical_ping_runtime_candidate_limit: int = 10
    random_walk_ttl_route_candidate_limit: int = 32
    route_pow_difficulty_bits: int = 16
    physical_session_keepalive_seconds: int = 30
    physical_session_runtime_interval_seconds: float = 1.0
    physical_session_handshake_timeout_seconds: float = 6.0
    physical_session_handshake_poll_interval_seconds: float = 0.25
    physical_node_validation_runtime_interval_seconds: float = 8.0
    physical_node_validation_backoff_seconds: int = 45
    physical_node_info_exchange_interval_seconds: int = 180
    physical_node_info_exchange_runtime_interval_seconds: float = 20.0
    physical_node_info_exchange_max_records: int = 50
    dht_replication_factor: int = 8
    dht_maintenance_runtime_interval_seconds: float = 15.0
    dht_client_response_timeout_seconds: float = 8.0
    dht_client_max_hops: int = 8
