from __future__ import annotations

from dataclasses import dataclass
import math


@dataclass(slots=True, frozen=True)
class SmokesConfig:
    """Central defaults used by integration smokes.

    Runtime-only values such as generated run ids may still be created by each
    smoke, but fixed timings, ports, load sizes, and thresholds live here.
    """

    min_cluster_nodes: int = 8
    network_pow_difficulty_bits: int = 8
    ready_cluster_ratio: float = 0.3
    network_ready_base_timeout_seconds: float = 90.0
    network_ready_seconds_per_node: float = 8.0
    forced_exchange_interval_seconds: float = 8.0
    cluster_network_maturity_base_seconds: float = 10.0
    cluster_network_maturity_seconds_per_node: float = 0.5
    cluster_network_maturity_tick_seconds: float = 1.0
    cluster_network_maturity_stable_ticks: int = 2
    cluster_container_base_timeout_seconds: float = 45.0
    cluster_container_seconds_per_node: float = 5.0
    cluster_container_poll_seconds: float = 1.0
    cluster_reachability_base_timeout_seconds: float = 20.0
    cluster_reachability_seconds_per_node: float = 1.5
    cluster_reachability_poll_seconds: float = 1.0

    test_core_route_expected_round_trip_base_ttl_ms: int = 50
    test_core_route_expected_round_trip_ttl_ms_per_node: int = 5
    test_core_route_pending_base_timeout_seconds: float = 18.0
    test_core_route_pending_seconds_per_node: float = 1.0
    test_core_route_pending_expected_ttl_multiplier: float = 4.0
    test_core_route_min_online_routes: int = 2
    test_core_route_max_pending_before_first_route: int = 2
    test_core_route_acceptance_error_base_ms: int = 20_000
    test_core_route_acceptance_error_ms_per_node: int = 2_000
    test_core_route_runtime_interval_seconds: float = 1.0
    test_core_route_drt_check_interval_seconds: float = 1.0
    test_core_route_candidate_limit: int = 16
    test_core_route_ok_drt_visibility_base_timeout_seconds: float = 6.0
    test_core_route_ok_drt_visibility_seconds_per_node: float = 0.6
    test_core_route_ok_drt_visibility_retry_seconds: float = 1.0
    test_core_virtual_session_drt_lookup_timeout_seconds: float = 45.0
    test_core_session_handshake_timeout_seconds: float = 60.0
    test_core_physical_node_validation_interval_seconds: float = 0.5
    test_core_physical_ping_base_timeout_seconds: float = 4.0
    test_core_physical_ping_seconds_per_node: float = 0.25
    test_core_physical_node_info_exchange_interval_seconds: int = 8
    test_core_physical_node_info_exchange_runtime_interval_seconds: float = 0.5

    route_active_base_timeout_seconds: float = 90.0
    route_active_seconds_per_node: float = 14.0
    test_core_dht_request_base_timeout_seconds: float = 12.0
    test_core_dht_request_seconds_per_node: float = 1.0
    test_core_dht_maintenance_interval_seconds: float = 1.0
    test_core_dht_republish_interval_seconds: float = 6.0
    drt_entry_base_timeout_seconds: float = 20.0
    drt_entry_seconds_per_node: float = 2.0
    drt_online_route_count_base_timeout_seconds: float = 90.0
    drt_online_route_count_seconds_per_node: float = 6.0
    drt_stable_reads: int = 2
    drt_stable_single_read_timeout_seconds: float = 6.0
    virtual_session_active_base_timeout_seconds: float = 10.0
    virtual_session_active_seconds_per_node: float = 0.75
    virtual_session_handshake_base_timeout_seconds: float = 60.0
    virtual_session_handshake_seconds_per_node: float = 6.0
    virtual_session_handshake_route_build_multiplier: float = 6.0
    virtual_session_drt_lookup_base_timeout_seconds: float = 45.0
    virtual_session_drt_lookup_seconds_per_node: float = 4.0
    virtual_keepalive_ack_base_timeout_seconds: float = 25.0
    virtual_keepalive_ack_seconds_per_node: float = 1.0
    virtual_keepalive_ack_route_build_multiplier: float = 2.0
    virtual_message_base_timeout_seconds: float = 12.0
    virtual_message_seconds_per_node: float = 0.5
    virtual_message_route_build_multiplier: float = 2.0
    generic_wait_poll_seconds: float = 0.25
    network_ready_poll_seconds: float = 1.0
    stable_drt_poll_seconds: float = 1.0
    route_candidate_ping_limit: int = 32
    route_candidate_query_limit: int = 32
    route_candidate_rtt_concurrency: int = 8
    node_info_exchange_candidate_limit: int = 16

    core_full_flow_cluster_nodes: int = 5
    core_full_flow_core_a_port: int = 19201
    core_full_flow_core_b_port: int = 19202

    virtual_session_core_a_port: int = 19101
    virtual_session_core_b_port: int = 19102

    virtual_message_core_a_port: int = 19301
    virtual_message_core_b_port: int = 19302

    virtual_content_core_a_port: int = 19401
    virtual_content_core_b_port: int = 19402

    debug_cluster_nodes: int = 4
    debug_startup_timeout_seconds: float = 90.0
    debug_stabilization_seconds: float = 20.0
    debug_poll_seconds: float = 1.0
    debug_node_timeout_seconds: float = 3.0
    debug_max_active_sessions_per_node: int = 12
    debug_max_total_sessions_per_node: int = 20

    physical_relay_relay_port: int = 19301
    physical_relay_requester_port: int = 19302
    physical_relay_private_node_port: int = 19303
    physical_relay_short_poll_seconds: float = 0.1
    physical_relay_medium_poll_seconds: float = 0.5
    physical_relay_registration_poll_seconds: float = 1.0

    physical_udp_core_a_tcp_port: int = 19801
    physical_udp_core_b_tcp_port: int = 19802
    physical_udp_core_a_udp_port: int = 29801
    physical_udp_core_b_udp_port: int = 29802
    physical_udp_chunk_payload_size: int = 384
    physical_udp_seed: int = 517_009
    physical_udp_max_stress_payload_size: int = 384_000
    physical_udp_datagram_size: int = 1200
    physical_udp_reassembly_timeout_seconds: float = 20.0
    physical_udp_max_frame_size: int = 2 * 1024 * 1024
    physical_udp_dpnt_timeout_seconds: float = 35.0
    physical_udp_session_timeout_seconds: float = 10.0
    physical_udp_reliable_ack_timeout_seconds: float = 45.0
    physical_udp_keepalive_ack_timeout_seconds: float = 6.0
    physical_udp_concurrent_batch_sizes: tuple[int, ...] = (
        4096,
        8192,
        16_384,
        32_768,
        65_536,
        131_072,
    )
    physical_udp_boundary_payload_sizes: tuple[int, ...] = (
        1,
        128,
        1199,
        1200,
        1201,
        1300,
        2048,
        4096,
        8192,
        16_384,
        32_768,
        65_536,
        131_072,
    )
    physical_udp_random_payload_min_size: int = 1500
    physical_udp_random_payload_count: int = 16

    reliable_seed: int = 742_931
    reliable_short_timeout_seconds: float = 5.0
    reliable_default_max_attempts: int = 5
    reliable_real_sender_port: int = 19701
    reliable_real_receiver_port: int = 19702
    reliable_route_service_rtt_ms: float = 9000.0
    reliable_metadata_route_rtt_ms: int = 6000
    reliable_direct_route_rtt_ms: int = 50
    reliable_random_message_count: int = 40
    reliable_physical_retry_seconds: float = 2.0
    reliable_virtual_retry_fallback_seconds: float = 5.0
    reliable_virtual_retry_rtt_multiplier: float = 2.0
    reliable_virtual_retry_min_seconds: float = 2.0
    reliable_virtual_retry_max_seconds: float = 30.0

    virtual_api_core_a_port: int = 19501
    virtual_api_core_b_port: int = 19502
    virtual_api_local_core_port_pool_start: int = 19501
    virtual_api_local_core_port_pool_stop: int = 19541
    virtual_api_route_inventory_timeout_seconds: float = 180.0
    virtual_api_route_expected_round_trip_ttl_ms: int = 30000
    virtual_api_route_pending_timeout_seconds: float = 30.0
    virtual_api_min_online_routes: int = 1
    virtual_api_message_rounds: int = 12
    virtual_api_content_downloads: int = 3
    virtual_api_seed: int = 20260521
    virtual_api_client_timeout_seconds: float = 90.0
    virtual_api_step_reset_cluster_timeout_seconds: float = 45.0
    virtual_api_step_start_cluster_timeout_seconds: float = 120.0
    virtual_api_step_wait_cluster_timeout_seconds: float = 90.0
    virtual_api_step_start_core_timeout_seconds: float = 30.0
    virtual_api_step_reachability_timeout_seconds: float = 30.0
    virtual_api_step_create_nodes_timeout_seconds: float = 20.0
    virtual_api_step_network_ready_timeout_seconds: float = 120.0
    virtual_api_step_maturity_timeout_seconds: float = 75.0
    virtual_api_step_route_inventory_extra_seconds: float = 8.0
    virtual_api_step_identity_timeout_seconds: float = 20.0
    virtual_api_step_session_timeout_seconds: float = 60.0
    virtual_api_step_subscribe_timeout_seconds: float = 20.0
    virtual_api_message_timeout_min_seconds: float = 35.0
    virtual_api_message_timeout_per_round_seconds: float = 20.0
    virtual_api_content_timeout_min_seconds: float = 60.0
    virtual_api_content_timeout_per_download_seconds: float = 45.0
    virtual_api_port_probe_poll_seconds: float = 0.5
    virtual_api_poll_message_timeout_seconds: float = 20.0
    virtual_api_publish_job_timeout_seconds: float = 35.0
    virtual_api_download_timeout_seconds: float = 35.0
    virtual_api_default_client_timeout_seconds: float = 35.0

    virtual_api_content_min_bytes: int = 8192
    virtual_api_content_max_bytes: int = 128 * 1024

    virtual_api_local_core_port: int = 19511
    virtual_api_local_route_inventory_timeout_seconds: float = 90.0
    virtual_api_local_route_pending_timeout_seconds: float = 30.0
    virtual_api_local_message_rounds: int = 80
    virtual_api_local_message_concurrency: int = 12
    virtual_api_local_content_downloads: int = 4
    virtual_api_local_seed: int = 2026052102
    virtual_api_local_session_timeout_seconds: float = 75.0
    virtual_api_local_message_timeout_min_seconds: float = 45.0
    virtual_api_local_message_timeout_per_round_seconds: float = 5.0
    virtual_api_local_content_timeout_min_seconds: float = 75.0
    virtual_api_local_content_timeout_per_download_seconds: float = 45.0
    virtual_api_local_wait_messages_min_seconds: float = 35.0
    virtual_api_local_wait_messages_per_round_seconds: float = 1.5

    virtual_api_local_content_min_bytes: int = 4096
    virtual_api_local_content_max_bytes: int = 192 * 1024
    virtual_api_local_message_progress_interval: int = 10

    virtual_content_info_timeout_seconds: float = 90.0
    virtual_content_download_timeout_seconds: float = 60.0
    virtual_content_line_repetitions: int = 4096

    social_cluster_nodes: int = 8
    social_core_listen_port: int = 19601
    social_api_port: int = 18180
    social_websocket_port: int = 18181
    social_route_active_base_timeout_seconds: float = 90.0
    social_route_active_seconds_per_node: float = 8.0
    social_route_active_retry_cycles: int = 3

    def required_ready_nodes(self, cluster_nodes: int, minimum_remote_nodes: int | None) -> int:
        if minimum_remote_nodes is not None:
            return max(1, minimum_remote_nodes)
        return max(1, math.ceil(cluster_nodes * self.ready_cluster_ratio))

    def cluster_container_timeout_seconds(self, cluster_nodes: int) -> float:
        return self.cluster_container_base_timeout_seconds + (
            cluster_nodes * self.cluster_container_seconds_per_node
        )

    def cluster_reachability_timeout_seconds(self, cluster_nodes: int) -> float:
        return self.cluster_reachability_base_timeout_seconds + (
            cluster_nodes * self.cluster_reachability_seconds_per_node
        )

    def network_ready_timeout_seconds(self, cluster_nodes: int) -> float:
        return self.network_ready_base_timeout_seconds + (
            cluster_nodes * self.network_ready_seconds_per_node
        )

    def physical_ping_timeout_seconds(self, cluster_nodes: int) -> float:
        return self.test_core_physical_ping_base_timeout_seconds + (
            cluster_nodes * self.test_core_physical_ping_seconds_per_node
        )

    def cluster_network_maturity_seconds(self, cluster_nodes: int) -> float:
        return self.cluster_network_maturity_base_seconds + (
            cluster_nodes * self.cluster_network_maturity_seconds_per_node
        )

    def route_build_timeout_seconds(self, cluster_nodes: int) -> float:
        return (
            self.test_core_route_pending_base_timeout_seconds
            + (cluster_nodes * self.test_core_route_pending_seconds_per_node)
            + (
                self.route_expected_round_trip_ttl_ms(cluster_nodes)
                / 1000
                * self.test_core_route_pending_expected_ttl_multiplier
            )
        )

    def route_ok_drt_visibility_timeout_seconds(self, cluster_nodes: int) -> float:
        return self.test_core_route_ok_drt_visibility_base_timeout_seconds + (
            cluster_nodes * self.test_core_route_ok_drt_visibility_seconds_per_node
        )

    def route_max_pending_before_first_route(self, cluster_nodes: int) -> int:
        scaled_limit = math.ceil(cluster_nodes * 0.25)
        return max(
            self.test_core_route_max_pending_before_first_route,
            min(3, scaled_limit),
        )

    def route_expected_round_trip_ttl_ms(self, cluster_nodes: int) -> int:
        return self.test_core_route_expected_round_trip_base_ttl_ms + (
            cluster_nodes * self.test_core_route_expected_round_trip_ttl_ms_per_node
        )

    def route_acceptance_error_ms(self, cluster_nodes: int) -> int:
        return self.test_core_route_acceptance_error_base_ms + (
            cluster_nodes * self.test_core_route_acceptance_error_ms_per_node
        )

    def route_active_timeout_seconds(self, cluster_nodes: int) -> float:
        return self.route_active_base_timeout_seconds + (
            cluster_nodes * self.route_active_seconds_per_node
        )

    def dht_request_timeout_seconds(self, cluster_nodes: int) -> float:
        return self.test_core_dht_request_base_timeout_seconds + (
            cluster_nodes * self.test_core_dht_request_seconds_per_node
        )

    def drt_entry_timeout_seconds(self, cluster_nodes: int) -> float:
        return self.drt_entry_base_timeout_seconds + (
            cluster_nodes * self.drt_entry_seconds_per_node
        )

    def drt_online_route_count_timeout_seconds(self, cluster_nodes: int) -> float:
        return self.drt_online_route_count_base_timeout_seconds + (
            cluster_nodes * self.drt_online_route_count_seconds_per_node
        )

    def virtual_session_active_timeout_seconds(self, cluster_nodes: int) -> float:
        return self.virtual_session_active_base_timeout_seconds + (
            cluster_nodes * self.virtual_session_active_seconds_per_node
        )

    def virtual_session_handshake_timeout_seconds(self, cluster_nodes: int) -> float:
        route_round_trip_seconds = self.route_expected_round_trip_ttl_ms(cluster_nodes) / 1000
        base_timeout = (
            self.virtual_session_handshake_base_timeout_seconds
            + (cluster_nodes * self.virtual_session_handshake_seconds_per_node)
            + (route_round_trip_seconds * 4)
        )
        route_pipeline_timeout = (
            self.route_build_timeout_seconds(cluster_nodes)
            * self.virtual_session_handshake_route_build_multiplier
        )
        return max(base_timeout, route_pipeline_timeout)

    def virtual_session_drt_lookup_timeout_seconds(self, cluster_nodes: int) -> float:
        return self.virtual_session_drt_lookup_base_timeout_seconds + (
            cluster_nodes * self.virtual_session_drt_lookup_seconds_per_node
        )

    def virtual_keepalive_ack_timeout_seconds(self, cluster_nodes: int) -> float:
        base_timeout = self.virtual_keepalive_ack_base_timeout_seconds + (
            cluster_nodes * self.virtual_keepalive_ack_seconds_per_node
        )
        route_pipeline_timeout = (
            self.route_build_timeout_seconds(cluster_nodes)
            * self.virtual_keepalive_ack_route_build_multiplier
        )
        return max(base_timeout, route_pipeline_timeout)

    def virtual_message_timeout_seconds(self, cluster_nodes: int) -> float:
        base_timeout = (
            self.virtual_message_base_timeout_seconds
            + (cluster_nodes * self.virtual_message_seconds_per_node)
            + (self.route_expected_round_trip_ttl_ms(cluster_nodes) / 1000)
        )
        route_pipeline_timeout = (
            self.route_build_timeout_seconds(cluster_nodes)
            * self.virtual_message_route_build_multiplier
        )
        return max(base_timeout, route_pipeline_timeout)

    def virtual_content_transfer_timeout_seconds(self, cluster_nodes: int) -> float:
        return (
            self.virtual_message_timeout_seconds(cluster_nodes)
            + self.drt_entry_timeout_seconds(cluster_nodes)
            + self.virtual_content_info_timeout_seconds
            + self.virtual_content_download_timeout_seconds
        )

    def social_route_active_timeout_seconds(self, cluster_nodes: int) -> float:
        return (
            self.social_route_active_base_timeout_seconds
            + (cluster_nodes * self.social_route_active_seconds_per_node)
            + (
                self.route_build_timeout_seconds(cluster_nodes)
                * self.social_route_active_retry_cycles
            )
            + (self.route_expected_round_trip_ttl_ms(cluster_nodes) / 1000)
        )


SMOKES_CONFIG = SmokesConfig()
