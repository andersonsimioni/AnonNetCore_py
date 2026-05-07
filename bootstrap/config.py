from __future__ import annotations

from dataclasses import dataclass, field

from .models import BootstrapEndpoint, DnsSeed


BOOTSTRAP_DNS_SEEDS: list[DnsSeed] = []


BOOTSTRAP_PUBLIC_ENDPOINTS: list[BootstrapEndpoint] = [
    BootstrapEndpoint(host="node-001", port=19001, source="static_cluster_bootstrap"),
    BootstrapEndpoint(host="node-002", port=19001, source="static_cluster_bootstrap"),
]


@dataclass(slots=True)
class BootstrapConfig:
    dns_seeds: list[DnsSeed] = field(default_factory=lambda: list(BOOTSTRAP_DNS_SEEDS))
    public_endpoints: list[BootstrapEndpoint] = field(
        default_factory=lambda: list(BOOTSTRAP_PUBLIC_ENDPOINTS)
    )
