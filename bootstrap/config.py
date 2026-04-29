from __future__ import annotations

from dataclasses import dataclass, field

from .models import BootstrapEndpoint, DnsSeed


BOOTSTRAP_DNS_SEEDS: list[DnsSeed] = [
    DnsSeed(host="seed-1.anonnet.local", port=9000),
    DnsSeed(host="seed-2.anonnet.local", port=9000),
]


BOOTSTRAP_PUBLIC_ENDPOINTS: list[BootstrapEndpoint] = [
    BootstrapEndpoint(host="127.0.0.1", port=9000, source="public_endpoint"),
    BootstrapEndpoint(host="127.0.0.1", port=9001, source="public_endpoint"),
    BootstrapEndpoint(host="127.0.0.1", port=9002, source="public_endpoint"),
]


@dataclass(slots=True)
class BootstrapConfig:
    dns_seeds: list[DnsSeed] = field(default_factory=lambda: list(BOOTSTRAP_DNS_SEEDS))
    public_endpoints: list[BootstrapEndpoint] = field(
        default_factory=lambda: list(BOOTSTRAP_PUBLIC_ENDPOINTS)
    )
