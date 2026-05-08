from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(slots=True, frozen=True)
class DnsSeed:
    host: str
    port: int
    transport: str = "tcp"
    is_enabled: bool = True


@dataclass(slots=True, frozen=True)
class BootstrapEndpoint:
    host: str
    port: int
    transport: str = "tcp"
    source: str = "public_endpoint"
    is_enabled: bool = True


@dataclass(slots=True, frozen=True)
class BootstrapResolutionResult:
    dns_seeds: list[DnsSeed] = field(default_factory=list)
    public_endpoints: list[BootstrapEndpoint] = field(default_factory=list)

    @property
    def all_endpoints(self) -> list[BootstrapEndpoint]:
        dns_endpoints = [
            BootstrapEndpoint(
                host=seed.host,
                port=seed.port,
                transport=seed.transport,
                source="dns_seed",
                is_enabled=seed.is_enabled,
            )
            for seed in self.dns_seeds
            if seed.is_enabled
        ]
        public_endpoints = [
            endpoint for endpoint in self.public_endpoints if endpoint.is_enabled
        ]
        return dns_endpoints + public_endpoints
