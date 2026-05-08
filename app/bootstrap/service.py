from __future__ import annotations

from .dns_seed_resolver import DnsSeedResolver
from .models import BootstrapEndpoint, BootstrapResolutionResult, DnsSeed


class BootstrapService:
    """Consolida seeds iniciais para a engine entrar na rede."""

    def __init__(
        self,
        dns_seed_resolver: DnsSeedResolver | None = None,
    ) -> None:
        self.dns_seed_resolver = dns_seed_resolver or DnsSeedResolver()

    async def load_bootstrap_targets(
        self,
        *,
        dns_seeds: list[DnsSeed],
        public_endpoints: list[BootstrapEndpoint],
    ) -> BootstrapResolutionResult:
        resolved_dns_seeds = await self.dns_seed_resolver.resolve(dns_seeds)
        return BootstrapResolutionResult(
            dns_seeds=resolved_dns_seeds,
            public_endpoints=self._get_enabled_public_endpoints(public_endpoints),
        )

    async def list_bootstrap_endpoints(
        self,
        *,
        dns_seeds: list[DnsSeed],
        public_endpoints: list[BootstrapEndpoint],
    ) -> list[BootstrapEndpoint]:
        resolution = await self.load_bootstrap_targets(
            dns_seeds=dns_seeds,
            public_endpoints=public_endpoints,
        )
        return resolution.all_endpoints

    def _get_enabled_public_endpoints(
        self,
        public_endpoints: list[BootstrapEndpoint],
    ) -> list[BootstrapEndpoint]:
        return [
            endpoint
            for endpoint in public_endpoints
            if endpoint.is_enabled
        ]
