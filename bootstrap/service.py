from __future__ import annotations

from .config import BootstrapConfig
from .dns_seed_resolver import DnsSeedResolver
from .models import BootstrapEndpoint, BootstrapResolutionResult


class BootstrapService:
    """Consolida seeds iniciais para a engine entrar na rede."""

    def __init__(
        self,
        config: BootstrapConfig | None = None,
        dns_seed_resolver: DnsSeedResolver | None = None,
    ) -> None:
        self.config = config or BootstrapConfig()
        self.dns_seed_resolver = dns_seed_resolver or DnsSeedResolver()

    async def load_bootstrap_targets(self) -> BootstrapResolutionResult:
        dns_seeds = await self.dns_seed_resolver.resolve(self.config.dns_seeds)
        public_endpoints = self._get_enabled_public_endpoints()
        return BootstrapResolutionResult(
            dns_seeds=dns_seeds,
            public_endpoints=public_endpoints,
        )

    async def list_bootstrap_endpoints(self) -> list[BootstrapEndpoint]:
        resolution = await self.load_bootstrap_targets()
        return resolution.all_endpoints

    def _get_enabled_public_endpoints(self) -> list[BootstrapEndpoint]:
        return [
            endpoint
            for endpoint in self.config.public_endpoints
            if endpoint.is_enabled
        ]
