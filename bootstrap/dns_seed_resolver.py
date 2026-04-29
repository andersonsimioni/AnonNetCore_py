from __future__ import annotations

from .models import DnsSeed


class DnsSeedResolver:
    """Resolver simples para DNS seeds hardcoded."""

    async def resolve(self, seeds: list[DnsSeed]) -> list[DnsSeed]:
        return [seed for seed in seeds if seed.is_enabled]
