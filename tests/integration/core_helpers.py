from __future__ import annotations

from pathlib import Path
import sys
import shutil
from typing import Iterable


APP_ROOT = Path(__file__).resolve().parents[2] / "app"
if str(APP_ROOT) not in sys.path:
    sys.path.insert(0, str(APP_ROOT))

from bootstrap.models import BootstrapEndpoint, DnsSeed
from core import CoreConfig, CoreEngine, EngineServices
from dht import DhtService
from identity import IdentityService
from route import RouteService
from storage import DatabaseConfig, DatabaseManager


def create_isolated_core(
    *,
    data_dir: Path,
    listen_port: int,
    listen_host: str = "0.0.0.0",
    log_dir: Path | None = None,
    bootstrap_public_endpoints: Iterable[BootstrapEndpoint] | None = None,
    bootstrap_dns_seeds: Iterable[DnsSeed] | None = None,
    reset_database: bool = False,
) -> CoreEngine:
    """Cria um CoreEngine com banco proprio para cenarios de integracao."""

    data_dir.mkdir(parents=True, exist_ok=True)
    db_path = data_dir / "anonnetcore.db"
    if reset_database and db_path.exists():
        db_path.unlink()

    database = DatabaseManager(DatabaseConfig(db_path=db_path))
    config = CoreConfig(
        listen_host=listen_host,
        listen_port=listen_port,
        log_dir=log_dir or data_dir / "logs",
    )
    config.api_enabled = False
    config.content_storage_dir = data_dir / "content"
    config.virtual_route_maintenance_runtime_interval_seconds = 1.0
    config.virtual_route_maintenance_expected_round_trip_ttl_ms = 2000
    config.virtual_route_maintenance_candidate_limit = 16
    if bootstrap_public_endpoints is not None:
        config.bootstrap_public_endpoints = list(bootstrap_public_endpoints)
    if bootstrap_dns_seeds is not None:
        config.bootstrap_dns_seeds = list(bootstrap_dns_seeds)

    services = EngineServices(
        config=config,
        database=database,
        dht_service=DhtService(config=config, database=database),
        identity_service=IdentityService(database=database),
        route_service=RouteService(database=database),
    )
    return CoreEngine(services=services)


def reset_core_data_dir(data_dir: Path) -> None:
    """Remove dados anteriores de um core de teste."""

    if data_dir.exists():
        shutil.rmtree(data_dir)
    data_dir.mkdir(parents=True, exist_ok=True)


async def stop_cores(*engines: CoreEngine) -> None:
    """Para engines ignorando erros de cleanup para nao esconder o erro real do teste."""

    for engine in engines:
        try:
            await engine.stop()
        except Exception:
            pass
