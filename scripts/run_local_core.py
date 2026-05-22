from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
APP_ROOT = PROJECT_ROOT / "app"
if str(APP_ROOT) not in sys.path:
    sys.path.insert(0, str(APP_ROOT))

from core import CoreConfig, CoreEngine
from main import run_node


def main() -> int:
    args = parse_args()
    print(f"Iniciando core local na porta TCP {args.listen_port}...")
    engine = CoreEngine()
    engine.services.config.listen_port = args.listen_port
    asyncio.run(run_node(engine))
    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Sobe apenas um core local AnonNetCore.")
    parser.add_argument(
        "--listen-port",
        type=int,
        default=19101,
        help="Porta TCP do core local.",
    )
    return parser.parse_args()


if __name__ == "__main__":
    raise SystemExit(main())
