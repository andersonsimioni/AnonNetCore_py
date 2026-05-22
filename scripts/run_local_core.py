from __future__ import annotations

import argparse
import asyncio
import shutil
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
APP_ROOT = PROJECT_ROOT / "app"
LOCAL_DATA_ROOT = PROJECT_ROOT / "data" / "local"
if str(APP_ROOT) not in sys.path:
    sys.path.insert(0, str(APP_ROOT))

from core import CoreConfig, CoreEngine
from main import run_node


def main() -> int:
    args = parse_args()
    if args.reset_data:
        reset_local_demo_state()
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
    parser.add_argument(
        "--no-reset-data",
        dest="reset_data",
        action="store_false",
        help="Nao limpa data/local antes de iniciar. Use apenas para depuracao.",
    )
    parser.set_defaults(reset_data=True)
    return parser.parse_args()


def reset_local_demo_state() -> None:
    print(f"Limpando estado local da demo: {LOCAL_DATA_ROOT}")
    if LOCAL_DATA_ROOT.exists():
        shutil.rmtree(LOCAL_DATA_ROOT)
    (LOCAL_DATA_ROOT / "logs").mkdir(parents=True, exist_ok=True)
    (LOCAL_DATA_ROOT / "content").mkdir(parents=True, exist_ok=True)


if __name__ == "__main__":
    raise SystemExit(main())
