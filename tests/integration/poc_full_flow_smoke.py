from __future__ import annotations

import asyncio
from pathlib import Path
import sys


PROJECT_ROOT = Path(__file__).resolve().parents[2]
SUPPORT_ROOT = PROJECT_ROOT / "tests" / "support"
if str(SUPPORT_ROOT) not in sys.path:
    sys.path.insert(0, str(SUPPORT_ROOT))

from poc_flow_runner import main as run_poc_flow  # noqa: E402


if __name__ == "__main__":
    raise SystemExit(asyncio.run(run_poc_flow()))
