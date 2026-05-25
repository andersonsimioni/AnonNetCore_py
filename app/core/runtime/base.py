from __future__ import annotations

import asyncio


class PeriodicRuntime:
    """Base para runtimes que executam `_run_once` em intervalo fixo."""

    def __init__(
        self,
        engine,
        *,
        loop_interval_seconds: float,
        task_name: str,
    ) -> None:
        self.engine = engine
        self._task: asyncio.Task[None] | None = None
        self._stop_event = asyncio.Event()
        self._loop_interval_seconds = loop_interval_seconds
        self._task_name = task_name

    async def start(self) -> None:
        if self._task is not None and not self._task.done():
            return

        self._stop_event = asyncio.Event()
        self._task = asyncio.create_task(self._run_loop(), name=self._task_name)

    async def stop(self) -> None:
        if self._task is None:
            return

        self._stop_event.set()
        try:
            await self._task
        finally:
            self._task = None

    async def _run_loop(self) -> None:
        while not self._stop_event.is_set():
            await self._run_once()
            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=self._loop_interval_seconds)
            except TimeoutError:
                continue

    async def _run_once(self) -> None:
        raise NotImplementedError
