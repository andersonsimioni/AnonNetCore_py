from __future__ import annotations

import json
import urllib.error
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from queue import Empty, Full, Queue
from threading import Lock, Thread
from typing import Final


_STOP_LOG_WORKER: Final = object()


@dataclass(slots=True, frozen=True)
class _LogEvent:
    timestamp: str
    level: str
    node_name: str | None
    component: str
    message: str
    fields: dict[str, object]
    line: str


class LogService:
    """Writes structured logs and can report serious events to a smoke collector."""

    def __init__(self) -> None:
        self.node_name: str | None = None
        self.log_file_path: Path | None = None
        self.enabled = True
        self.print_enabled = True
        self.async_enabled = True
        self.batch_size = 256
        self.error_report_enabled = False
        self.error_report_endpoint: str | None = None
        self.error_report_levels = {"WARNING", "ERROR"}
        self.error_report_timeout_seconds = 0.25
        self.error_report_batch_size = 64
        self._write_lock = Lock()
        self._queue: Queue[_LogEvent | object] | None = None
        self._worker: Thread | None = None

    def configure(
        self,
        *,
        node_name: str | None = None,
        log_file_path: str | Path | None = None,
        enabled: bool = True,
        print_enabled: bool = True,
        async_enabled: bool = True,
        queue_max_size: int = 20_000,
        batch_size: int = 256,
        error_report_enabled: bool = False,
        error_report_endpoint: str | None = None,
        error_report_levels: tuple[str, ...] = ("WARNING", "ERROR"),
        error_report_timeout_seconds: float = 0.25,
        error_report_batch_size: int = 64,
    ) -> None:
        self.shutdown()
        self.enabled = enabled
        self.print_enabled = print_enabled
        self.async_enabled = async_enabled
        self.batch_size = max(1, batch_size)
        self.error_report_enabled = error_report_enabled
        self.error_report_endpoint = error_report_endpoint
        self.error_report_levels = {level.upper() for level in error_report_levels}
        self.error_report_timeout_seconds = max(0.05, error_report_timeout_seconds)
        self.error_report_batch_size = max(1, error_report_batch_size)

        if node_name is not None:
            self.node_name = node_name

        if log_file_path is not None:
            self.log_file_path = Path(log_file_path)
            if self.enabled:
                self.log_file_path.parent.mkdir(parents=True, exist_ok=True)

        if self.enabled and self.async_enabled:
            self._queue = Queue(maxsize=max(1, queue_max_size))
            self._worker = Thread(
                target=self._run_worker,
                name="anonnet-log-writer",
                daemon=True,
            )
            self._worker.start()

    def shutdown(self) -> None:
        queue = self._queue
        worker = self._worker
        if queue is None or worker is None:
            return

        try:
            queue.put(_STOP_LOG_WORKER, timeout=0.25)
            queue.join()
        except Full:
            pass
        self._queue = None
        self._worker = None
        worker.join(timeout=2.0)

    def debug(self, component: str, message: str, **fields: object) -> None:
        self._write("DEBUG", component, message, fields)

    def info(self, component: str, message: str, **fields: object) -> None:
        self._write("INFO", component, message, fields)

    def warning(self, component: str, message: str, **fields: object) -> None:
        self._write("WARNING", component, message, fields)

    def error(self, component: str, message: str, **fields: object) -> None:
        self._write("ERROR", component, message, fields)

    def _write(
        self,
        level: str,
        component: str,
        message: str,
        fields: dict[str, object],
    ) -> None:
        if not self.enabled:
            return

        event = self._build_log_event(
            level=level,
            component=component,
            message=message,
            fields=fields,
        )
        if self.async_enabled and self._queue is not None:
            try:
                self._queue.put_nowait(event)
            except Full:
                pass
            return

        self._write_events([event])

    def _run_worker(self) -> None:
        queue = self._queue
        if queue is None:
            return

        while True:
            batch = self._collect_batch(queue)
            if batch is None:
                return
            if not batch:
                continue
            self._write_events(batch)

    def _collect_batch(self, queue: Queue[_LogEvent | object]) -> list[_LogEvent] | None:
        try:
            item = queue.get(timeout=0.25)
        except Empty:
            return [] if self._queue is not None else None

        if item is _STOP_LOG_WORKER:
            queue.task_done()
            return None

        batch = [item]
        queue.task_done()
        while len(batch) < self.batch_size:
            try:
                item = queue.get_nowait()
            except Empty:
                break

            if item is _STOP_LOG_WORKER:
                queue.task_done()
                return batch

            batch.append(item)
            queue.task_done()
        return batch

    def _write_events(self, events: list[_LogEvent]) -> None:
        self._write_lines([event.line for event in events])
        self._report_events(events)

    def _write_lines(self, lines: list[str]) -> None:
        if not lines:
            return

        with self._write_lock:
            if self.print_enabled:
                for line in lines:
                    try:
                        print(line, flush=True)
                    except OSError:
                        pass
            if self.log_file_path is not None:
                try:
                    with self.log_file_path.open("a", encoding="utf-8") as log_file:
                        log_file.write("\n".join(lines) + "\n")
                except OSError:
                    pass

    def _report_events(self, events: list[_LogEvent]) -> None:
        if (
            not self.error_report_enabled
            or not self.error_report_endpoint
            or not events
        ):
            return

        reportable_events = [
            event
            for event in events
            if event.level.upper() in self.error_report_levels
        ]
        for index in range(0, len(reportable_events), self.error_report_batch_size):
            batch = reportable_events[index:index + self.error_report_batch_size]
            if batch:
                self._post_error_report_batch(batch)

    def _post_error_report_batch(self, events: list[_LogEvent]) -> None:
        payload = {
            "events": [
                {
                    "timestamp": event.timestamp,
                    "node": event.node_name,
                    "level": event.level,
                    "component": event.component,
                    "message": event.message,
                    "fields": event.fields,
                    "line": event.line,
                }
                for event in events
            ]
        }
        data = json.dumps(payload, separators=(",", ":"), ensure_ascii=True).encode("utf-8")
        request = urllib.request.Request(
            self.error_report_endpoint,
            data=data,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            urllib.request.urlopen(
                request,
                timeout=self.error_report_timeout_seconds,
            ).close()
        except (OSError, TimeoutError, ValueError, urllib.error.URLError):
            pass

    def _build_log_event(
        self,
        *,
        level: str,
        component: str,
        message: str,
        fields: dict[str, object],
    ) -> _LogEvent:
        timestamp = datetime.now(timezone.utc).isoformat()
        parts = [timestamp, level]

        if self.node_name:
            parts.append(self.node_name)

        parts.append(component)
        parts.append(message)

        serialized_fields = self._serialize_fields(fields)
        if serialized_fields:
            parts.append(serialized_fields)

        return _LogEvent(
            timestamp=timestamp,
            level=level,
            node_name=self.node_name,
            component=component,
            message=message,
            fields=fields,
            line=" | ".join(parts),
        )

    @staticmethod
    def _serialize_fields(fields: dict[str, object]) -> str:
        serialized_items: list[str] = []
        for key, value in fields.items():
            if value is None:
                continue
            serialized_items.append(f"{key}={LogService._stringify_value(value)}")
        return " ".join(serialized_items)

    @staticmethod
    def _stringify_value(value: object) -> str:
        if isinstance(value, (str, int, float, bool)):
            return str(value)
        return json.dumps(value, separators=(",", ":"), sort_keys=True, ensure_ascii=True)
