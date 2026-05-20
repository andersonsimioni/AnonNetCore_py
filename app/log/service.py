from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from threading import Lock


class LogService:
    """Escreve logs estruturados no stdout e, opcionalmente, em arquivo local."""

    def __init__(self) -> None:
        self.node_name: str | None = None
        self.log_file_path: Path | None = None
        self._write_lock = Lock()

    def configure(
        self,
        *,
        node_name: str | None = None,
        log_file_path: str | Path | None = None,
    ) -> None:
        if node_name is not None:
            self.node_name = node_name

        if log_file_path is not None:
            self.log_file_path = Path(log_file_path)
            self.log_file_path.parent.mkdir(parents=True, exist_ok=True)

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
        line = self._build_log_line(
            level=level,
            component=component,
            message=message,
            fields=fields,
        )
        with self._write_lock:
            try:
                print(line, flush=True)
            except OSError:
                pass
            if self.log_file_path is not None:
                try:
                    with self.log_file_path.open("a", encoding="utf-8") as log_file:
                        log_file.write(line + "\n")
                except OSError:
                    pass

    def _build_log_line(
        self,
        *,
        level: str,
        component: str,
        message: str,
        fields: dict[str, object],
    ) -> str:
        timestamp = datetime.now(timezone.utc).isoformat()
        parts = [timestamp, level]

        if self.node_name:
            parts.append(self.node_name)

        parts.append(component)
        parts.append(message)

        serialized_fields = self._serialize_fields(fields)
        if serialized_fields:
            parts.append(serialized_fields)

        return " | ".join(parts)

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
