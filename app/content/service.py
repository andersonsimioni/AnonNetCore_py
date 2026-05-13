from __future__ import annotations

import base64
import binascii
from dataclasses import dataclass
from datetime import datetime, timezone
from hashlib import sha512
from pathlib import Path

from sqlalchemy import select
from storage import DatabaseManager, get_database
from storage.models import ContentObject


@dataclass(slots=True, frozen=True)
class ContentInfo:
    content_id: str
    content_hash: str
    size_bytes: int
    content_type: str
    storage_path: str | None = None


@dataclass(slots=True, frozen=True)
class ContentRange:
    content_id: str
    start_byte: int
    end_byte: int
    data: bytes

    @property
    def data_base64(self) -> str:
        return base64.b64encode(self.data).decode("ascii")


@dataclass(slots=True)
class ContentDownloadState:
    session_id: str
    content_id: str
    content_hash: str
    size_bytes: int
    content_type: str
    partial_path: Path
    final_path: Path
    next_start_byte: int = 0
    status: str = "downloading"
    error_message: str | None = None

    @property
    def completed(self) -> bool:
        return self.status == "completed"


class ContentTransferService:
    """Gerencia conteudo local e downloads remotos por byte ranges."""

    def __init__(
        self,
        *,
        database: DatabaseManager | None = None,
        storage_dir: str | Path = "data/local/content",
        download_range_size: int = 64 * 1024,
    ) -> None:
        self.database = database or get_database()
        self.storage_dir = Path(storage_dir)
        self.download_range_size = download_range_size
        self._downloads: dict[tuple[str, str], ContentDownloadState] = {}

    def configure(
        self,
        *,
        storage_dir: str | Path,
        download_range_size: int,
    ) -> None:
        self.storage_dir = Path(storage_dir)
        self.download_range_size = max(1, download_range_size)
        self.storage_dir.mkdir(parents=True, exist_ok=True)

    def get_content_info(self, content_id: str | None) -> ContentInfo | None:
        if not content_id:
            return None

        now = datetime.now(timezone.utc)
        with self.database.session_scope() as session:
            content_object = session.scalar(
                select(ContentObject)
                .where(ContentObject.content_hash == content_id)
                .where(ContentObject.is_deleted.is_(False))
            )
            if content_object is None:
                return None

            content_object.last_access_at = now
            return self._build_content_info(content_object)

    def read_content_range(
        self,
        *,
        content_id: str,
        start_byte: int,
        end_byte: int,
    ) -> ContentRange:
        if start_byte < 0:
            raise ValueError("start_byte precisa ser maior ou igual a zero.")
        if end_byte <= start_byte:
            raise ValueError("end_byte precisa ser maior que start_byte.")

        content_info = self.get_content_info(content_id)
        if content_info is None or content_info.storage_path is None:
            raise FileNotFoundError("Conteudo nao encontrado no storage local.")
        if end_byte > content_info.size_bytes:
            raise ValueError("end_byte nao pode passar do tamanho do conteudo.")

        file_path = Path(content_info.storage_path)
        with file_path.open("rb") as file:
            file.seek(start_byte)
            data = file.read(end_byte - start_byte)

        return ContentRange(
            content_id=content_id,
            start_byte=start_byte,
            end_byte=start_byte + len(data),
            data=data,
        )

    def start_or_update_download(
        self,
        *,
        session_id: str,
        content_id: str,
        content_hash: str,
        size_bytes: int,
        content_type: str,
    ) -> ContentDownloadState:
        key = self._download_key(session_id, content_id)
        existing_state = self._downloads.get(key)
        if existing_state is not None:
            return existing_state

        self.storage_dir.mkdir(parents=True, exist_ok=True)
        final_path = self.storage_dir / content_hash
        partial_path = self.storage_dir / f"{content_hash}.part"
        partial_path.parent.mkdir(parents=True, exist_ok=True)
        partial_path.write_bytes(b"")

        state = ContentDownloadState(
            session_id=session_id,
            content_id=content_id,
            content_hash=content_hash,
            size_bytes=size_bytes,
            content_type=content_type,
            partial_path=partial_path,
            final_path=final_path,
        )
        self._downloads[key] = state
        return state

    def get_next_range_request(
        self,
        *,
        session_id: str,
        content_id: str,
    ) -> tuple[int, int] | None:
        state = self._get_download_state(session_id=session_id, content_id=content_id)
        if state.status != "downloading":
            return None
        if state.next_start_byte >= state.size_bytes:
            return None

        start_byte = state.next_start_byte
        end_byte = min(start_byte + self.download_range_size, state.size_bytes)
        return start_byte, end_byte

    def handle_content_range_response(
        self,
        *,
        session_id: str,
        content_id: str,
        start_byte: int,
        end_byte: int,
        data_base64: str,
    ) -> ContentDownloadState:
        state = self._get_download_state(session_id=session_id, content_id=content_id)
        if state.status != "downloading":
            return state
        if start_byte != state.next_start_byte:
            state.status = "failed"
            state.error_message = "Range recebido fora de ordem."
            return state

        try:
            data = base64.b64decode(data_base64.encode("ascii"), validate=True)
        except (UnicodeEncodeError, binascii.Error):
            state.status = "failed"
            state.error_message = "Range recebido com base64 invalido."
            return state
        if end_byte != start_byte + len(data):
            state.status = "failed"
            state.error_message = "Tamanho do range recebido nao bate com start/end."
            return state

        with state.partial_path.open("ab") as file:
            file.write(data)
        state.next_start_byte = end_byte

        if state.next_start_byte >= state.size_bytes:
            self._complete_download(state)
        return state

    def _complete_download(self, state: ContentDownloadState) -> None:
        if state.partial_path.stat().st_size != state.size_bytes:
            state.status = "failed"
            state.error_message = "Tamanho final do arquivo nao bate com metadata."
            return

        actual_hash = sha512(state.partial_path.read_bytes()).hexdigest()
        if actual_hash != state.content_hash:
            state.status = "failed"
            state.error_message = "Hash final do arquivo nao bate com content_hash."
            return

        if state.final_path.exists():
            state.final_path.unlink()
        state.partial_path.replace(state.final_path)
        self._upsert_downloaded_content(state)
        state.status = "completed"

    def _upsert_downloaded_content(self, state: ContentDownloadState) -> None:
        with self.database.session_scope() as session:
            content_object = session.scalar(
                select(ContentObject).where(ContentObject.content_hash == state.content_hash)
            )
            if content_object is None:
                content_object = ContentObject(
                    content_hash=state.content_hash,
                    title=None,
                    content_type=state.content_type,
                    mime_type=state.content_type,
                    size_bytes=state.size_bytes,
                    storage_path=str(state.final_path),
                    is_encrypted=False,
                    encryption_scheme=None,
                    is_deleted=False,
                )
                session.add(content_object)
                return

            content_object.content_type = state.content_type
            content_object.mime_type = state.content_type
            content_object.size_bytes = state.size_bytes
            content_object.storage_path = str(state.final_path)
            content_object.is_deleted = False
            content_object.last_access_at = datetime.now(timezone.utc)

    def _get_download_state(
        self,
        *,
        session_id: str,
        content_id: str,
    ) -> ContentDownloadState:
        state = self._downloads.get(self._download_key(session_id, content_id))
        if state is None:
            raise ValueError("Download de conteudo nao foi iniciado.")
        return state

    @staticmethod
    def _download_key(session_id: str, content_id: str) -> tuple[str, str]:
        return session_id, content_id

    @staticmethod
    def _build_content_info(content_object: ContentObject) -> ContentInfo:
        return ContentInfo(
            content_id=content_object.content_hash,
            content_hash=content_object.content_hash,
            size_bytes=content_object.size_bytes,
            content_type=(
                content_object.mime_type
                or content_object.content_type
                or "application/octet-stream"
            ),
            storage_path=content_object.storage_path,
        )
