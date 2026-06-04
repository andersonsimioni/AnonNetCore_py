from __future__ import annotations

from hashlib import sha512
from pathlib import Path

from storage.models import ContentAdvertisement, ContentObject

from smoke_helpers import (
    validate_virtual_message_roundtrip,
    wait_for_virtual_keepalive_ack,
    wait_until_value,
)
from smokes_config import SMOKES_CONFIG


APP_MESSAGE_TYPE = "integration.virtual-message.echo"
APP_REPLY_MESSAGE_TYPE = "integration.virtual-message.echo.reply"


async def validate_virtual_session_keepalive(
    engine,
    session_id: str,
    *,
    cluster_nodes: int | None = None,
) -> None:
    await wait_for_virtual_keepalive_ack(engine, session_id, cluster_nodes=cluster_nodes)


async def validate_virtual_message_exchange(
    core_a,
    core_b,
    session_id: str,
    *,
    cluster_nodes: int | None = None,
) -> None:
    await validate_virtual_message_roundtrip(
        sender_engine=core_b,
        receiver_engine=core_a,
        session_id=session_id,
        cluster_nodes=cluster_nodes,
        app_message_type=APP_MESSAGE_TYPE,
        reply_message_type=APP_REPLY_MESSAGE_TYPE,
        payload={
            "text": "hello from core full flow smoke",
            "sequence": 1,
        },
    )


async def validate_virtual_content_download(
    *,
    provider_engine,
    downloader_engine,
    session_id: str,
    cluster_nodes: int | None = None,
) -> bytes:
    content_bytes = build_test_content()
    content_hash = register_local_content(provider_engine, content_bytes)

    await downloader_engine.services.protocol_clients.virtual.session.send_protocol_message(
        session_id=session_id,
        message_type="VIRTUAL_CONTENT_INFO_REQUEST",
        payload={
            "content_id": content_hash,
        },
    )

    downloaded_content = await wait_for_downloaded_content(
        downloader_engine,
        content_hash=content_hash,
        cluster_nodes=cluster_nodes,
    )
    await wait_for_ddt_provider_advertisement(
        downloader_engine,
        content_hash=content_hash,
        cluster_nodes=cluster_nodes,
    )
    downloaded_bytes = Path(downloaded_content.storage_path).read_bytes()
    if downloaded_bytes != content_bytes:
        raise RuntimeError("Downloaded content bytes mismatch.")
    return downloaded_bytes


def build_test_content() -> bytes:
    line = b"AnonNetCore virtual content smoke payload.\n"
    return line * SMOKES_CONFIG.virtual_content_line_repetitions


def register_local_content(engine, content_bytes: bytes) -> str:
    content_hash = sha512(content_bytes).hexdigest()
    storage_dir = Path(engine.services.config.content_storage_dir)
    storage_dir.mkdir(parents=True, exist_ok=True)
    storage_path = storage_dir / content_hash
    storage_path.write_bytes(content_bytes)

    with engine.services.database.session_scope() as session:
        session.add(
            ContentObject(
                content_hash=content_hash,
                title="core-full-flow-content",
                content_type="application/octet-stream",
                mime_type="application/octet-stream",
                size_bytes=len(content_bytes),
                storage_path=str(storage_path),
                is_encrypted=False,
                encryption_scheme=None,
                is_deleted=False,
            )
        )

    return content_hash


async def wait_for_downloaded_content(engine, *, content_hash: str, cluster_nodes: int | None = None):
    async def load_content():
        return engine.services.content_transfer_service.get_content_info(content_hash)

    content_info = await wait_until_value(
        load_content,
        timeout_seconds=SMOKES_CONFIG.virtual_content_transfer_timeout_seconds(
            cluster_nodes or SMOKES_CONFIG.min_cluster_nodes
        ),
        label="virtual content download completed",
    )
    if content_info.storage_path is None:
        raise RuntimeError("Downloaded content has no storage path.")
    return content_info


async def wait_for_ddt_provider_advertisement(
    engine,
    *,
    content_hash: str,
    cluster_nodes: int | None = None,
):
    async def load_advertisement():
        with engine.services.database.session_scope() as session:
            content_object = (
                session.query(ContentObject)
                .filter(ContentObject.content_hash == content_hash)
                .first()
            )
            if content_object is None:
                return None

            return (
                session.query(ContentAdvertisement)
                .filter(ContentAdvertisement.content_object_id == content_object.id)
                .filter(ContentAdvertisement.published_in_ddt.is_(True))
                .filter(ContentAdvertisement.is_active.is_(True))
                .first()
            )

    return await wait_until_value(
        load_advertisement,
        timeout_seconds=SMOKES_CONFIG.virtual_content_transfer_timeout_seconds(
            cluster_nodes or SMOKES_CONFIG.min_cluster_nodes
        ),
        label="downloaded content ddt provider advertisement",
    )
