from __future__ import annotations

import struct


FRAME_LENGTH_SIZE = 4


class LengthPrefixedFrameCodec:
    """Codec simples com cabecalho de 4 bytes contendo o tamanho do frame."""

    @staticmethod
    def encode(payload: bytes) -> bytes:
        return struct.pack(">I", len(payload)) + payload

    @staticmethod
    async def read_frame(reader) -> bytes:
        length_data = await reader.readexactly(FRAME_LENGTH_SIZE)
        frame_length = struct.unpack(">I", length_data)[0]
        return await reader.readexactly(frame_length)

    @staticmethod
    async def write_frame(writer, payload: bytes) -> None:
        writer.write(LengthPrefixedFrameCodec.encode(payload))
        await writer.drain()
