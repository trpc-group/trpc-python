# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.
"""Fakes for driving ``TCPConnection`` without a socket."""

from __future__ import annotations

import struct
from typing import List

from trpc_sdk._inner import trpc_pb2


class FakeTransport:
    """Records writes; ``is_closing`` turns True after ``close``."""

    def __init__(self) -> None:
        self.written: List[bytes] = []
        self.closing = False

    def get_extra_info(self, key, default=None):
        return {"sockname": ("127.0.0.1", 8000), "peername": ("127.0.0.1", 50000)}.get(key, default)

    def write(self, data: bytes) -> None:
        self.written.append(data)

    def is_closing(self) -> bool:
        return self.closing

    def close(self) -> None:
        self.closing = True


class _Parser:
    """Every ``feed_data`` chunk is one complete frame."""

    def __init__(self) -> None:
        self.frames: List[bytes] = []

    def feed_data(self, data: bytes) -> None:
        self.frames.append(data)

    def read(self) -> List[bytes]:
        frames, self.frames = self.frames, []
        return frames

    def reset(self) -> None:
        self.frames = []


class ParserFactory:
    def create(self) -> _Parser:
        return _Parser()


def unary_frame() -> bytes:
    return struct.pack(">HBBIHIH", 0x930, 0, 0, 16, 0, 0, 0)


def stream_frame(stream_id: int, frame_state: int = trpc_pb2.TRPC_STREAM_FRAME_DATA) -> bytes:
    return struct.pack(">HBBIHIH", 0x930, trpc_pb2.TRPC_STREAM_FRAME, frame_state, 16, 0, stream_id, 0)
