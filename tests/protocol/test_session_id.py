# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.
"""Every TCP connection takes a process-unique, non-zero ``session_id``."""

from __future__ import annotations

import asyncio
import itertools
import threading
from typing import List

from tests.protocol._fakes import ParserFactory
from tests.protocol._fakes import FakeTransport
from tests.protocol._fakes import stream_frame
from tests.protocol._fakes import unary_frame
from trpc_sdk.abc import get_message
from trpc_sdk.transport import TCPConnection
from trpc_sdk.transport._server._cython import _tcp_protocol
from trpc_sdk.transport import TcpConnectionOption


def _connection(handle=None) -> TCPConnection:

    async def _noop(ctx, req_buf):
        return None

    conn = TCPConnection(TcpConnectionOption(parser=ParserFactory(), rpc_handler=handle or _noop, timeout=1000))
    conn.connection_made(FakeTransport())
    return conn


def test_connections_get_distinct_non_zero_ids():
    first, second = _connection(), _connection()
    assert first.session_id != 0 and second.session_id != 0
    assert first.session_id != second.session_id


async def test_every_frame_carries_the_connection_id():
    seen: List[int] = []

    async def handle(ctx, req_buf):
        seen.append(get_message(ctx).session_id)
        return None

    conn = _connection(handle)
    conn.data_received(unary_frame())
    conn.data_received(stream_frame(7))
    for _ in range(3):
        await asyncio.sleep(0)
    assert seen == [conn.session_id] * 2


def test_wrap_skips_zero(monkeypatch):
    monkeypatch.setattr(_tcp_protocol, "_session_ids", itertools.count(0x7FFFFFFF))
    assert _tcp_protocol.next_session_id() == 0x7FFFFFFF
    # 0x80000000 masks to 0, which means "not assigned"; it is skipped.
    assert _tcp_protocol.next_session_id() == 1
    assert _tcp_protocol.next_session_id() == 2


def test_concurrent_threads_never_share_an_id():
    results: List[List[int]] = [[] for _ in range(4)]

    def take(out: List[int]) -> None:
        for _ in range(10000):
            out.append(_tcp_protocol.next_session_id())

    threads = [threading.Thread(target=take, args=(out,)) for out in results]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    ids = [i for out in results for i in out]
    assert len(set(ids)) == len(ids) == 40000
    assert 0 not in ids
