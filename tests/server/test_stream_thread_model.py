# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.
"""Stream frames under each merge thread model: where they run and how a close ends them."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from typing import List
from typing import Tuple

from tests.protocol._fakes import ParserFactory
from tests.protocol._fakes import FakeTransport
from tests.protocol._fakes import stream_frame
from tests.protocol._fakes import unary_frame
from trpc_sdk.abc import STREAM_WRITER_CTX_KEY
from trpc_sdk.transport import ConnWriter
from trpc_sdk.transport import TCPConnection
from trpc_sdk.server._server import Server
from trpc_sdk.transport import TcpConnectionOption


class _Dispatcher:

    def __init__(self) -> None:
        self.aborted: List[Tuple[int, bool]] = []
        self.writer = None

    def abort_session(self, session_id: int) -> None:
        self.aborted.append((session_id, self.writer.closed))


class _Service:
    """Stands in for ``ServiceAdapter``: the protocol reaches the dispatcher through ``handle.__self__``."""

    def __init__(self) -> None:
        self.opt = SimpleNamespace(stream_handle=_Dispatcher())
        self.frames: List[object] = []

    async def handle(self, ctx, req_buf):
        self.frames.append(ctx.get_meta_data(STREAM_WRITER_CTX_KEY))
        return None


class _Pool:
    """SharedHandlerPool double recording what an I/O loop hands over."""

    def __init__(self) -> None:
        self.unary: List[object] = []
        self.streams: List[Tuple[int, object]] = []
        self.posts: List[Tuple[int, object, tuple]] = []

    def submit(self, run) -> None:
        self.unary.append(run)

    @staticmethod
    def stream_target(session_id: int, stream_id: int) -> int:
        return (session_id + stream_id) % 3

    def submit_stream(self, index: int, run) -> None:
        self.streams.append((index, run))

    def post(self, index: int, callback, *args) -> None:
        self.posts.append((index, callback, args))


def _connection(service: _Service, pool=None) -> Tuple[TCPConnection, FakeTransport]:
    transport = FakeTransport()
    conn = TCPConnection(
        TcpConnectionOption(parser=ParserFactory(), rpc_handler=service.handle, timeout=1000, shared_pool=pool))
    conn.connection_made(transport)
    return conn, transport


def test_streaming_services_keep_the_configured_thread_model():
    assert not hasattr(Server, "_resolve_stream_thread_model")


async def test_merge_runs_stream_frames_on_the_connection_loop_with_a_lazy_writer():
    service = _Service()
    conn, _ = _connection(service)
    conn.data_received(unary_frame())
    await asyncio.sleep(0)
    # Unary-only connections never build a writer.
    assert conn.writer is None and not conn.has_streams

    conn.data_received(stream_frame(101))
    await asyncio.sleep(0)
    assert isinstance(conn.writer, ConnWriter)
    assert service.frames == [None, conn.writer]
    assert conn.has_streams and conn.stream_dispatcher is service.opt.stream_handle

    dispatcher = service.opt.stream_handle
    dispatcher.writer = conn.writer
    conn.close()
    assert dispatcher.aborted == []  # posted, not called
    await asyncio.sleep(0)
    assert dispatcher.aborted == [(conn.session_id, True)]


async def test_separate_pins_each_stream_and_aborts_on_every_pinned_thread():
    service = _Service()
    pool = _Pool()
    conn, transport = _connection(service, pool)

    for stream_id in (101, 102, 101, 103):
        conn.data_received(stream_frame(stream_id))
    conn.data_received(unary_frame())

    assert not transport.closing
    targets = [index for index, _ in pool.streams]
    assert targets == [(conn.session_id + s) % 3 for s in (101, 102, 101, 103)]
    assert len(pool.unary) == 1
    assert conn.stream_loops == set(targets)

    await asyncio.gather(*(run() for _, run in pool.streams))
    assert service.frames == [conn.writer] * 4

    conn.close()
    assert conn.writer.closed
    dispatcher = service.opt.stream_handle
    assert sorted(index for index, _, _ in pool.posts) == sorted(set(targets))
    assert all(cb == dispatcher.abort_session and args == (conn.session_id,) for _, cb, args in pool.posts)


async def test_close_without_streams_posts_nothing():
    service = _Service()
    pool = _Pool()
    conn, _ = _connection(service, pool)
    conn.data_received(unary_frame())
    conn.close()
    assert pool.posts == []
