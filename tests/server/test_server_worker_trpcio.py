# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.
"""Rpc-task pipe connection that replaced CWorkerConnectionTrpcio.

There is no trpcio driver. Separate mode delivers framed requests to
``RpcTaskConnection``, which runs the handler and writes the response back.
"""

import asyncio
from unittest.mock import MagicMock

import pytest

from trpc_sdk.abc import BaseServerTransportOption
from trpc_sdk.abc import TrpcContext
from trpc_sdk.transport import RpcTaskConnection
from trpc_sdk.transport._cython import inner_decode_rsp
from trpc_sdk.transport._cython import inner_encode_req
from trpc_sdk.utils import now_ms


class _App:
    def __init__(self, loop):
        self.loop = loop
        self.rpcz_pipe_conn = None
        self.worker_id = 0


def _option():
    opt = BaseServerTransportOption()
    opt.set_socket_type("net")
    opt.set_address(("127.0.0.1", 8000))
    return opt


def _frame(index=0, body=b"ping"):
    return inner_encode_req(4, index, 9, int(now_ms()), b"127.0.0.1:8000|10.0.0.2:5000", body)


class _Pipe:
    def __init__(self, closing=False):
        self.written = []
        self.closing = closing
        self.closed = False

    def write(self, data):
        self.written.append(data)

    def is_closing(self):
        return self.closing

    def close(self):
        self.closed = True
        self.closing = True


@pytest.mark.asyncio
async def test_connection_made_and_lost():
    loop = asyncio.get_running_loop()
    conn = RpcTaskConnection(_App(loop), [None], [1000], [_option()])
    pipe = _Pipe()
    conn.connection_made(pipe)
    assert conn.aio_connection is pipe

    conn.connection_lost(RuntimeError("gone"))
    assert pipe.closed is True

    already = _Pipe(closing=True)
    conn.aio_connection = already
    conn.connection_lost(None)
    assert already.closed is False

    conn.aio_connection = None
    conn.connection_lost(None)


@pytest.mark.asyncio
async def test_data_received_runs_the_handler_and_writes_the_response():
    seen = {}

    async def handle(ctx: TrpcContext, req: bytes) -> bytes:
        seen["req"] = req
        seen["fd"] = ctx.get_meta_data("fd")
        seen["session"] = ctx.get_meta_data("session_id")
        seen["remote"] = (ctx.remote_ip, ctx.remote_port)
        seen["local"] = (ctx.local_ip, ctx.local_port)
        return b"pong"

    loop = asyncio.get_running_loop()
    conn = RpcTaskConnection(_App(loop), [handle], [1000], [_option()])
    pipe = _Pipe()
    conn.connection_made(pipe)

    frame = _frame()
    for offset in range(0, len(frame), 20):
        conn.data_received(frame[offset:offset + 20])
    await asyncio.sleep(0.05)

    assert seen["req"] == b"ping"
    assert seen["fd"] == 4
    assert seen["session"] == 9
    assert seen["remote"] == ("10.0.0.2", 5000)
    assert seen["local"] == ("127.0.0.1", 8000)
    fd, session_id, _timeout, body = inner_decode_rsp(pipe.written[0])
    assert (fd, session_id, body) == (4, 9, b"pong")


@pytest.mark.asyncio
async def test_handler_exception_and_decode_error_are_contained():
    async def boom(_ctx, _req):
        raise RuntimeError("handler")

    loop = asyncio.get_running_loop()
    conn = RpcTaskConnection(_App(loop), [boom, boom], [1000, 2000], [_option(), _option()])
    pipe = _Pipe()
    conn.connection_made(pipe)

    conn.data_received(_frame(index=1, body=b"nope"))
    await asyncio.sleep(0.05)
    assert pipe.written == []

    conn.data_received(b"\x00" * 40)
    assert pipe.written == []


def test_indexes_handlers_by_address_index():
    loop = asyncio.new_event_loop()
    try:
        called = []

        async def first(ctx, req):
            called.append(("first", req))
            return b"a"

        async def second(ctx, req):
            called.append(("second", req))
            return b"b"

        conn = RpcTaskConnection(_App(loop), [first, second], [1000, 2000], [_option(), _option()])
        pipe = _Pipe()
        conn.connection_made(pipe)
        conn.data_received(_frame(index=1, body=b"to-second"))
        loop.run_until_complete(asyncio.sleep(0.05))
        assert called == [("second", b"to-second")]
        _fd, _session, _timeout, body = inner_decode_rsp(pipe.written[0])
        assert body == b"b"
    finally:
        loop.close()
