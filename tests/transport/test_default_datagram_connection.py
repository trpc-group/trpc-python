# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.
"""``DefaultDatagramConnection``: one immutable peer view per datagram."""

from __future__ import annotations

import asyncio
import threading
from typing import Any
from typing import List
from typing import Tuple

from trpc_sdk.abc import BaseServerTransportOption
from trpc_sdk.exceptions import ServerDecodeError
from trpc_sdk.transport import ConnectionOption
from trpc_sdk.transport import DefaultDatagramConnection
from trpc_sdk.transport import TrpcServerTransport
from trpc_sdk.transport._server._constants import SERVER_CONN_KEY

LOCAL = ("127.0.0.1", 8000)
FIRST = ("10.0.0.1", 30001)
SECOND = ("10.0.0.2", 30002)


class _DatagramTransport:
    """Records ``sendto`` calls and the thread they ran on."""

    def __init__(self) -> None:
        self.sent: List[Tuple[bytes, Any]] = []
        self.send_threads: List[int] = []
        self.closing = False

    def get_extra_info(self, key, default=None):
        return {"sockname": LOCAL}.get(key, default)

    def sendto(self, data: bytes, addr: Any) -> None:
        self.sent.append((data, addr))
        self.send_threads.append(threading.get_ident())

    def is_closing(self) -> bool:
        return self.closing


class _Parser:
    """Each datagram is one frame; ``b"bad"`` fails to decode."""

    def __init__(self) -> None:
        self.frames: List[bytes] = []
        self.resets = 0

    def create(self) -> "_Parser":
        return self

    def feed_data(self, data: bytes) -> None:
        self.frames.append(data)

    def read(self) -> List[bytes]:
        if b"bad" in self.frames:
            raise ServerDecodeError("bad frame")
        frames, self.frames = self.frames, []
        return frames

    def reset(self) -> None:
        self.frames = []
        self.resets += 1


def _connection(msg_handler, shared_pool=None) -> DefaultDatagramConnection:
    conn = DefaultDatagramConnection(ConnectionOption(parser=_Parser(), msg_handler=msg_handler,
                                                              shared_pool=shared_pool))
    conn.connection_made(_DatagramTransport())
    return conn


async def _drain() -> None:
    for _ in range(5):
        await asyncio.sleep(0)


async def test_each_handler_sees_its_own_datagram_source():
    seen: List[Tuple[bytes, Tuple[str, int]]] = []

    async def handle(conn, frame):
        await asyncio.sleep(0)
        seen.append((frame, conn.remote_addr))
        conn.write(frame)

    conn = _connection(handle)
    conn.datagram_received(b"a", FIRST)
    conn.datagram_received(b"b", SECOND)
    await _drain()

    assert sorted(seen) == [(b"a", FIRST), (b"b", SECOND)]
    assert sorted(conn.aio_conn.sent) == [(b"a", FIRST), (b"b", SECOND)]


async def test_decode_error_drops_only_that_datagram():
    seen: List[bytes] = []

    async def handle(conn, frame):
        seen.append(frame)

    conn = _connection(handle)
    conn.datagram_received(b"bad", FIRST)
    conn.datagram_received(b"good", SECOND)
    await _drain()

    assert seen == [b"good"]
    assert conn.parser.resets == 1
    assert not conn.aio_conn.is_closing()


async def test_handler_error_is_only_logged():

    async def handle(conn, frame):
        raise RuntimeError("boom")

    conn = _connection(handle)
    conn.datagram_received(b"a", FIRST)
    await _drain()
    assert not conn.aio_conn.is_closing()


async def test_error_received_does_not_raise():
    conn = _connection(None)
    conn.error_received(ConnectionRefusedError("port unreachable"))


async def test_shared_pool_answers_on_the_loop_to_the_source():
    loop_thread = threading.get_ident()
    handler_threads: List[int] = []

    async def handle(conn, frame):
        handler_threads.append(threading.get_ident())
        conn.write(b"rsp:" + frame)

    class _Pool:

        def submit(self, run):
            threading.Thread(target=lambda: asyncio.run(run())).start()

    conn = _connection(handle, shared_pool=_Pool())
    conn.datagram_received(b"a", FIRST)
    for _ in range(100):
        if conn.aio_conn.sent:
            break
        await asyncio.sleep(0.01)

    assert handler_threads and handler_threads[0] != loop_thread
    assert conn.aio_conn.sent == [(b"rsp:a", FIRST)]
    assert conn.aio_conn.send_threads == [loop_thread]


async def test_msg_handler_fills_real_addresses_and_answers_the_source():
    seen = []

    async def rpc_handler(ctx, data):
        seen.append(ctx)
        return b"rsp"

    transport = TrpcServerTransport()
    transport.bind(BaseServerTransportOption(parser=_Parser(), rpc_handler=rpc_handler, idle_timeout=1000))
    factory = transport.datagram_protocol_factory(type("Ctx", (), {"shared_pool": None})())
    conn = factory()
    conn.connection_made(_DatagramTransport())

    conn.datagram_received(b"req", FIRST)
    await _drain()

    ctx = seen[0]
    assert (ctx.remote_ip, ctx.remote_port) == FIRST
    assert (ctx.local_ip, ctx.local_port) == LOCAL
    assert ctx.get_meta_data(SERVER_CONN_KEY).remote_addr == FIRST
    assert conn.aio_conn.sent == [(b"rsp", FIRST)]


async def test_ipv6_source_is_truncated_but_sendto_keeps_it():
    seen: List[Tuple[str, int]] = []

    async def handle(conn, frame):
        seen.append(conn.remote_addr)
        conn.write(frame)

    source = ("::1", 30003, 0, 0)
    conn = _connection(handle)
    conn.datagram_received(b"a", source)
    await _drain()

    assert seen == [("::1", 30003)]
    assert conn.aio_conn.sent == [(b"a", source)]
