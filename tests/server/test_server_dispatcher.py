# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.
"""Separate-mode I/O connections that replaced the Python dispatcher."""

import asyncio
import time
from unittest.mock import MagicMock

import pytest

from trpc_sdk._inner import trpc_pb2
from trpc_sdk.codec import get_server_parser
from trpc_sdk.transport import ClientConnectionInfo
from trpc_sdk.transport import SchedulerParams
from trpc_sdk.transport import SeparateClientConnection
from trpc_sdk.transport import SeparateRpcTaskConnection
from trpc_sdk.transport import get_next_worker
from trpc_sdk.transport import register_dispatcher_worker_func
from trpc_sdk.transport import select_rpc_task
from trpc_sdk.transport._cython import inner_encode_rsp

from tests.protocol._fakes import FakeTransport
from tests.protocol._fakes import ParserFactory
from tests.protocol._fakes import unary_frame
from trpc_sdk.transport import ConnectionOption
from trpc_sdk.transport import DefaultIoConnection


class _App:
    """I/O-process stand-in: the tables SeparateClientConnection writes."""

    def __init__(self, loop):
        self.client_connections = {}
        self.session_id = 0
        self.loop = loop
        self.worker_connections = [None]
        self.rpc_task_alive = [True]
        self.rpcz_span_map = {}
        self.exiting = False
        self.dead = []

    def mark_rpc_task_dead(self, index):
        self.dead.append(index)
        if 0 <= index < len(self.rpc_task_alive):
            self.rpc_task_alive[index] = False


class _Sock:
    def fileno(self):
        return 7


def _transport():
    transport = FakeTransport()
    extra = {
        "socket": _Sock(),
        "peername": ("127.0.0.1", 9),
        "sockname": ("127.0.0.1", 8000),
    }
    transport.get_extra_info = lambda key, default=None: extra.get(key, default)
    return transport


def _client(loop, parser=None):
    return SeparateClientConnection(
        _App(loop),
        ("127.0.0.1", 8000),
        parser or get_server_parser("trpc"),
        index=0,
    )


@pytest.fixture(autouse=True)
def _clear_scheduler():
    register_dispatcher_worker_func(None)
    register_dispatcher_worker_func(None, True)
    yield
    register_dispatcher_worker_func(None)
    register_dispatcher_worker_func(None, True)


@pytest.mark.asyncio
async def test_conn_idle_check():
    loop = asyncio.get_running_loop()
    conn = _client(loop)
    conn.check_idle_timeout = 20
    transport = _transport()
    conn.connection_made(transport)
    conn.last_recv_time = time.time() - 1

    await asyncio.sleep(0.08)

    assert conn.fd not in conn.app.client_connections
    assert transport.closing is True


@pytest.mark.asyncio
async def test_conn_idle_check_lost():
    loop = asyncio.get_running_loop()
    conn = _client(loop)
    conn.check_idle_timeout = 20
    transport = _transport()
    conn.connection_made(transport)
    conn.connection_lost(None)

    await asyncio.sleep(0.05)

    assert conn.fd not in conn.app.client_connections
    assert transport.closing is True


def test_connection_made_exception_does_not_keep_the_client():
    loop = asyncio.new_event_loop()
    try:
        conn = _client(loop)
        transport = _transport()

        def boom(key, default=None):
            if key == "socket":
                raise RuntimeError("mock socket error")
            return {"peername": ("127.0.0.1", 9), "sockname": ("127.0.0.1", 8000)}.get(key, default)

        transport.get_extra_info = boom
        conn.connection_made(transport)
        assert conn.app.client_connections == {}
    finally:
        loop.close()


def test_data_received_parser_exception_closes_client():
    loop = asyncio.new_event_loop()
    try:

        class _Fatal:
            def feed_data(self, _data):
                return None

            def feeds_data(self, _data):
                return None

            def read(self):
                raise RuntimeError("parser fatal")

            def reset(self):
                return None

        class _Factory:
            def create(self):
                return _Fatal()

        conn = _client(loop, _Factory())
        transport = _transport()
        conn.connection_made(transport)
        conn.data_received(b"some_data")
        assert conn.fd not in conn.app.client_connections
        assert transport.closing is True
    finally:
        loop.close()


def test_data_received_worker_write_exception_closes_client():
    loop = asyncio.new_event_loop()
    try:
        conn = _client(loop, ParserFactory())
        transport = _transport()
        conn.connection_made(transport)
        conn.parser.frames = [unary_frame()]

        def boom(_packages):
            raise RuntimeError("worker write")

        conn._forward_frames_to_workers = boom
        conn.data_received(b"frame")
        assert conn.fd not in conn.app.client_connections
    finally:
        loop.close()


def test_worker_response_is_written_to_the_matching_client():
    loop = asyncio.new_event_loop()
    try:
        app = _App(loop)
        client = FakeTransport()
        app.client_connections[1] = ClientConnectionInfo(session_id=1, conn=client)
        worker = SeparateRpcTaskConnection(app, 0)
        pipe = FakeTransport()
        worker.connection_made(pipe)
        assert app.worker_connections[0] is pipe

        worker.data_received(inner_encode_rsp(1, 1, 0, b"rsp"))
        assert client.written == [b"rsp"]

        worker.connection_lost(RuntimeError("gone"))
        assert app.dead == [0]
        assert pipe.closing is True
    finally:
        loop.close()


def test_get_target_worker():
    unary = SchedulerParams(worker_conn_num=1, remote_ip="", session_id=1, stream_id=1)
    register_dispatcher_worker_func(lambda worker_conn_num: worker_conn_num)
    assert get_next_worker(unary) == 1

    register_dispatcher_worker_func(lambda data: data.worker_conn_num, True)
    assert get_next_worker(unary) == 1

    sticky = SchedulerParams(
        worker_conn_num=2,
        remote_ip="",
        session_id=1,
        stream_id=1,
        frame_type=trpc_pb2.TRPC_STREAM_FRAME,
    )
    assert get_next_worker(sticky) == 0
    assert select_rpc_task(sticky, [False, True]) is None


@pytest.mark.asyncio
async def test_default_io_connection_idle_closes():
    """Merge mode uses DefaultIoConnection for the same idle-close job."""

    async def _noop(_conn, _frame):
        return None

    conn = DefaultIoConnection(
        ConnectionOption(parser=ParserFactory(), msg_handler=_noop, connection_idle_timeout_ms=20))
    transport = FakeTransport()
    conn.connection_made(transport)
    conn.last_recv_time = time.monotonic() - 10
    await asyncio.sleep(0.08)
    assert conn._is_closed is True
    assert transport.closing is True
