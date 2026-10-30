# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.
"""``DefaultIoConnection`` lifecycle, and which connection class ``protocol_factory`` builds."""

from __future__ import annotations

import asyncio
from typing import List

from tests.protocol._fakes import ParserFactory
from tests.protocol._fakes import FakeTransport
from trpc_sdk.abc import BaseServerTransportOption
from trpc_sdk.transport import TCPConnection
from trpc_sdk.transport import ConnectionOption
from trpc_sdk.transport import DefaultIoConnection
from trpc_sdk.transport import TrpcServerTransport


async def _noop(conn, frame):
    return None


def _connection(msg_handler=_noop, **kwargs) -> DefaultIoConnection:
    conn = DefaultIoConnection(ConnectionOption(parser=ParserFactory(), msg_handler=msg_handler, **kwargs))
    conn.connection_made(FakeTransport())
    return conn


def test_each_connection_creates_its_own_parser():
    option = ConnectionOption(parser=ParserFactory(), msg_handler=_noop)
    first, second = DefaultIoConnection(option), DefaultIoConnection(option)
    assert first.parser is not second.parser
    assert option.connection_close_handler is None


async def test_msg_handler_runs_once_per_frame():
    seen: List[bytes] = []

    async def handle(conn, frame):
        seen.append(frame)

    conn = _connection(handle)
    conn.data_received(b"a")
    conn.data_received(b"b")
    await asyncio.sleep(0)
    assert seen == [b"a", b"b"]
    assert conn.local_addr == ("127.0.0.1", 8000)
    assert conn.remote_addr == ("127.0.0.1", 50000)


def test_unix_and_missing_addresses_are_normalized():

    class _UnixTransport(FakeTransport):

        def get_extra_info(self, key, default=None):
            return {"sockname": "/tmp/trpc.sock", "peername": ""}.get(key, default)

    conn = DefaultIoConnection(ConnectionOption(parser=ParserFactory(), msg_handler=_noop))
    conn.connection_made(_UnixTransport())
    assert conn.local_addr == ("/tmp/trpc.sock", 0)
    assert conn.remote_addr == ("0.0.0.0", 0)


def test_write_and_is_closing_go_through_the_transport():
    conn = _connection()
    conn.write(b"rsp")
    assert conn.aio_conn.written == [b"rsp"]
    assert not conn.is_closing()
    conn.close()
    assert conn.is_closing()


def test_is_closing_before_connection_made():
    conn = DefaultIoConnection(ConnectionOption(parser=ParserFactory(), msg_handler=_noop))
    assert conn.is_closing()


async def test_msg_handler_error_keeps_connection_open():

    async def handle(conn, frame):
        raise RuntimeError("boom")

    conn = _connection(handle)
    conn.data_received(b"a")
    await asyncio.sleep(0)
    assert not conn.aio_conn.is_closing()


def test_close_is_idempotent_and_calls_back_once():
    closed: List[DefaultIoConnection] = []
    conn = _connection(connection_close_handler=closed.append)
    conn.close()
    conn.connection_lost(None)
    conn.close()
    assert closed == [conn]
    assert conn.aio_conn.is_closing()


async def test_idle_timeout_closes_connection():
    closed: List[DefaultIoConnection] = []
    conn = _connection(connection_idle_timeout_ms=10, connection_close_handler=closed.append)
    assert conn.check_task is not None
    await asyncio.sleep(0.05)
    assert closed == [conn]
    assert conn.aio_conn.is_closing()
    assert conn.check_task is None


def _bound(transport: TrpcServerTransport) -> TrpcServerTransport:
    transport.bind(BaseServerTransportOption(parser=ParserFactory(), rpc_handler=_noop, idle_timeout=1000))
    return transport


def test_protocol_factory_defaults_to_cython_connection():
    assert isinstance(_bound(TrpcServerTransport()).protocol_factory()(), TCPConnection)


class _Echo(TrpcServerTransport):

    async def _msg_handler(self, conn, data):
        conn.write(data)


def test_protocol_factory_ignores_msg_handler_override_without_setter():
    assert isinstance(_bound(_Echo()).protocol_factory()(), TCPConnection)


def test_protocol_factory_uses_python_connection_after_set_io_connection():
    transport = _bound(_Echo())
    transport.set_io_connection()
    conn = transport.protocol_factory()()
    assert isinstance(conn, DefaultIoConnection)
    assert conn.option.msg_handler == transport._msg_handler


def test_set_io_connection_accepts_subclass():

    class _MyConnection(DefaultIoConnection):
        pass

    transport = _bound(_Echo())
    transport.set_io_connection(_MyConnection)
    assert isinstance(transport.protocol_factory()(), _MyConnection)
