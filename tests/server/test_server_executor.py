# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.
"""Merge worker and datagram connection that replaced ExecutorProcess."""

import asyncio
from unittest.mock import MagicMock

import pytest

from trpc_sdk.abc import BaseServerTransportOption
from trpc_sdk.exceptions import ServerDecodeError
from trpc_sdk.runtime import MergeWorker
from trpc_sdk.server import ServerOption
from trpc_sdk.server import ServiceAdapter
from trpc_sdk.server import ServiceOption
from trpc_sdk.server import TrpcServerAdapter
from trpc_sdk.task import TaskWorkerConnection
from trpc_sdk.transport import ConnectionOption
from trpc_sdk.transport import DefaultDatagramConnection
from trpc_sdk.transport import TrpcServerTransport
from trpc_sdk.transport import get_protocol_handle
from trpc_sdk.transport import register_protocol_handle


class _Parser:
    def __init__(self, frames=None, error=None):
        self.frames = list(frames or [])
        self.error = error
        self.reset_count = 0

    def feed_data(self, data):
        if not self.frames and self.error is None:
            self.frames.append(data)

    def feeds_data(self, data):
        self.feed_data(data)

    def read(self):
        if self.error is not None:
            raise self.error
        frames, self.frames = self.frames, []
        return frames

    def reset(self):
        self.reset_count += 1
        self.frames = []


class _Factory:
    def __init__(self, parser):
        self.parser = parser

    def create(self):
        return self.parser


def test_merge_worker_reads_endpoint_fields_from_transport_opt():
    opt = ServiceOption()
    opt.set_address(("127.0.0.1", 18081))
    opt.set_network("tcp")
    opt.set_protocol("trpc")
    opt.set_timeout(1000)
    service = ServiceAdapter(opt)
    transport_opt = opt.transport_option
    transport_opt.service_adapter = service
    transport_opt.rpc_handler = service.handle

    adapter = TrpcServerAdapter(ServerOption())
    adapter.process_info.add_transport_opt(transport_opt)
    adapter.process_info.add_transport(object())
    worker = MergeWorker(adapter)

    assert worker.addresses == [("127.0.0.1", 18081)]
    assert worker.networks == ["tcp"]
    assert worker.handlers == [service.handle]
    assert worker.timeouts == [1000]
    assert worker.services == [service]
    assert worker.parsers[0] is not None


def test_trpc_transport_rejects_a_network_it_cannot_serve():
    transport = TrpcServerTransport()
    opt = BaseServerTransportOption()
    opt.set_network("http")
    opt.set_address(("127.0.0.1", 1))
    transport.bind(opt)
    assert transport.check_endpoint() is False

    udp = TrpcServerTransport()
    bad = BaseServerTransportOption()
    bad.set_network("udp")
    bad.set_address(("127.0.0.1", -1))
    udp.bind(bad)
    assert udp.check_endpoint() is False


def test_user_protocol_handle_is_registered_for_the_worker():
    def starter(worker, index):
        return index

    register_protocol_handle("http_test_unit", starter)
    assert get_protocol_handle("http_test_unit") is starter
    assert get_protocol_handle("missing_protocol_unit") is None


@pytest.mark.asyncio
async def test_datagram_connection_dispatches_and_survives_decode_errors():
    seen = []

    async def handle(conn, frame):
        seen.append((conn.remote_addr, frame))

    parser = _Parser()
    conn = DefaultDatagramConnection(ConnectionOption(parser=_Factory(parser), msg_handler=handle))
    transport = MagicMock()
    transport.get_extra_info.return_value = ("127.0.0.1", 8000)
    transport.is_closing.return_value = False
    conn.connection_made(transport)
    conn.datagram_received(b"hello", ("127.0.0.1", 9))
    await asyncio.sleep(0)
    assert seen == [(("127.0.0.1", 9), b"hello")]

    failing = _Parser(error=ServerDecodeError("bad"))
    bad = DefaultDatagramConnection(ConnectionOption(parser=_Factory(failing), msg_handler=handle))
    bad.connection_made(transport)
    bad.datagram_received(b"x", ("127.0.0.1", 1))
    assert failing.reset_count == 1
    assert transport.close.called is False


@pytest.mark.asyncio
async def test_datagram_handler_error_does_not_close_the_socket():
    async def handle(_conn, _frame):
        raise RuntimeError("proc")

    parser = _Parser(frames=[b"pkg"])
    conn = DefaultDatagramConnection(ConnectionOption(parser=_Factory(parser), msg_handler=handle))
    transport = MagicMock()
    transport.get_extra_info.return_value = ("127.0.0.1", 8000)
    transport.is_closing.return_value = False
    conn.connection_made(transport)
    conn.datagram_received(b"y", ("127.0.0.1", 2))
    await asyncio.sleep(0)
    transport.close.assert_not_called()


def test_task_worker_connection_decode_and_close(monkeypatch):
    received = []
    monkeypatch.setattr(
        "trpc_sdk.task._connection.receive_pkg_from_worker",
        lambda index, package: received.append((index, package)),
    )
    conn = TaskWorkerConnection(app=None, index=1)
    transport = MagicMock()
    transport.is_closing.return_value = False
    conn.connection_made(transport)

    conn.parser = _Parser(error=ServerDecodeError("d"))
    conn.parser.feeds_data = lambda data: None
    conn.data_received(b"x")
    assert received == []

    conn.parser = _Parser(frames=[b"pkg"])
    conn.data_received(b"y")
    assert received == [(1, b"pkg")]

    conn.connection_lost(None)
    transport.close.assert_called()
