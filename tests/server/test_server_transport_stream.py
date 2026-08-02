# -*- coding: utf-8 -*-
"""Test server stream transport"""

import asyncio
import socket

from functools import partial

import pytest

from tests.common_util import create_test_connection
from trpc_sdk.abc.server import BaseTrpcServerEngine  # pylint: disable=no-name-in-module
from trpc_sdk.abc.transport import BaseServerTransport
from trpc_sdk.abc.transport import BaseServerTransportOption
from trpc_sdk.codec.message import get_message  # pylint: disable=no-name-in-module
from trpc_sdk.context import Context
from trpc_sdk.exceptions import ServerSystemError
from trpc_sdk.transport.server_transport_stream import TaskServerStreamTransport
from trpc_sdk.transport.server_transport_stream import TrpcServerStreamTransport
from trpc_sdk.utils import get_or_create_event_loop


class MockTrpcServerEngine(BaseTrpcServerEngine):
    """Mock server engine"""

    def close(self):
        """close"""
        return


class MockTrpcServerTransport(BaseServerTransport):
    """Mock trpc server transport"""

    def __init__(self):
        self.opts = BaseServerTransportOption()
        self.loop = None
        self.server_engine = MockTrpcServerEngine()
        self.worker_process = False
        self.dispatcher_process = False
        self.process_mode = "dispatcher"
        self.is_close = False

    def listen_and_serve(self, opts: BaseServerTransportOption, server_engine: BaseTrpcServerEngine):
        self.opts = opts
        self.server_engine = server_engine

        self.loop = get_or_create_event_loop()

        if ("tcp" in self.opts.network and self.opts.mode == "multi-listener") or ("udp" in self.opts.network):
            self.listen_and_serve_multi_listener()
        else:
            self.listen_and_serve_worker_dispatcher()

    def listen_and_serve_worker_dispatcher(self):
        """dispatcher mode"""
        self.process_mode = "dispatcher"
        # set worker process
        self.set_worker_process()

        # set dispatcher process
        self.set_dispatcher_process()

    def set_worker_process(self):
        """worker process"""
        self.worker_process = True

    def set_dispatcher_process(self):
        """dispatcher process"""
        self.dispatcher_process = True

    def listen_and_serve_multi_listener(self):
        """multi-listener mode"""
        # set executor process
        self.set_executor_process()

    def set_executor_process(self):
        """merge mode"""
        self.process_mode = "multi-listener"

    def close(self):
        """close"""
        self.is_close = True
        self.server_engine.close()


class MockTrpcServerStreamTransport(TrpcServerStreamTransport):
    """Mock trpc server stream transport"""

    def __init__(self):
        super().__init__()
        self.server_transport = MockTrpcServerTransport()

    def close_stream(self, session_id: int, stream_id: int):
        """Close stream"""
        if stream_id in self.addr_to_conn[session_id]:
            self.addr_to_conn[session_id][stream_id].close()


class _TestProtocol(asyncio.Protocol):
    """Test protocol"""

    def __init__(self, server_transport: MockTrpcServerStreamTransport, on_con_lost: asyncio.Future):
        self.transport = None
        self.server_transport = server_transport
        self.on_con_lost: asyncio.Future = on_con_lost

    def connection_made(self, transport):
        self.transport = transport
        ctx = Context()
        ctx.with_meta_data("aio_transport", transport)
        msg = get_message(ctx)
        msg.session_id, msg.stream_id = 0, 1
        self.server_transport.store_stream_conn(ctx)
        with pytest.raises(ServerSystemError):
            self.server_transport.store_stream_conn(ctx)

    def data_received(self, data):
        assert data.decode() == "abc"

    def connection_lost(self, exc):
        ctx = Context()
        msg = get_message(ctx)
        msg.session_id, msg.stream_id = 0, 1
        self.server_transport.delete_stream_conn(ctx)
        self.server_transport.delete_stream_conn(ctx)
        # The socket has been closed
        self.on_con_lost.set_result(True)


@pytest.mark.asyncio
async def test_trpc_server_stream_transport():
    """Test trpc server stream"""
    mock_transport = MockTrpcServerStreamTransport()
    server_engine = MockTrpcServerEngine()
    opts = BaseServerTransportOption()
    opts.network = "tcp"
    opts.mode = "multi-listener"
    mock_transport.listen_and_serve(opts, server_engine)
    assert mock_transport.server_transport.process_mode == opts.mode
    mock_transport.listen_and_serve_worker_dispatcher()
    assert mock_transport.server_transport.process_mode == "dispatcher"
    mock_transport.listen_and_serve_multi_listener()
    assert mock_transport.server_transport.process_mode == opts.mode

    async def test_cb(loop: asyncio.BaseEventLoop, wsock: socket.socket):
        ctx = Context()
        ctx.with_meta_data("session_id", 0)
        ctx.with_meta_data("fd", 0)
        msg = get_message(ctx)
        msg.session_id, msg.stream_id = 0, 1
        mock_transport.send(ctx, 'efg'.encode(), 1)
        wsock.recv(8)
        mock_transport.close_stream(0, 1)
        await asyncio.sleep(0.1)

    handler = partial(_TestProtocol, server_transport=mock_transport)
    await create_test_connection(handler, test_cb)
    mock_transport.close()
    assert mock_transport.server_transport.is_close is True


class MockTaskServerStreamTransport(TaskServerStreamTransport):
    """Mock trpc server stream transport"""

    def close_stream(self, session_id: int, stream_id: int):
        """Close stream"""
        if stream_id in self.addr_to_conn[session_id]:
            self.addr_to_conn[session_id][stream_id].close()


@pytest.mark.asyncio
async def test_trpc_task_stream_transport():
    """Test trpc server stream"""
    mock_transport = MockTaskServerStreamTransport()
    server_engine = MockTrpcServerEngine()
    opts = BaseServerTransportOption()
    opts.network = "tcp"
    opts.mode = "multi-listener"
    with pytest.raises(NotImplementedError):
        mock_transport.listen_and_serve(opts, server_engine)
    with pytest.raises(NotImplementedError):
        mock_transport.listen_and_serve_worker_dispatcher()
    with pytest.raises(NotImplementedError):
        mock_transport.listen_and_serve_multi_listener()
    with pytest.raises(NotImplementedError):
        mock_transport.close()

    async def test_cb(loop: asyncio.BaseEventLoop, wsock: socket.socket):
        loop.call_soon(wsock.send, 'abc'.encode())
        ctx = Context()
        ctx.with_meta_data("session_id", 0)
        ctx.with_meta_data("fd", 0)
        msg = get_message(ctx)
        msg.session_id, msg.stream_id = 0, 100
        with pytest.raises(ValueError):
            mock_transport.send(ctx, 'efg'.encode(), -10)
        msg.session_id, msg.stream_id = 0, 1
        mock_transport.send(ctx, 'efg'.encode(), 1)
        wsock.recv(8)
        mock_transport.close_stream(0, 1)
        await asyncio.sleep(0.1)

    handler = partial(_TestProtocol, server_transport=mock_transport)
    await create_test_connection(handler, test_cb)
