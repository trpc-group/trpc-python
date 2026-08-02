# -*- coding: utf-8 -*-
"""Test client transport stream module"""
import asyncio
import socket

import pytest

from tests.common_util import BaseTestProtocol
from tests.common_util import create_test_connection
from tests.transport_util import TestChecker
from tests.transport_util import TestConn
from trpc_sdk.abc.codec import Parser
from trpc_sdk.codec import TrpcServerParser
from trpc_sdk.codec import get_message
from trpc_sdk.context import Context
from trpc_sdk.exceptions import ClientInvokeError
from trpc_sdk.transport.client_transport import ClientTaskStreamTransport
from trpc_sdk.transport.client_transport_stream import ClientConn
from trpc_sdk.transport.options import RoundTripOptions


class _TestProtocol(BaseTestProtocol):
    """Test protocol"""

    def data_received(self, data):
        assert data.decode() == "abc"
        self.transport.write(b'abc')
        self.transport.write_eof()


@pytest.mark.asyncio
async def test_client_stream_transport():
    """Test client transport"""
    client_transport = ClientTaskStreamTransport()
    assert len(client_transport.stream_id_to_conn) == 0
    client_opts = RoundTripOptions()
    client_opts.set_network('tcp')
    client_opts.set_enable_multiplexed(False)
    client_opts.set_address(("127.0.0.1", 8000))
    ctx = Context()
    msg = get_message(ctx)
    msg.request_id = 1
    msg.stream_id = 1
    client_opts.set_msg(msg)
    with pytest.raises(RuntimeError):
        await client_transport.round_trip(ctx, b'123', client_opts)

    async def test_cb(loop: asyncio.BaseEventLoop, wsock: socket.socket):
        client_opts.set_address(wsock)
        await client_transport.start(ctx, client_opts)
        conn = client_transport.stream_id_to_conn.get(1, None)
        assert conn is not None
        assert len(conn.recv_queue.queue) == 0
        conn.push_data_to_recv_queue(b'123')
        assert len(conn.recv_queue.queue) == 1
        client_transport.send(ctx, b'abc', client_opts)
        await conn.recv()
        with pytest.raises(ClientInvokeError):
            await client_transport.recv(ctx, client_opts)
        client_transport.close(ctx)

    await create_test_connection(_TestProtocol, test_cb)
    client_transport.close(ctx)


class _TestExceptionChecker(Parser):

    def create(self):
        """Create parser"""
        return TrpcServerParser()

    async def read_frame(self, conn) -> bytes:
        """Read frame"""
        raise RuntimeError('test err')  # pylint: disable=broad-except


@pytest.mark.asyncio
async def test_recv_frame():
    """Test recv frame"""
    client_conn = ClientConn(None, None)
    client_conn.conn = TestConn()
    client_conn.options = RoundTripOptions()
    client_conn.options.checker = TestChecker()
    ctx = Context()
    ctx.deadline = 0
    try:
        await client_conn.recv_frame(ctx)
    except Exception:  # pylint: disable=broad-except
        assert True

    try:
        client_conn.options.checker = _TestExceptionChecker()
        client_conn.conn = TestConn()
        await client_conn.recv_frame(ctx)
    except Exception:  # pylint: disable=broad-except
        assert True
