# -*- coding: utf-8 -*-
"""Test client transport module"""
import asyncio
import socket
import time

import pytest

from tests.common_util import BaseTestProtocol
from tests.common_util import create_test_connection
from tests.transport_util import TestConn
from tests.transport_util import TestPool
from trpc_sdk.codec import ClientCodec
from trpc_sdk.codec import Message
from trpc_sdk.codec import get_message
from trpc_sdk.codec import new_message
from trpc_sdk.context import Context
from trpc_sdk.exceptions import ClientInvokeError
from trpc_sdk.exceptions import ClientTimeoutError
from trpc_sdk.exceptions import ServerSystemError
from trpc_sdk.pool.connpool import _default_connection_pools
from trpc_sdk.pool.multiplexed import new_multiplexed
from trpc_sdk.transport import udp_connection
from trpc_sdk.transport.client_transport import ClientTaskStreamTransport
from trpc_sdk.transport.client_transport import ClientTransport
from trpc_sdk.transport.options import RoundTripOptions
from trpc_sdk.utils import get_or_create_event_loop


@pytest.mark.asyncio
async def test_client_transport_with_multiplexed():
    """Test client transport"""
    client_transport = ClientTransport()
    assert len(client_transport.stream_id_to_conn) == 0
    client_opts = RoundTripOptions()
    client_opts.set_network('tcp')
    client_opts.set_enable_multiplexed(True)
    client_opts.set_multiplexed(new_multiplexed())
    ctx = Context()
    msg = get_message(ctx)
    msg.request_id = 1
    client_opts.set_msg(msg)
    with pytest.raises(ClientInvokeError):
        await client_transport.round_trip(ctx, b'123', client_opts)


@pytest.mark.asyncio
async def test_client_transport_with_tcp():
    """Test client transport"""
    client_transport = ClientTransport()
    assert len(client_transport.stream_id_to_conn) == 0
    client_opts = RoundTripOptions()
    client_opts.set_network('tcp')
    client_opts.set_enable_multiplexed(False)
    ctx = Context()
    msg = get_message(ctx)
    msg.request_id = 1
    client_opts.set_msg(msg)
    with pytest.raises(ClientInvokeError):
        await client_transport.round_trip(ctx, b'123', client_opts)


@pytest.mark.asyncio
async def test_client_transport_with_udp():
    """Test client transport"""
    client_transport = ClientTransport()
    assert len(client_transport.stream_id_to_conn) == 0
    client_opts = RoundTripOptions()
    client_opts.set_network('udp')
    client_opts.set_enable_multiplexed(False)
    client_opts.set_address(("127.0.0.1", 8000))
    ctx = Context()
    msg = get_message(ctx)
    msg.request_id = 1
    client_opts.set_msg(msg)
    with pytest.raises(ConnectionRefusedError):
        await client_transport.round_trip(ctx, b'123', client_opts)


def test_client_transport_send_error():
    """Test client transport"""
    client_transport = ClientTransport()
    assert len(client_transport.stream_id_to_conn) == 0
    client_opts = RoundTripOptions()
    client_opts.set_network('udp')
    client_opts.set_enable_multiplexed(False)
    client_opts.set_address(("127.0.0.1", 8000))
    ctx = Context()
    msg = get_message(ctx)
    msg.request_id = 1
    msg.stream_id = 1
    client_opts.set_msg(msg)
    with pytest.raises(ServerSystemError):
        client_transport.send(ctx, b'123', client_opts)
    client_transport.close(ctx)


@pytest.mark.asyncio
async def test_client_transport_recv_error():
    """Test client transport"""
    client_transport = ClientTransport()
    assert len(client_transport.stream_id_to_conn) == 0
    client_opts = RoundTripOptions()
    client_opts.set_network('udp')
    client_opts.set_enable_multiplexed(False)
    client_opts.set_address(("127.0.0.1", 8000))
    ctx = Context()
    msg = get_message(ctx)
    msg.request_id = 1
    msg.stream_id = 1
    client_opts.set_msg(msg)
    with pytest.raises(ServerSystemError):
        await client_transport.recv(ctx, client_opts)
    client_transport.close(ctx)


@pytest.mark.asyncio
async def test_client_transport_start_error():
    """Test client transport"""
    client_transport = ClientTransport()
    assert len(client_transport.stream_id_to_conn) == 0
    client_opts = RoundTripOptions()
    client_opts.set_network('udp')
    client_opts.set_enable_multiplexed(False)
    client_opts.set_address(("127.0.0.1", 8088))
    ctx = Context()
    msg = get_message(ctx)
    msg.request_id = 1
    msg.stream_id = 1
    client_opts.set_msg(msg)
    with pytest.raises(ConnectionRefusedError):
        await client_transport.start(ctx, client_opts)
    client_transport.close(ctx)


class _TestProtocol(BaseTestProtocol):
    """Test protocol"""

    def __init__(self, on_con_lost: asyncio.Future):
        super().__init__(on_con_lost)

    def data_received(self, data: bytes):
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
        client_transport.send(ctx, b'abc', client_opts)
        with pytest.raises(ClientInvokeError):
            await client_transport.recv(ctx, client_opts)
        client_transport.close(ctx)

    await create_test_connection(_TestProtocol, test_cb)
    client_transport.close(ctx)


@pytest.mark.asyncio
async def test_client_transport_stream():
    """Test client transport stream"""
    data = ''.encode('utf8')
    ctx = Context()
    ctx, msg = new_message(ctx)
    msg.stream_id = 100
    msg.callee_service_name = '/trpc_sdk.test.helloworld.Greeter'
    msg.client_rpc_name = '/trpc_sdk.test.helloworld.Greeter/SayHi'
    client_codec = ClientCodec()
    client_codec.encode(msg, data)

    options = RoundTripOptions()
    options.set_address(('127.0.0.1', 8000))
    options.pool = TestPool()

    options.set_codec(None)

    loop = get_or_create_event_loop()
    _default_connection_pools.get(loop, None)
    _default_connection_pools[loop] = TestPool()
    client_transport = ClientTransport()
    try:
        await client_transport.start(ctx, options)
        client_transport.send(ctx, b'', options)

        ctx.deadline = int(time.time() + 2) * 1000
        client_transport.stream_id_to_conn[100].push_data_to_recv_queue(b'')
        await client_transport.recv(ctx, options)

        client_transport.close(ctx)
    except Exception:  # pylint: disable=broad-except
        assert False

    try:
        client_transport.send(ctx, b'', options)
    except Exception as ex:  # pylint: disable=broad-except
        assert str(ex) == 'Connection is Closed'

    try:
        await client_transport.recv(ctx, options)
    except Exception as ex:  # pylint: disable=broad-except
        assert str(ex) == 'Connection is Closed'


async def _mock_open_datagram_endpoint(host=None, port=None, *, loop=None, **kwds):
    """Mock udp"""
    return TestConn()


@pytest.mark.asyncio
async def test_client_transport():
    """Test client transport"""
    data = ''.encode('utf8')
    msg = Message()
    msg.callee_service_name = '/trpc_sdk.test.helloworld.Greeter'
    msg.client_rpc_name = '/trpc_sdk.test.helloworld.Greeter/SayHi'
    client_codec = ClientCodec()
    req_data = client_codec.encode(msg, data)

    options = RoundTripOptions()
    options.set_address(('127.0.0.1', 8000))
    options.pool = TestPool()

    options.set_codec(None)

    loop = get_or_create_event_loop()
    old_pool = _default_connection_pools.get(loop, None)
    _default_connection_pools[loop] = TestPool()
    client_transport = ClientTransport()
    ctx = Context()
    try:
        await client_transport.round_trip(ctx, req_data, options)
    except ClientTimeoutError as err:
        assert err.msg == "client reached deadline"

    ctx.deadline = int(time.time() + 2) * 1000
    rsp = await client_transport.round_trip(ctx, req_data, options)
    assert rsp == b'\t0\x00\x00\x00\x00\x00\x1f\x00\x02\x00\x00\x00\x00\x00\x00\x18\x01\n\x0bhi: hello: '

    ctx.timeout = 1000
    rsp = await client_transport.round_trip(ctx, req_data, options)
    assert rsp == b'\t0\x00\x00\x00\x00\x00\x1f\x00\x02\x00\x00\x00\x00\x00\x00\x18\x01\n\x0bhi: hello: '

    options.set_network("udp")
    udp_connection.open_datagram_endpoint = _mock_open_datagram_endpoint
    try:
        rsp = await client_transport.round_trip(ctx, req_data, options)
        assert rsp == b'hi: hello: '
    except ConnectionRefusedError as err:
        assert err.errno == 111

    _default_connection_pools[loop] = old_pool


class _MockMultiplexedConn:
    """Mock connection returned by multiplexed.get_conn"""

    def __init__(self):
        self._rsp = b'\t0\x00\x00\x00\x00\x00\x1f\x00\x02\x00\x00\x00\x00\x00\x00\x18\x01\n\x0bhi: hello: '

    def write(self, data):
        pass

    async def read(self):
        return self._rsp

    def close(self, force=False):
        pass


class _MockMultiplexed:
    """Mock multiplexed pool that returns a mock connection"""

    async def get_conn(self, network, address, request_id, msg, checker, codec, timeout):
        return _MockMultiplexedConn()


@pytest.mark.asyncio
async def test_tcp_multiplexed_covers_time_lines():
    """Test tcp_multiplexed path to cover the time.time() lines changed for Python 3.6 compat."""
    from trpc_sdk.transport.client_transport_tcp import tcp_multiplexed

    ctx = Context()
    ctx.deadline = int(time.time() + 5) * 1000
    msg = Message()
    msg.request_id = 1

    options = RoundTripOptions()
    options.set_network('tcp')
    options.set_address(('127.0.0.1', 8000))
    options.set_enable_multiplexed(True)
    options.set_multiplexed(_MockMultiplexed())
    options.set_msg(msg)

    rsp = await tcp_multiplexed(ctx, b'test_data', options)
    assert rsp is not None
    assert ctx.client_post_sched_send_msg_us > 0
    assert ctx.client_pre_io_send_msg_us > 0
    assert ctx.client_post_io_send_msg_us > 0
    assert ctx.client_post_sched_recv_msg_us > 0
