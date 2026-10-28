# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.
"""Test client transport module"""
import asyncio
from types import SimpleNamespace

import pytest

from trpc_sdk.abc import TrpcContext
from trpc_sdk.codec import create_message
from trpc_sdk.exceptions import ClientInvokeError
from trpc_sdk.exceptions import ClientTimeoutError
from trpc_sdk.exceptions import ServerSystemError
from trpc_sdk.transport import ClientTransport
from trpc_sdk.transport import RoundTripOptions
from trpc_sdk.transport._client._client_transport_tcp import tcp_round_trip
from trpc_sdk.utils import now_ms


def _context(stream_id=0):
    ctx, msg = create_message(TrpcContext())
    msg.set_request_id(1)
    if stream_id:
        msg.set_stream_id(stream_id)
    return ctx, msg


class _FakeConn:
    """Stand-in for a pooled connection."""

    def __init__(self, frame=b"frame"):
        self.frame = frame
        self.written = b""
        self.closed = False
        self.transport = SimpleNamespace(get_extra_info=lambda key: ("127.0.0.1", 8000))

    def write(self, data):
        self.written += data

    def close(self, force=False):
        self.closed = True


class _FrameParser:
    def __init__(self, frame=b"frame", error=None):
        self.frame = frame
        self.error = error

    async def read_frame(self, conn):
        if self.error is not None:
            raise self.error
        return self.frame


class _UdpConn:
    def __init__(self, payload):
        self.payload = payload
        self.transport = SimpleNamespace(get_extra_info=lambda key: ("127.0.0.1", 8000))

    def write(self, data):
        self.written = data

    async def receive(self):
        return self.payload, ("127.0.0.1", 8000)

    def close(self):
        self.closed = True


def _tcp_options(parser=None):
    options = RoundTripOptions()
    options.set_network("tcp")
    options.set_address(("127.0.0.1", 8000))
    if parser is not None:
        options.set_parser(parser)
    return options


@pytest.mark.asyncio
async def test_client_transport_rejects_unknown_network():
    """Networks other than tcp and udp are rejected. Multiplexed mode is gone."""
    client_transport = ClientTransport()
    assert len(client_transport.stream_id_to_conn) == 0
    client_opts = RoundTripOptions()
    client_opts.set_network("sctp")
    ctx, _msg = _context()
    with pytest.raises(ValueError, match="error network type"):
        await client_transport.round_trip(ctx, b"123", client_opts)


@pytest.mark.asyncio
async def test_client_transport_with_tcp(monkeypatch):
    """A connect failure on the TCP path becomes ClientInvokeError."""
    async def _refused(*_args, **_kwargs):
        raise ConnectionRefusedError(111, "Connection refused")

    monkeypatch.setattr(
        "trpc_sdk.transport._client._client_transport_tcp.get_pool_connection",
        _refused,
    )
    client_transport = ClientTransport()
    ctx, _msg = _context()
    with pytest.raises(ClientInvokeError):
        await client_transport.round_trip(ctx, b"123", _tcp_options())


@pytest.mark.asyncio
async def test_client_transport_with_udp():
    """UDP recv against a closed port raises ConnectionRefusedError."""
    client_transport = ClientTransport()
    client_opts = RoundTripOptions()
    client_opts.set_network("udp")
    client_opts.set_address(("127.0.0.1", 1))
    ctx, _msg = _context()
    ctx.timeout = 200
    ctx.deadline = now_ms() + 1000
    with pytest.raises(ConnectionRefusedError):
        await client_transport.round_trip(ctx, b"123", client_opts)


def test_client_transport_send_error():
    """Send without a started stream reports a closed connection."""
    client_transport = ClientTransport()
    assert len(client_transport.stream_id_to_conn) == 0
    ctx, _msg = _context(stream_id=1)
    with pytest.raises(ServerSystemError, match="Connection is Closed"):
        client_transport.send(ctx, b"123", RoundTripOptions())
    client_transport.close(ctx)


@pytest.mark.asyncio
async def test_client_transport_recv_error():
    """Recv without a started stream reports a closed connection."""
    client_transport = ClientTransport()
    ctx, _msg = _context(stream_id=1)
    with pytest.raises(ServerSystemError, match="Connection is Closed"):
        await client_transport.recv(ctx, RoundTripOptions())
    client_transport.close(ctx)


@pytest.mark.asyncio
async def test_client_transport_start_error(monkeypatch):
    """start_stream surfaces a refused dial."""
    async def _refused(*_args, **_kwargs):
        raise ConnectionRefusedError(111, "Connection refused")

    monkeypatch.setattr(
        "trpc_sdk.transport._client._client_transport.get_pool_connection",
        _refused,
    )
    client_transport = ClientTransport()
    ctx, _msg = _context(stream_id=1)
    client_opts = RoundTripOptions()
    client_opts.set_network("tcp")
    client_opts.set_address(("127.0.0.1", 8088))
    with pytest.raises(ConnectionRefusedError):
        await client_transport.start_stream(ctx, client_opts)
    client_transport.close(ctx)


@pytest.mark.asyncio
async def test_client_stream_transport(monkeypatch):
    """Start, send, and recv a stream connection looked up by stream id."""
    fake = _FakeConn()

    async def _get_conn(*_args, **_kwargs):
        return fake

    monkeypatch.setattr(
        "trpc_sdk.transport._client._client_transport.get_pool_connection",
        _get_conn,
    )
    client_transport = ClientTransport()
    client_opts = RoundTripOptions()
    client_opts.set_network("tcp")
    client_opts.set_address(("127.0.0.1", 8000))
    client_opts.set_parser(_FrameParser(b"abc"))
    ctx, _msg = _context(stream_id=1)
    await client_transport.start_stream(ctx, client_opts)
    conn = client_transport.stream_id_to_conn.get(1, None)
    assert conn is not None
    client_transport.send(ctx, b"abc", client_opts)
    assert fake.written == b"abc"
    assert await client_transport.recv(ctx, client_opts) == b"abc"
    client_transport.close(ctx)
    assert 1 not in client_transport.stream_id_to_conn
    with pytest.raises(ServerSystemError, match="Connection is Closed"):
        client_transport.send(ctx, b"", client_opts)


@pytest.mark.asyncio
async def test_client_transport_stream(monkeypatch):
    """A started stream can send, recv, and then reject use after close."""
    fake = _FakeConn()

    async def _get_conn(*_args, **_kwargs):
        return fake

    monkeypatch.setattr(
        "trpc_sdk.transport._client._client_transport.get_pool_connection",
        _get_conn,
    )
    ctx, msg = _context(stream_id=100)
    msg.set_callee_service_name("/trpc_sdk.test.helloworld.Greeter")
    msg.set_client_rpc_name("/trpc_sdk.test.helloworld.Greeter/SayHi")
    options = RoundTripOptions()
    options.set_address(("127.0.0.1", 8000))
    options.set_parser(_FrameParser(b"rsp"))
    client_transport = ClientTransport()
    await client_transport.start_stream(ctx, options)
    client_transport.send(ctx, b"", options)
    assert await client_transport.recv(ctx, options) == b"rsp"
    client_transport.close(ctx)

    with pytest.raises(ServerSystemError, match="Connection is Closed"):
        client_transport.send(ctx, b"", options)
    with pytest.raises(ServerSystemError, match="Connection is Closed"):
        await client_transport.recv(ctx, options)


@pytest.mark.asyncio
async def test_client_transport(monkeypatch):
    """TCP returns the parser frame; a read timeout is a client timeout; UDP returns the datagram."""
    frame = b"\t0\x00\x00\x00\x00\x00\x1f\x00\x02\x00\x00\x00\x00\x00\x00\x18\x01\n\x0bhi: hello: "
    fake = _FakeConn(frame)

    async def _get_conn(*_args, **_kwargs):
        return fake

    monkeypatch.setattr(
        "trpc_sdk.transport._client._client_transport_tcp.get_pool_connection",
        _get_conn,
    )
    options = RoundTripOptions()
    options.set_address(("127.0.0.1", 8000))
    options.set_network("tcp")
    options.set_parser(_FrameParser(error=asyncio.TimeoutError()))
    client_transport = ClientTransport()
    ctx, _msg = _context()
    ctx.deadline = now_ms() + 2000
    with pytest.raises(ClientTimeoutError, match="reached deadline"):
        await client_transport.round_trip(ctx, b"req", options)

    options.set_parser(_FrameParser(frame))
    rsp = await client_transport.round_trip(ctx, b"req", options)
    assert rsp == frame

    udp = _UdpConn(b"hi: hello: ")

    async def _open(*_args, **_kwargs):
        return udp

    monkeypatch.setattr(
        "trpc_sdk.transport._client._client_transport_udp.open_datagram_endpoint",
        _open,
    )
    options.set_network("udp")
    ctx.timeout = 1000
    rsp = await client_transport.round_trip(ctx, b"req", options)
    assert rsp == b"hi: hello: "


@pytest.mark.asyncio
async def test_tcp_round_trip_records_rpcz_timestamps(monkeypatch):
    """The unary TCP path records send and receive timestamps on the context."""
    fake = _FakeConn(b"rsp")

    async def _get_conn(*_args, **_kwargs):
        return fake

    monkeypatch.setattr(
        "trpc_sdk.transport._client._client_transport_tcp.get_pool_connection",
        _get_conn,
    )
    ctx, msg = _context()
    ctx.deadline = now_ms() + 5000
    options = RoundTripOptions()
    options.set_network("tcp")
    options.set_address(("127.0.0.1", 8000))
    options.set_parser(_FrameParser(b"rsp"))
    rsp = await tcp_round_trip(ctx, b"test_data", options)
    assert rsp == b"rsp"
    assert ctx.rpcz_data.client_post_sched_send_msg_us > 0
    assert ctx.rpcz_data.client_pre_io_send_msg_us > 0
    assert ctx.rpcz_data.client_post_io_send_msg_us > 0
    assert ctx.rpcz_data.client_post_sched_recv_msg_us > 0
    assert msg.request_id == 1
