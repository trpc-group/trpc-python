# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.
"""Test client transport stream module"""
import pytest

from trpc_sdk.abc import TrpcContext
from trpc_sdk.codec import create_message
from trpc_sdk.exceptions import ClientInvokeError
from trpc_sdk.exceptions import ClientTimeoutError
from trpc_sdk.transport import ClientConn
from trpc_sdk.transport import ClientTransport
from trpc_sdk.transport import RoundTripOptions


class _FakeConn:
    def __init__(self):
        self.written = b""
        self.closed = False

    def write(self, data):
        self.written += data

    def close(self, force=False):
        self.closed = True


class _Parser:
    def __init__(self, payload=b"123", error=None):
        self.payload = payload
        self.error = error

    async def read_frame(self, conn):
        if self.error is not None:
            raise self.error
        return self.payload


def _stream_context():
    ctx, msg = create_message(TrpcContext())
    msg.set_request_id(1)
    msg.set_stream_id(1)
    return ctx, msg


@pytest.mark.asyncio
async def test_client_stream_transport(monkeypatch):
    """A stream conn is stored by stream id and can queue, send, and recv."""
    fake = _FakeConn()

    async def _get_conn(*_args, **_kwargs):
        return fake

    monkeypatch.setattr(
        "trpc_sdk.transport._client._client_transport.get_pool_connection",
        _get_conn,
    )
    client_transport = ClientTransport()
    assert len(client_transport.stream_id_to_conn) == 0
    client_opts = RoundTripOptions()
    client_opts.set_network("tcp")
    client_opts.set_address(("127.0.0.1", 8000))
    client_opts.set_parser(_Parser(b"abc"))
    ctx, _msg = _stream_context()
    await client_transport.start_stream(ctx, client_opts)
    conn = client_transport.stream_id_to_conn.get(1, None)
    assert conn is not None
    assert len(conn.recv_queue.queue) == 0
    conn.push_data_to_recv_queue(b"123")
    assert len(conn.recv_queue.queue) == 1
    client_transport.send(ctx, b"abc", client_opts)
    assert fake.written == b"abc"
    assert await client_transport.recv(ctx, client_opts) == b"abc"
    client_transport.close(ctx)
    assert 1 not in client_transport.stream_id_to_conn


@pytest.mark.asyncio
async def test_recv_frame():
    """Recv frame turns parser failures into client errors."""
    timeout_options = RoundTripOptions()
    timeout_options.set_parser(_Parser(error=TimeoutError("slow")))
    client_conn = ClientConn(conn=_FakeConn(), options=timeout_options)
    ctx = TrpcContext()
    ctx.deadline = 0
    with pytest.raises(ClientTimeoutError):
        await client_conn.recv_frame(ctx)

    error_options = RoundTripOptions()
    error_options.set_parser(_Parser(error=RuntimeError("test err")))
    client_conn = ClientConn(conn=_FakeConn(), options=error_options)
    with pytest.raises(ClientInvokeError, match="test err"):
        await client_conn.recv_frame(ctx)


def test_client_conn_close_marks_force():
    """Closing a stream conn forwards force-close to the pooled connection."""
    fake = _FakeConn()
    options = RoundTripOptions()
    options.parser = _Parser()
    client_conn = ClientConn(conn=fake, options=options)
    client_conn.close(True)
    assert fake.closed is True
