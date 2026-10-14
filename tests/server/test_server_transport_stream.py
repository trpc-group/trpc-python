# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.
"""Test server stream transport"""

import pytest

from trpc_sdk.abc import STREAM_WRITER_CTX_KEY
from trpc_sdk.abc import BaseServerTransportOption
from trpc_sdk.abc import TrpcContext
from trpc_sdk.abc import get_message
from trpc_sdk.exceptions import ServerSystemError
from trpc_sdk.transport import TrpcServerStreamTransport
from trpc_sdk.transport import TrpcServerTransport


class _Writer:
    """Stream write handle that records every frame."""

    closed = False

    def __init__(self):
        self.frames = []

    def write(self, data, ctx=None):  # pylint: disable=unused-argument
        self.frames.append(data)


def _ctx(session_id: int, stream_id: int, writer=None) -> TrpcContext:
    ctx = TrpcContext()
    msg = get_message(ctx)
    msg.session_id, msg.stream_id = session_id, stream_id
    if writer is not None:
        ctx.with_meta_data(STREAM_WRITER_CTX_KEY, writer)
    return ctx


def test_stream_transport_shares_unary_bind_and_listen():
    """Lifecycle comes from TrpcServerTransport; bind also marks is_stream."""
    transport = TrpcServerStreamTransport()
    opts = BaseServerTransportOption()

    transport.bind(opts)

    assert isinstance(transport, TrpcServerTransport)
    assert transport.opts is opts
    assert opts.is_stream is True


def test_store_send_delete_stream_conn():
    """INIT stores the write handle; send writes through it; delete drops it."""
    transport = TrpcServerStreamTransport()
    writer = _Writer()
    init_ctx = _ctx(0, 1, writer)

    transport.store_stream_conn(init_ctx)
    assert init_ctx.get_meta_data(STREAM_WRITER_CTX_KEY) is None
    with pytest.raises(ServerSystemError):
        transport.store_stream_conn(_ctx(0, 1, writer))

    transport.send(_ctx(0, 1), b"efg", 1)
    assert writer.frames == [b"efg"]

    with pytest.raises(ValueError):
        transport.send(_ctx(0, 100), b"efg", 100)

    transport.delete_stream_conn(_ctx(0, 1))
    transport.delete_stream_conn(_ctx(0, 1))
    assert transport.stream_conns == {}
