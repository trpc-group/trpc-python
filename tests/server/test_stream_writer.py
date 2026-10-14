# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.
"""Stream frames are written through the handle stored at INIT, whatever the mode."""

from __future__ import annotations

from typing import List

import pytest

from trpc_sdk.abc import STREAM_WRITER_CTX_KEY
from trpc_sdk.abc import TrpcContext
from trpc_sdk.abc import clone_context_and_message
from trpc_sdk.abc import get_message
from trpc_sdk.exceptions import ServerSystemError
from trpc_sdk.transport import IpcStreamWriter
from trpc_sdk.transport import TrpcServerStreamTransport
from trpc_sdk.transport._cython import inner_decode_rsp  # pylint: disable=no-name-in-module


class _Pipe:

    def __init__(self) -> None:
        self.written: List[bytes] = []

    def write(self, data: bytes) -> None:
        self.written.append(data)


def _ctx(writer=None, session_id: int = 9, stream_id: int = 101) -> TrpcContext:
    ctx = TrpcContext()
    msg = get_message(ctx)
    msg.set_session_id(session_id)
    msg.set_stream_id(stream_id)
    if writer is not None:
        ctx.with_meta_data(STREAM_WRITER_CTX_KEY, writer)
    return ctx


def test_ipc_writer_uses_the_fd_captured_at_init():
    pipe = _Pipe()
    transport = TrpcServerStreamTransport()
    init_ctx = _ctx(IpcStreamWriter(pipe, fd=17, session_id=9))
    init_ctx.with_meta_data("fd", 17)
    transport.store_stream_conn(init_ctx)
    assert init_ctx.get_meta_data(STREAM_WRITER_CTX_KEY) is None

    # The frame context's own metadata no longer matters.
    clone, clone_msg = clone_context_and_message(init_ctx)
    clone_msg.set_stream_id(101)
    clone.pop_meta_data("fd")
    transport.send(clone, b"frame", 101)
    transport.send(_ctx(), b"bare", 101)

    decoded = [inner_decode_rsp(buf) for buf in pipe.written]
    assert [(fd, session_id, data) for fd, session_id, _, data in decoded] == [(17, 9, b"frame"), (17, 9, b"bare")]


def test_send_writes_through_the_stored_handle_and_rejects_unknown_streams():
    frames: List[bytes] = []

    class _Writer:
        closed = False

        def write(self, data, ctx=None):
            frames.append(data)

    transport = TrpcServerStreamTransport()
    transport.store_stream_conn(_ctx(_Writer()))
    with pytest.raises(ServerSystemError):
        transport.store_stream_conn(_ctx(_Writer()))
    transport.send(_ctx(), b"raw", 101)
    assert frames == [b"raw"]

    with pytest.raises(ValueError):
        transport.send(_ctx(stream_id=102), b"raw", 102)
    transport.delete_stream_conn(_ctx())
    transport.delete_stream_conn(_ctx())  # a second delete only logs
    assert transport.stream_conns == {}
