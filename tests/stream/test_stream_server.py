# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.
"""Server stream send/recv and the dispatcher frame switch."""

from unittest.mock import MagicMock

import pytest

from trpc_sdk._inner import trpc_pb2
from trpc_sdk.abc import TrpcContext
from trpc_sdk.abc import get_message
from trpc_sdk.exceptions import ServerSystemError
from trpc_sdk.serialization import Body
from trpc_sdk.serialization import SerializationType
from trpc_sdk.server import ServiceOption
from trpc_sdk.stream import ServerStream
from trpc_sdk.stream import StreamDispatcher
from trpc_sdk.stream import create_stream_dispatcher
from trpc_sdk.transport import TrpcServerStreamTransport


def _options():
    opts = ServiceOption()
    opts.set_protocol("trpc")
    opts.current_serialization_type = SerializationType.Noop
    opts.stream_transport = MagicMock()
    return opts


def _stream():
    ctx = TrpcContext()
    msg = get_message(ctx)
    msg.stream_id = 100
    return ServerStream(ctx, 1, _options()), ctx


@pytest.mark.asyncio
async def test_server_stream():
    server_stream, ctx = _stream()
    msg = get_message(ctx)
    await server_stream.feedback(10)
    server_stream.opts.stream_transport.send.assert_called()

    init = trpc_pb2.TrpcStreamInitMeta()
    init.init_window_size = 0
    msg.stream_frame = init
    assert server_stream.set_send_control(msg) == 0
    assert server_stream.s_control is None

    init.init_window_size = 100
    msg.stream_frame = init
    assert server_stream.set_send_control(msg) == 100
    assert server_stream.s_control.available_window == 100

    msg.stream_frame = None
    with pytest.raises(ServerSystemError, match="TrpcStreamInitMeta"):
        server_stream.set_send_control(msg)

    body = Body(data=b"hi")
    await server_stream.send_msg(body)
    server_stream.close_send(trpc_pb2.TRPC_STREAM_CLOSE, 0, "test msg")

    incoming = Body()
    server_stream.recv_queue.push((b"payload", None))
    await server_stream.recv_msg(incoming)
    assert incoming.data == b"payload"

    server_stream.recv_queue.push((b"", EOFError))
    with pytest.raises(EOFError):
        await server_stream.recv_msg(Body())

    server_stream.recv_queue.push((b"", Exception("test exception")))
    with pytest.raises(ServerSystemError, match="stream is already closed"):
        await server_stream.recv_msg(Body())

    assert server_stream.context() is ctx


def test_stream_dispatcher_requires_a_stream_transport():
    dispatcher = create_stream_dispatcher()
    opts = ServiceOption()
    opts.transport = TrpcServerStreamTransport()
    dispatcher.init(opts)
    assert opts.stream_transport is opts.transport

    opts.transport = None
    with pytest.raises(TypeError, match="server StreamTransport is not implemented"):
        dispatcher.init(opts)

    ctx = TrpcContext()
    dispatcher.handle_error(ctx, None)
    msg = get_message(ctx)
    # A fresh message carries a success error, so a missing head is a connection-level no-op.
    msg.frame_head = None
    assert dispatcher.stream_handle_func(ctx, None, None) is None

    msg.set_server_rsp_err(None)
    with pytest.raises(ServerSystemError, match="frameHead is not contained in msg"):
        dispatcher.stream_handle_func(ctx, None, None)


@pytest.mark.asyncio
async def test_unknown_frame_and_bad_feedback():
    dispatcher = StreamDispatcher()
    opts = _options()
    opts.transport = TrpcServerStreamTransport()
    dispatcher.init(opts)

    ctx = TrpcContext()
    msg = get_message(ctx)
    msg.frame_head.frame_state = 0
    msg.stream_id = 1
    msg.session_id = 1
    with pytest.raises(ValueError, match="unknown stream frame type 0"):
        await dispatcher.handle_by_stream_frame_type(ctx, None, b"")

    msg.frame_head.frame_state = trpc_pb2.TRPC_STREAM_FRAME_FEEDBACK
    msg.stream_frame = object()
    with pytest.raises(ServerSystemError, match="feedback fields"):
        await dispatcher.handle_by_stream_frame_type(ctx, None, b"")


def test_stream_transport_send_without_a_stored_handle():
    stream_transport = TrpcServerStreamTransport()
    ctx = TrpcContext()
    msg = get_message(ctx)
    msg.session_id = 1
    msg.stream_id = 1
    with pytest.raises(ValueError, match="does not exist"):
        stream_transport.send(ctx, b"", 1)


class _MockServerStream(ServerStream):
    def close_send(self, close_type, ret_code, ret_msg):
        self.closed = (close_type, ret_code, ret_msg)


@pytest.mark.asyncio
async def test_filter_stream():
    dispatcher = StreamDispatcher()
    opts = _options()
    opts.enable_stream_filter = False
    dispatcher.opts = opts

    async def _stream_handle(_server_stream):
        return

    ctx = TrpcContext()
    get_message(ctx)
    mock_stream = _MockServerStream(ctx, 1, opts)
    await dispatcher.start_stream_handler(ctx, mock_stream, _stream_handle)
    assert mock_stream.handler_done is True
    assert mock_stream.closed[0] == trpc_pb2.TRPC_STREAM_CLOSE

    async def _fail(_server_stream):
        raise RuntimeError("stream failed")

    failing = _MockServerStream(ctx, 2, opts)
    await dispatcher.start_stream_handler(ctx, failing, _fail)
    assert failing.closed[0] == trpc_pb2.TRPC_STREAM_RESET
    assert "stream failed" in failing.closed[2]

    # The filter chain logs the handler error and does not re-raise, so the
    # dispatcher still closes the stream as a normal CLOSE.
    opts.enable_stream_filter = True
    swallowed = _MockServerStream(ctx, 3, opts)
    await dispatcher.start_stream_handler(ctx, swallowed, _fail)
    assert swallowed.closed[0] == trpc_pb2.TRPC_STREAM_CLOSE
