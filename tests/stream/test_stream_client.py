# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.
"""Client stream framing, flow-control hooks, and window clamping."""

from types import SimpleNamespace
from unittest.mock import AsyncMock
from unittest.mock import MagicMock

import pytest

from trpc_sdk._inner import trpc_pb2
from trpc_sdk.abc import TrpcContext
from trpc_sdk.abc import get_message
from trpc_sdk.client import Options
from trpc_sdk.codec import FrameHead
from trpc_sdk.exceptions import ClientDecodeError
from trpc_sdk.exceptions import ClientTimeoutError
from trpc_sdk.exceptions import ServerSystemError
from trpc_sdk.stream import ClientStream
from trpc_sdk.stream import ClientStreamDesc
from trpc_sdk.stream import ReceiveControl
from trpc_sdk.stream import SendControl
from trpc_sdk.stream import create_stream_client
from trpc_sdk.stream import get_window_size
from trpc_sdk.stream._constants import MAX_INIT_WINDOW_SIZE
from trpc_sdk.stream._constants import STREAM_ID
from trpc_sdk.stream import _client as stream_mod


def _client_stream():
    ctx = TrpcContext()
    get_message(ctx)
    stream = ClientStream(ctx, ClientStreamDesc(stream_name="/method", client_streams=True), 101, "/method")
    stream.stream = SimpleNamespace(send=AsyncMock(), recv=AsyncMock(), close=MagicMock(), start=AsyncMock())
    stream.opts = Options()
    stream.opts.r_control = None
    stream.opts.s_control = None
    stream.opts.current_compress_type = -1
    stream.opts.current_serialization_type = -1
    return stream


def test_create_stream_client_starts_after_reserved_ids():
    client = create_stream_client()
    assert client.stream_id == STREAM_ID


@pytest.mark.asyncio
async def test_init_stream_disables_flow_control_when_window_is_zero(monkeypatch):
    stream = _client_stream()

    async def start(_ctx, _options):
        opts = Options()
        opts.max_window_size = 0
        return opts

    calls = {"n": 0}

    async def recv(_ctx):
        calls["n"] += 1
        if calls["n"] == 1:
            return b""
        raise RuntimeError("socket failed")

    stream.stream.start = start
    stream.stream.recv = recv
    stream.stream.send = AsyncMock()
    stream.stream.close = MagicMock()
    monkeypatch.setattr(stream_mod, "get_options", lambda *_args, **_kwargs: Options())
    monkeypatch.setattr(stream_mod, "update_message", lambda *_args, **_kwargs: None)

    await stream.init_stream([])
    # dispatch is scheduled by invoke and exits when recv fails.
    await __import__("asyncio").sleep(0.05)

    assert stream.server_closed is True
    assert stream.opts.s_control is None
    assert stream.opts.r_control is None


@pytest.mark.asyncio
async def test_receive_and_send_control_credit():
    ctrl = ReceiveControl(window_size=1, send_feedback=None)
    ctrl.refresh_remaining_window()
    assert ctrl.get_remaining_window() == 1
    await ctrl.on_recv(2)
    assert ctrl.get_remaining_window() == 1

    send = SendControl(10)
    send.update_window(1)
    await send.acquire_window(1)
    assert send.available_window == 10


def test_get_window_size():
    assert get_window_size(MAX_INIT_WINDOW_SIZE + 1) == MAX_INIT_WINDOW_SIZE
    assert get_window_size(-1) == 0
    assert get_window_size(128) == 128


@pytest.mark.asyncio
async def test_close_send_server_closed_and_double_close():
    stream = _client_stream()
    stream.server_closed = True
    await stream.close_send()
    stream.stream.send.assert_not_awaited()

    stream.server_closed = False
    stream.client_closed = True
    with pytest.raises(ServerSystemError, match="closed stream"):
        await stream.close_send()


@pytest.mark.asyncio
async def test_recv_msg_raises_queued_error():
    stream = _client_stream()
    stream.recv_queue.push((b"", RuntimeError("recv failed")))
    with pytest.raises(RuntimeError, match="recv failed"):
        await stream.recv_msg(object())


@pytest.mark.asyncio
async def test_send_msg_rejects_closed_stream():
    stream = _client_stream()
    stream.client_closed = True
    with pytest.raises(ServerSystemError, match="closed conn"):
        await stream.send_msg(object())


def test_handle_close_frame_closes_transport_and_flow_control():
    stream = _client_stream()
    ctx = TrpcContext()
    msg = get_message(ctx)
    head = FrameHead()
    head.frame_state = trpc_pb2.TRPC_STREAM_FRAME_CLOSE
    msg.set_frame_head(head)
    control = MagicMock()
    stream.opts.s_control = control

    stream.handle_frame(ctx, b"")

    assert stream.server_closed is True
    stream.stream.close.assert_called_once_with(ctx)
    control.close_control.assert_called_once()


def test_handle_feedback_updates_window():
    stream = _client_stream()
    control = MagicMock()
    stream.opts.s_control = control
    ctx = TrpcContext()
    msg = get_message(ctx)
    head = FrameHead()
    head.frame_state = trpc_pb2.TRPC_STREAM_FRAME_FEEDBACK
    msg.set_frame_head(head)
    feedback = trpc_pb2.TrpcStreamFeedBackMeta()
    feedback.window_size_increment = 17
    msg.set_stream_frame(feedback)

    stream.handle_frame(ctx, b"")

    control.update_window.assert_called_once_with(17)


def test_handle_feedback_rejects_wrong_metadata_type():
    stream = _client_stream()
    with pytest.raises(ClientDecodeError, match="feedback fields"):
        stream.handle_feedback(SimpleNamespace(stream_frame=object()))


@pytest.mark.asyncio
async def test_dispatch_recv_exception_synthesizes_close_frame():
    stream = _client_stream()
    stream.stream.recv.side_effect = RuntimeError("socket failed")

    await stream.dispatch()

    assert stream.server_closed is True
    rsp, err = await stream.recv_queue.pop()
    assert rsp == b""
    assert isinstance(err, ClientTimeoutError)
    assert stream.stream.close.call_count == 2


@pytest.mark.asyncio
async def test_feedback_sends_control_frame():
    stream = _client_stream()
    await stream.feedback(25)
    stream.stream.send.assert_awaited_once()


def test_set_send_control_zero_disables_both_controls():
    stream = _client_stream()
    stream.opts.r_control = object()
    stream.opts.s_control = object()

    stream.set_send_control(0)

    assert stream.opts.r_control is None
    assert stream.opts.s_control is None


@pytest.mark.asyncio
async def test_stream_filter_wraps_invoke_exception(monkeypatch):
    stream = _client_stream()

    class _Filters:
        async def handle(self, ctx, req, callback):
            return await callback(ctx, req)

    options = SimpleNamespace(enable_stream_filter=True, filters=_Filters())
    monkeypatch.setattr(stream_mod, "get_options", lambda *_args, **_kwargs: options)
    monkeypatch.setattr(stream_mod, "update_message", lambda *_args, **_kwargs: None)
    stream.invoke = AsyncMock(side_effect=RuntimeError("invoke failed"))
    ctx = TrpcContext()
    msg = get_message(ctx)

    result, err = await stream._rpc_stream(ctx, msg, [])

    assert result is None
    assert isinstance(err, RuntimeError)
