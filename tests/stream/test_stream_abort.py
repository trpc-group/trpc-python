# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.
"""Server stream maps per thread, cleanup when both sides closed, and abort on disconnect."""

from __future__ import annotations

import asyncio
import threading
from typing import Any
from typing import Dict
from typing import List
from typing import Optional

import pytest

from trpc_sdk._inner import trpc_pb2
from trpc_sdk.abc import STREAM_WRITER_CTX_KEY
from trpc_sdk.abc import TrpcContext
from trpc_sdk.abc import create_frame_head
from trpc_sdk.abc import get_message
from trpc_sdk.codec import TrpcServerCodec
from trpc_sdk.exceptions import ServerSystemError
from trpc_sdk.server import ServiceOption
from trpc_sdk.stream import ServerStream
from trpc_sdk.stream import StreamDispatcher
from trpc_sdk.stream import _server as stream_server
from trpc_sdk.stream._flow_control import SendControl
from trpc_sdk.transport import TrpcServerStreamTransport

SESSION = 42


class _Writer:
    """Stream write handle double: records frames, has the ``closed`` flag."""

    def __init__(self) -> None:
        self.closed = False
        self.frames: List[bytes] = []

    def write(self, data: bytes, ctx: Any = None) -> None:
        if not self.closed:
            self.frames.append(data)


def _dispatcher() -> StreamDispatcher:
    opts = ServiceOption()
    opts.codec = TrpcServerCodec()
    opts.transport = TrpcServerStreamTransport()
    dispatcher = StreamDispatcher()
    dispatcher.init(opts)
    return dispatcher


def _frame_ctx(stream_id: int, frame_state: int, writer: Optional[_Writer] = None,
               session_id: int = SESSION) -> TrpcContext:
    ctx = TrpcContext()
    msg = get_message(ctx)
    msg.set_session_id(session_id)
    msg.set_stream_id(stream_id)
    msg.set_frame_head(create_frame_head(trpc_pb2.TRPC_STREAM_FRAME, frame_state, stream_id))
    if writer is not None:
        ctx.with_meta_data(STREAM_WRITER_CTX_KEY, writer)
    return ctx


def _init_ctx(dispatcher: StreamDispatcher, stream_id: int, writer: _Writer, window: int = 65535) -> TrpcContext:
    """INIT context as the codec leaves it: meta cached, stream frame set."""
    ctx = _frame_ctx(stream_id, trpc_pb2.TRPC_STREAM_FRAME_INIT, writer)
    meta = trpc_pb2.TrpcStreamInitMeta()
    meta.init_window_size = window
    meta.request_meta.func = b"/svc/Chat"
    msg = get_message(ctx)
    msg.set_stream_frame(meta)
    dispatcher.opts.codec.store_init_meta(msg, meta)
    return ctx


def _close_ctx(stream_id: int) -> TrpcContext:
    ctx = _frame_ctx(stream_id, trpc_pb2.TRPC_STREAM_FRAME_CLOSE)
    get_message(ctx).set_stream_frame(trpc_pb2.TrpcStreamCloseMeta())
    return ctx


def _tables(dispatcher: StreamDispatcher) -> Dict[str, dict]:
    return {
        "stream_to_ss": dispatcher.stream_to_ss,
        "stream_conns": dispatcher.opts.stream_transport.stream_conns,
        "init_metas": dispatcher.opts.codec.init_metas,
    }


def _assert_empty(dispatcher: StreamDispatcher) -> None:
    assert _tables(dispatcher) == {"stream_to_ss": {}, "stream_conns": {}, "init_metas": {}}


async def _settle() -> None:
    for _ in range(10):
        await asyncio.sleep(0)


def test_each_thread_sees_its_own_streams():
    dispatcher = _dispatcher()
    seen: Dict[int, List[int]] = {}
    stored = threading.Barrier(2)
    deleted = threading.Barrier(2)

    def run(stream_id: int) -> None:
        ctx = _frame_ctx(stream_id, trpc_pb2.TRPC_STREAM_FRAME_INIT)
        dispatcher.store_server_stream(ctx, ServerStream(ctx, stream_id, dispatcher.opts))
        stored.wait()
        seen[stream_id] = sorted(dispatcher.stream_to_ss[SESSION])
        if stream_id == 1:
            dispatcher.delete_server_stream(ctx)
        deleted.wait()
        if stream_id == 2:
            seen[0] = sorted(dispatcher.stream_to_ss[SESSION])

    threads = [threading.Thread(target=run, args=(i,)) for i in (1, 2)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert seen == {1: [1], 2: [2], 0: [2]}


async def test_abort_wakes_recv_and_send_and_empties_every_table():
    dispatcher = _dispatcher()
    writer = _Writer()
    errors: Dict[str, BaseException] = {}

    async def receiver(stream: ServerStream) -> None:
        try:
            await stream.recv_msg(trpc_pb2.TrpcStreamCloseMeta())
        except Exception as err:  # pylint: disable=broad-except
            errors["recv"] = err
            raise

    async def sender(stream: ServerStream) -> None:
        body = trpc_pb2.TrpcStreamCloseMeta(msg=b"x" * 8)
        try:
            await stream.send_msg(body)  # takes the only credit
            await stream.send_msg(body)  # waits for a FEEDBACK that never comes
        except Exception as err:  # pylint: disable=broad-except
            errors["send"] = err
            raise

    await dispatcher.handle_init(_init_ctx(dispatcher, 101, writer), receiver)
    await dispatcher.handle_init(_init_ctx(dispatcher, 102, writer, window=1), sender)
    await _settle()
    assert set(dispatcher.stream_to_ss[SESSION]) == {101, 102}

    writer.closed = True
    dispatcher.abort_session(SESSION)
    await _settle()

    assert isinstance(errors["recv"], ServerSystemError)
    assert "closed" in str(errors["send"])
    _assert_empty(dispatcher)


async def test_abort_drops_stream_whose_handler_already_returned():
    dispatcher = _dispatcher()

    async def done(stream: ServerStream) -> None:
        return None

    await dispatcher.handle_init(_init_ctx(dispatcher, 101, _Writer()), done)
    await _settle()
    # Handler returned and sent CLOSE, but the client never closed.
    assert dispatcher.stream_to_ss[SESSION][101].handler_done
    dispatcher.abort_session(SESSION)
    _assert_empty(dispatcher)


async def test_init_after_abort_creates_nothing(monkeypatch):
    counted: List[str] = []
    monkeypatch.setattr(stream_server, "report_attr", lambda name, value: counted.append(name))
    dispatcher = _dispatcher()
    writer = _Writer()
    started: List[int] = []

    async def handler(stream: ServerStream) -> None:
        started.append(stream.stream_id)

    ctx = _init_ctx(dispatcher, 101, writer)
    # The protocol closes the writer first, then posts the abort, which here
    # runs before the INIT frame's task.
    writer.closed = True
    dispatcher.abort_session(SESSION)
    assert await dispatcher.handle_init(ctx, handler) is None
    await _settle()

    assert started == []
    assert writer.frames == []
    assert counted == ["trpc_stream_init_after_close"]
    _assert_empty(dispatcher)


async def test_one_failing_stream_does_not_stop_the_abort_of_the_others():
    dispatcher = _dispatcher()
    writer = _Writer()

    async def receiver(stream: ServerStream) -> None:
        await stream.recv_msg(trpc_pb2.TrpcStreamCloseMeta())

    await dispatcher.handle_init(_init_ctx(dispatcher, 101, writer), receiver)
    await dispatcher.handle_init(_init_ctx(dispatcher, 102, writer), receiver)
    await _settle()

    def broken(data):
        raise RuntimeError("boom")

    dispatcher.stream_to_ss[SESSION][101].push_to_recv_queue = broken
    writer.closed = True
    dispatcher.abort_session(SESSION)
    await _settle()

    assert list(dispatcher.stream_to_ss[SESSION]) == [101]
    assert dispatcher.opts.codec.init_metas == {}
    assert list(dispatcher.opts.stream_transport.stream_conns[SESSION]) == [101]


async def test_handler_finishing_before_client_close_leaves_nothing_behind():
    dispatcher = _dispatcher()
    writer = _Writer()

    async def done(stream: ServerStream) -> None:
        return None

    for _ in range(100):
        await dispatcher.handle_init(_init_ctx(dispatcher, 101, writer), done)
        await _settle()
        # The server CLOSE was encoded, yet the client's CLOSE must still decode.
        close = _close_ctx(101)
        dispatcher.opts.codec.set_init_meta(get_message(close))
        dispatcher.handle_close(close)
        _assert_empty(dispatcher)


async def test_client_closing_first_is_dropped_when_the_handler_returns():
    dispatcher = _dispatcher()
    release = asyncio.Event()

    async def handler(stream: ServerStream) -> None:
        with pytest.raises(EOFError):
            await stream.recv_msg(trpc_pb2.TrpcStreamCloseMeta())
        await release.wait()

    await dispatcher.handle_init(_init_ctx(dispatcher, 101, _Writer()), handler)
    await _settle()
    dispatcher.handle_close(_close_ctx(101))
    await _settle()
    assert SESSION in dispatcher.stream_to_ss
    release.set()
    await _settle()
    _assert_empty(dispatcher)


async def test_feedback_then_close_in_one_turn_does_not_raise():
    control = SendControl(1)
    control.available_window = 0
    waiter = asyncio.ensure_future(control.acquire_window(1))
    await asyncio.sleep(0)
    control.update_window(10)
    control.close_control()
    with pytest.raises(Exception, match="closed"):
        await waiter
