# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.
"""End-to-end checks of streaming RPC in mode 1 over real sockets.

Three thread shapes are covered: merge on one thread (``SINGLE``), merge on
several threads (``THREADED``) and ``io_handle_type: separate``
(``SEPARATE``). The subprocess registers one bidirectional stream,
``Chat``, whose ``HelloRequest.msg`` drives it:

* ``"end"``: return, so the handler finishes before the client's CLOSE;
* ``"big:<size>:<count>"``: send ``count`` replies of ``size`` bytes, then
  ``done|<thread>``;
* anything else: reply ``<msg>|<session_id>|<stream_id>|<thread>``.

The unary ``SayHello`` with ``msg == "stats"`` returns JSON with the stream
table sizes and live stream handlers summed over every serving loop, plus
how many handlers saw an error. The client hand-rolls stream frames so these
tests do not depend on the stream client.
"""

from __future__ import annotations

import asyncio
import json
import struct
import time
from collections import Counter
from typing import Dict
from typing import Iterator
from typing import List
from typing import Optional
from typing import Tuple

import pytest

from tests.server._e2e_support import SERVICES
from tests.server._e2e_support import Server
from tests.server._e2e_support import free_port
from tests.server._e2e_support import pb
from tests.server._e2e_support import tcp_call
from trpc_sdk._inner.pb import trpc_pb2

SINGLE = ""
THREADED = "io_thread_num: 3"
SEPARATE = "io_handle_type: separate\nio_thread_num: 2\nhandle_thread_num: 3"
SHAPES = {"single": SINGLE, "threaded": THREADED, "separate": SEPARATE}
CHAT = f"/{SERVICES['tcp']}/Chat"

STREAM_SERVER_SCRIPT = """
import asyncio
import json
import sys
import threading

sys.path.insert(0, {stub!r})

from trpc_sdk.abc import get_message
from trpc_sdk.server import Method
from trpc_sdk.server import ServiceDesc
from trpc_sdk.server import StreamDesc
from trpc_sdk.server import create_server
from trpc_sdk.runtime import current_handler_thread
from trpc_sdk.runtime import register_handler_thread_hook
from trpc_sdk.stream import create_stream_dispatcher
from trpc_trpc_python_group_helloworld import pb

SERVICE = "__SERVICE__"
DISPATCHER = create_stream_dispatcher()
LOOPS = []
ERRORS = []


def on_start(info):
    LOOPS.append(asyncio.get_event_loop())


register_handler_thread_hook(on_start=on_start)


def where():
    info = current_handler_thread()
    return "%d:%d" % (info.index if info else -1, threading.get_ident())


async def chat(stream):
    session_id = get_message(stream.ctx).session_id
    try:
        while True:
            req = pb.HelloRequest()
            try:
                await stream.recv_msg(req)
            except EOFError:
                return
            if req.msg == "end":
                return
            if req.msg.startswith("big:"):
                _, size, count = req.msg.split(":")
                for _ in range(int(count)):
                    await stream.send_msg(pb.HelloReply(msg="x" * int(size)))
                await stream.send_msg(pb.HelloReply(msg="done|" + where()))
                continue
            reply = "%s|%d|%d|%s" % (req.msg, session_id, stream.stream_id, where())
            await stream.send_msg(pb.HelloReply(msg=reply))
    except Exception as err:
        ERRORS.append(repr(err))
        raise


async def sizes():
    opts = DISPATCHER.opts
    handlers = [t for t in asyncio.all_tasks()
                if getattr(t.get_coro(), "__qualname__", "").endswith("start_stream_handler")]
    return [sum(len(v) for v in DISPATCHER.stream_to_ss.values()),
            sum(len(v) for v in opts.stream_transport.stream_conns.values()),
            sum(len(v) for v in opts.codec.init_metas.values()),
            len(handlers)]


async def say_hello(ctx, request):
    running = asyncio.get_running_loop()
    totals = [0, 0, 0, 0]
    for loop in LOOPS or [running]:
        if loop is running:
            part = await sizes()
        else:
            part = await asyncio.wrap_future(asyncio.run_coroutine_threadsafe(sizes(), loop))
        totals = [a + b for a, b in zip(totals, part)]
    keys = ("streams", "conns", "metas", "handlers")
    stats = dict(zip(keys, totals))
    stats["errors"] = len(ERRORS)
    return pb.HelloReply(msg=json.dumps(stats))


server = create_server(sys.argv[1])
server.register(ServiceDesc(
    service_name="/" + SERVICE,
    methods=[Method(name="/%s/SayHello" % SERVICE, req_cls=pb.HelloRequest,
                    rsp_cls=pb.HelloReply, impl_func=say_hello)],
    streams=[StreamDesc(name="/%s/Chat" % SERVICE, impl_func=chat, server_stream=None)],
    stream_handler=DISPATCHER,
), False)
server.serve()
""".replace("__SERVICE__", SERVICES["tcp"])

FRAME_HEAD = ">HBBIHIH"
CLOSED = -1


def _frame(state: int, stream_id: int, payload: bytes = b"") -> bytes:
    head = struct.pack(FRAME_HEAD, 0x930, trpc_pb2.TRPC_STREAM_FRAME, state, 16 + len(payload), 0, stream_id, 0)
    return head + payload


class StreamConn:
    """One TCP connection carrying hand-rolled stream frames.

    Inbound frames are split per ``stream_id``; FEEDBACK frames are only
    counted. ``CLOSED`` is queued on every stream when the server hangs up.
    """

    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        self.reader = reader
        self.writer = writer
        self.queues: Dict[int, asyncio.Queue] = {}
        self.feedbacks: Counter = Counter()
        self.reader_task = asyncio.ensure_future(self._read())

    @classmethod
    async def open(cls, port: int) -> "StreamConn":
        reader, writer = await asyncio.open_connection("127.0.0.1", port)
        return cls(reader, writer)

    def _queue(self, stream_id: int) -> asyncio.Queue:
        return self.queues.setdefault(stream_id, asyncio.Queue())

    async def _read(self) -> None:
        try:
            while True:
                head = await self.reader.readexactly(16)
                _, _, state, total, _, stream_id, _ = struct.unpack(FRAME_HEAD, head)
                body = await self.reader.readexactly(total - 16)
                if state == trpc_pb2.TRPC_STREAM_FRAME_FEEDBACK:
                    self.feedbacks[stream_id] += 1
                    continue
                self._queue(stream_id).put_nowait((state, body))
        except (asyncio.IncompleteReadError, ConnectionError):
            for queue in self.queues.values():
                queue.put_nowait((CLOSED, b""))

    def init(self, stream_id: int, window: int = 65535) -> None:
        meta = trpc_pb2.TrpcStreamInitMeta(
            request_meta=trpc_pb2.TrpcStreamInitRequestMeta(caller=b"test.client",
                                                            callee=SERVICES["tcp"].encode(),
                                                            func=CHAT.encode()),
            init_window_size=window)
        self._queue(stream_id)
        self.writer.write(_frame(trpc_pb2.TRPC_STREAM_FRAME_INIT, stream_id, meta.SerializeToString()))

    def send(self, stream_id: int, msg: str) -> None:
        body = pb.HelloRequest(msg=msg).SerializeToString()
        self.writer.write(_frame(trpc_pb2.TRPC_STREAM_FRAME_DATA, stream_id, body))

    def close_stream(self, stream_id: int) -> None:
        meta = trpc_pb2.TrpcStreamCloseMeta(close_type=trpc_pb2.TRPC_STREAM_CLOSE)
        self.writer.write(_frame(trpc_pb2.TRPC_STREAM_FRAME_CLOSE, stream_id, meta.SerializeToString()))

    def feedback(self, stream_id: int, increment: int) -> None:
        meta = trpc_pb2.TrpcStreamFeedBackMeta(window_size_increment=increment)
        self.writer.write(_frame(trpc_pb2.TRPC_STREAM_FRAME_FEEDBACK, stream_id, meta.SerializeToString()))

    async def frame(self, stream_id: int, timeout: float = 10) -> Tuple[int, bytes]:
        return await asyncio.wait_for(self._queue(stream_id).get(), timeout)

    async def expect_init(self, stream_id: int) -> None:
        state, body = await self.frame(stream_id)
        assert state == trpc_pb2.TRPC_STREAM_FRAME_INIT, state
        meta = trpc_pb2.TrpcStreamInitMeta()
        meta.ParseFromString(body)
        assert meta.response_meta.ret == 0, meta

    async def reply(self, stream_id: int) -> str:
        state, body = await self.frame(stream_id)
        assert state == trpc_pb2.TRPC_STREAM_FRAME_DATA, state
        return pb.HelloReply.FromString(body).msg

    async def expect_close(self, stream_id: int) -> trpc_pb2.TrpcStreamCloseMeta:
        state, body = await self.frame(stream_id)
        assert state == trpc_pb2.TRPC_STREAM_FRAME_CLOSE, state
        return trpc_pb2.TrpcStreamCloseMeta.FromString(body)

    async def aclose(self) -> None:
        self.writer.close()
        try:
            await self.writer.wait_closed()
        except ConnectionError:
            pass
        await asyncio.wait([self.reader_task], timeout=5)


def _chat_reply(reply: str) -> Tuple[str, int, int, str]:
    """``<msg>|<session_id>|<stream_id>|<index>:<ident>`` -> its four fields."""
    msg, session_id, stream_id, thread = reply.split("|")
    return msg, int(session_id), int(stream_id), thread


async def _stats(port: int) -> Dict[str, int]:
    _, ret, reply = await tcp_call(port, 1, "stats")
    assert ret == 0, reply
    return json.loads(reply)


async def _wait_drained(port: int, timeout: float = 3.0, errors: Optional[int] = None) -> Dict[str, int]:
    """Poll ``stats`` until every table is empty (and ``errors`` reached)."""
    deadline = time.monotonic() + timeout
    while True:
        stats = await _stats(port)
        drained = not any(stats[key] for key in ("streams", "conns", "metas", "handlers"))
        if drained and (errors is None or stats["errors"] >= errors):
            return stats
        if time.monotonic() > deadline:
            raise AssertionError(f"stream state left behind: {stats}")
        await asyncio.sleep(0.02)


@pytest.fixture
def start_server(tmp_path) -> Iterator:
    servers: List[Server] = []

    def start(server_extra: str, service_extra: str = "") -> Tuple[Server, int]:
        port = free_port()
        server = Server(tmp_path, {"tcp": port},
                        server_extra,
                        script_template=STREAM_SERVER_SCRIPT,
                        service_extra=service_extra)
        servers.append(server)
        server.wait_ready()
        return server, port

    yield start
    for server in servers:
        server.stop()


def _assert_clean_exit(server: Server, code: int) -> None:
    assert code == 0, server.output
    for noise in ("Event loop is closed", "Task was destroyed", "Traceback"):
        assert noise not in server.output, server.output


@pytest.mark.parametrize("shape", ["single", "separate"])
async def test_same_stream_id_on_two_connections_does_not_cross(start_server, shape):
    _, port = start_server(SHAPES[shape])
    first, second = await StreamConn.open(port), await StreamConn.open(port)
    try:
        for conn, tag in ((first, "a"), (second, "b")):
            conn.init(1)
            await conn.expect_init(1)
            conn.send(1, tag)
        msg_a, session_a, stream_a, _ = _chat_reply(await first.reply(1))
        msg_b, session_b, stream_b, _ = _chat_reply(await second.reply(1))
        assert (msg_a, stream_a) == ("a", 1)
        assert (msg_b, stream_b) == ("b", 1)
        assert session_a != session_b
        for conn in (first, second):
            conn.close_stream(1)
            assert (await conn.expect_close(1)).close_type == trpc_pb2.TRPC_STREAM_CLOSE
        await _wait_drained(port)
    finally:
        await first.aclose()
        await second.aclose()


async def test_threaded_merge_keeps_each_stream_on_its_connection_thread(start_server):
    _, port = start_server(THREADED)
    conns = [await StreamConn.open(port) for _ in range(6)]
    try:
        for conn in conns:
            conn.init(1)
            await conn.expect_init(1)
        for i in range(5):
            for conn in conns:
                conn.send(1, str(i))
        for conn in conns:
            replies = [_chat_reply(await conn.reply(1)) for _ in range(5)]
            assert [r[0] for r in replies] == [str(i) for i in range(5)]
            assert len({r[3] for r in replies}) == 1, replies
    finally:
        for conn in conns:
            await conn.aclose()


async def test_separate_spreads_streams_of_one_connection_and_keeps_order(start_server):
    _, port = start_server(SEPARATE)
    conn = await StreamConn.open(port)
    stream_ids = list(range(1, 7))
    try:
        for stream_id in stream_ids:
            conn.init(stream_id)
        for stream_id in stream_ids:
            await conn.expect_init(stream_id)
        for i in range(5):
            for stream_id in stream_ids:
                conn.send(stream_id, str(i))
        threads = set()
        for stream_id in stream_ids:
            replies = [_chat_reply(await conn.reply(stream_id)) for _ in range(5)]
            assert [r[0] for r in replies] == [str(i) for i in range(5)]
            assert len({r[3] for r in replies}) == 1, replies
            threads.add(replies[0][3])
        assert len(threads) >= 2, threads
        for stream_id in stream_ids:
            conn.close_stream(stream_id)
            await conn.expect_close(stream_id)
        await _wait_drained(port)
    finally:
        await conn.aclose()


@pytest.mark.parametrize("shape", ["single", "threaded", "separate"])
async def test_small_window_round_trips_feedback(start_server, shape):
    _, port = start_server(SHAPES[shape], service_extra="max_window_size: 1024")
    conn = await StreamConn.open(port)
    try:
        conn.init(1, window=1024)
        await conn.expect_init(1)
        for i in range(20):
            conn.send(1, f"{i}:" + "y" * 200)
        for i in range(20):
            state, body = await conn.frame(1)
            assert state == trpc_pb2.TRPC_STREAM_FRAME_DATA, state
            assert pb.HelloReply.FromString(body).msg.startswith(f"{i}:")
            conn.feedback(1, len(body))
        assert conn.feedbacks[1] > 0

        conn.send(1, "big:800:10")
        received = 0
        while True:
            state, body = await conn.frame(1)
            assert state == trpc_pb2.TRPC_STREAM_FRAME_DATA, state
            if pb.HelloReply.FromString(body).msg.startswith("done|"):
                break
            received += 1
            conn.feedback(1, len(body))
        assert received == 10
        conn.close_stream(1)
        await conn.expect_close(1)
        await _wait_drained(port)
    finally:
        await conn.aclose()


@pytest.mark.parametrize("shape", ["single", "threaded", "separate"])
async def test_client_disconnect_wakes_blocked_handlers(start_server, shape):
    _, port = start_server(SHAPES[shape])
    before = (await _stats(port))["errors"]
    conn = await StreamConn.open(port)
    for stream_id in (1, 2):
        conn.init(stream_id, window=1024)
        await conn.expect_init(stream_id)
    conn.send(1, "ping")
    await conn.reply(1)
    # Stream 2 parks in send_msg once the 1024-byte window is spent.
    conn.send(2, "big:800:10")
    await conn.reply(2)
    await conn.reply(2)
    started = time.monotonic()
    await conn.aclose()
    await _wait_drained(port, timeout=1.0, errors=before + 2)
    assert time.monotonic() - started < 1.5


@pytest.mark.parametrize("shape", ["single", "separate"])
async def test_init_then_immediate_disconnect_leaves_nothing(start_server, shape):
    _, port = start_server(SHAPES[shape])
    for i in range(50):
        conn = await StreamConn.open(port)
        conn.init(1)
        if i % 2:
            conn.send(1, "hi")
        await conn.aclose()
    await _wait_drained(port)


@pytest.mark.parametrize("shape", ["single", "separate"])
async def test_handler_finishing_before_client_close_leaves_nothing(start_server, shape):
    _, port = start_server(SHAPES[shape])
    conn = await StreamConn.open(port)
    try:
        for stream_id in range(1, 101):
            conn.init(stream_id)
            conn.send(stream_id, "end")
            if stream_id % 2:
                # Race the server's CLOSE instead of waiting for it.
                conn.close_stream(stream_id)
            await conn.expect_init(stream_id)
            assert (await conn.expect_close(stream_id)).close_type == trpc_pb2.TRPC_STREAM_CLOSE
            if not stream_id % 2:
                conn.close_stream(stream_id)
        await _wait_drained(port)
    finally:
        await conn.aclose()


async def test_handler_limits_do_not_apply_to_streams(start_server):
    _, port = start_server(SEPARATE + "\nhandler_max_concurrency: 1\nhandler_queue_size: 0")
    conn = await StreamConn.open(port)
    stream_ids = list(range(1, 7))
    try:
        for stream_id in stream_ids:
            conn.init(stream_id)
        for stream_id in stream_ids:
            await conn.expect_init(stream_id)
            conn.send(stream_id, "hi")
        for stream_id in stream_ids:
            assert _chat_reply(await conn.reply(stream_id))[0] == "hi"
    finally:
        await conn.aclose()


@pytest.mark.parametrize("shape", ["single", "threaded", "separate"])
async def test_sigterm_with_open_streams_exits_cleanly(start_server, shape):
    server, port = start_server(SHAPES[shape])
    conn = await StreamConn.open(port)
    for stream_id in (1, 2, 3):
        conn.init(stream_id)
        await conn.expect_init(stream_id)
    conn.send(1, "hi")
    await conn.reply(1)
    code = await asyncio.get_running_loop().run_in_executor(None, server.stop)
    await conn.aclose()
    _assert_clean_exit(server, code)
