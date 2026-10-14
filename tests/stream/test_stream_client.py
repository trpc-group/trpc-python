# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

import time
import socket
from typing import Tuple
from threading import Thread
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

import trpc_sdk.stream.client as stream_mod
from trpc_sdk.client.options import *
from trpc_sdk.context import TrpcContext
from trpc_sdk.codec import new_message, clone_client_message, FrameHead
from trpc_sdk.codec.serialization import SerializationType
from trpc_sdk.client.stream import new_stream
from trpc_sdk.client.client import get_options, update_message
from trpc_sdk.pool.connpool import ConnectionPool
from trpc_sdk.pool.tcpconn import open_connection
from trpc_sdk.abc.node import Node
from trpc_sdk.stream import client, ReceiveControl, SendControl
from trpc_sdk.stream.config import get_window_size, MAX_INIT_WINDOW_SIZE
from trpc_sdk import exceptions
from trpc_sdk.trpcpb import TRPC_STREAM_FRAME_CLOSE, TRPC_STREAM_FRAME_FEEDBACK, TrpcStreamFeedBackMeta

from ..test_pool import ThreadTcpServer
from ..common_util import unused_port


class _MockTransport():

    async def recv(self, ctx: TrpcContext, options: RoundTripOptions) -> bytes:
        return b'\t0\x01\x02\x00\x00\x00\x12\x00\x00\x00\x00\x00d\x00\x00\x12\x00'

    def send(self, ctx: TrpcContext, req_data: bytes, options):
        return

    def close(self, ctx: TrpcContext):
        return

    async def start(self, ctx: TrpcContext, options: RoundTripOptions):
        return


class _MockClientStream(client.ClientStream):

    def __init__(self, ctx: TrpcContext, desc: client.ClientStreamDesc, stream_id: int, method: str):
        super().__init__(ctx, desc, stream_id, method)
        self.is_error = False

    async def invoke(self, ctx, msg, client_options: Options):
        if self.is_error:
            raise Exception("error")


@pytest.mark.asyncio
async def test_new_stream():
    stream = new_stream()
    ctx = TrpcContext()
    ctx, msg = new_message(ctx)
    node = Node()
    options = [
        with_target('ip://127.0.0.1:8000'),
        with_protocol('trpc'),
        with_serialization_type(SerializationType.PB),
        with_network('tcp'),
        with_transport(_MockTransport()),
        with_selector_node(node)
    ]
    try:
        client_options: Options = get_options(msg, options)
        update_message(msg, client_options)
        await stream.start(ctx, client_options)
    except Exception:  # pylint: disable=broad-except
        assert False

    assert await stream.send(ctx, None) is None

    assert await stream.recv(ctx) == b'\x12\x00'

    try:
        stream.close(ctx)
    except Exception:  # pylint: disable=broad-except
        assert False


class ThreadTcpStreamServer(object):

    def __init__(self, port, handle=None):
        self.addr = ('127.0.0.1', port)
        self.handle = handle
        self.thread = Thread(target=self.running, daemon=False)
        self.thread.start()
        self.actives = []

    def running(self):
        self._running = True
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.bind(self.addr)
        self.sock.listen()

        while self._running:
            ss, addr = self.sock.accept()
            print("accept", ss, addr)
            if self.handle:
                self.handle(ss, addr)
            ss.send('byebye')
            ss.recv(512)
            self.actives.append(ss)

    def stop(self):
        self._running = False
        time.sleep(1)
        for s in self.actives:
            s.close()
        self.sock.close()
        print("after tcp server sock close")


class TestStreamClient():

    def setup_class(self):

        def HandlerRead(sock, addr):
            sock.send(b'')

        self.port = unused_port()
        self.tcpsrv = ThreadTcpServer("tcp", ("127.0.0.1", self.port), HandlerRead)
        self.tcpsrv.start()
        print("start simple tcpserver")

    def teardown_class(self):
        print("stop simple tcpserver")
        self.tcpsrv.stop()
        print("stop simple tcpserver end")

    def newpool(self, address: Tuple[str, int], max_idle: int, max_active: int):

        async def dial(host: str, port: int, timeout: float):
            conn = await open_connection(host, port)
            return conn

        pool = ConnectionPool(
            dial,
            "tcp",
            address,
            max_idle=max_idle,
            max_active=max_active,
        )
        return pool

    @pytest.mark.asyncio
    async def test_stream_client(self):
        options = [
            with_target('ip://127.0.0.1:' + str(self.port)),
            with_protocol('trpc'),
            with_serialization_type(SerializationType.PB),
            with_network('tcp')
        ]
        ctx = TrpcContext()
        ctx, msg = clone_client_message(ctx)
        msg.set_client_rpc_name('/trpc_sdk.stream.Greeter/SayHelloReq')
        msg.set_callee_service_name('trpc_sdk.stream.Greeter')
        msg.set_callee_method('SayHelloReqStream')

        clientStreamDesc = client.ClientStreamDesc('/trpc_sdk.stream.Greeter/SayHelloReq', True)

        stream = client.ClientStream(ctx, clientStreamDesc, 100, '/trpc_sdk.stream.Greeter/SayHelloReq')

        class test_stream():

            def __init__(self):
                self.opts = None

            async def recv(self, ctx: TrpcContext) -> bytes:
                return b''

            async def send(self, ctx: TrpcContext, req):
                return

            async def close(self, ctx: TrpcContext):
                return

            async def start(self, ctx: TrpcContext, options: List) -> Options:
                opts = Options()
                opts.max_window_size = 0
                return opts

        stream.stream = test_stream()
        try:
            await stream.init_stream(options)

            await stream.send_msg(None)
            await stream.feedback(0)
            # data rsp
            head = FrameHead()
            head.frame_type = 1
            head.frame_state = 2
            msg.set_frame_head(head)
            stream.handle_frame(ctx, b'\x12\x00')
            # close rsp
            head = FrameHead()
            head.frame_type = 1
            head.frame_state = 4
            msg.set_frame_head(head)
            stream.handle_frame(ctx, b'\x12\x00')
            # recv msg
            stream.recv_queue.push((b'', None))
            stream.opts.current_compress_type = 0
            stream.opts.serialization_type = 0
            await stream.recv_msg(None)
            # close msg
            await stream.close_send()
        except Exception as err:  # pylint: disable=broad-except
            print(err)
            assert False


@pytest.mark.asyncio
async def test_receive_control():
    try:
        r_control = ReceiveControl(1, None)
        r_control.update_left()
        assert r_control.get_left() == 1
        await r_control.on_recv(2)
    except Exception:  # pylint: disable=broad-except
        assert False


@pytest.mark.asyncio
async def test_send_control():
    try:
        s_control = SendControl(10)
        s_control.update_window(1)
        await s_control.get_window(1)
    except Exception:  # pylint: disable=broad-except
        assert False


def test_get_window_size():
    window_size = get_window_size(MAX_INIT_WINDOW_SIZE + 1)
    assert window_size == MAX_INIT_WINDOW_SIZE


@pytest.mark.asyncio
async def test_client_task_stream():
    """test client stream"""
    client_options = Options()
    client_options.compress_type = 1
    client_options.codec = None
    client_options.transport = _MockTransport()
    ctx = TrpcContext()
    ctx, _ = new_message(ctx)
    stream = client.new_task_client_stream()
    await stream.start(ctx, client_options)


@pytest.mark.asyncio
async def test_client_task_stream_init():
    """test client stream for init"""
    ctx = TrpcContext()
    ctx, _ = new_message(ctx)
    desc = client.ClientStreamDesc()
    mock_stream = _MockClientStream(ctx, desc, 1, "test")
    options = [
        with_target('ip://127.0.0.1:8000'),
        with_protocol('trpc'),
        with_serialization_type(SerializationType.PB),
        with_metadata("aa", 1),
        with_network('tcp'),
        with_transport(_MockTransport()),
    ]
    await mock_stream.init_stream(options)
    await mock_stream.init_stream(options, True)


class _Transport:
    def __init__(self):
        self.send = AsyncMock()
        self.recv = AsyncMock()
        self.close = MagicMock()


def _client_stream():
    ctx = TrpcContext()
    ctx, _ = new_message(ctx)
    stream = stream_mod.ClientStream(ctx, stream_mod.ClientStreamDesc(), 101, "/method")
    stream.stream = _Transport()
    stream.opts = SimpleNamespace(
        r_control=None,
        s_control=None,
        current_compress_type=-1,
        current_serialization_type=-1,
    )
    return stream


@pytest.mark.asyncio
async def test_close_send_server_closed_and_double_close():
    stream = _client_stream()
    stream.server_closed = True
    await stream.close_send()
    stream.stream.send.assert_not_awaited()

    stream.server_closed = False
    stream.client_closed = True
    with pytest.raises(exceptions.ServerSystemError, match="closed stream"):
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
    with pytest.raises(exceptions.ServerSystemError, match="closed conn"):
        await stream.send_msg(object())


def test_handle_close_frame_closes_transport_and_flow_control():
    stream = _client_stream()
    ctx, msg = new_message(TrpcContext())
    head = FrameHead()
    head.frame_state = TRPC_STREAM_FRAME_CLOSE
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
    ctx, msg = new_message(TrpcContext())
    head = FrameHead()
    head.frame_state = TRPC_STREAM_FRAME_FEEDBACK
    msg.set_frame_head(head)
    feedback = TrpcStreamFeedBackMeta()
    feedback.window_size_increment = 17
    msg.set_stream_frame(feedback)

    stream.handle_frame(ctx, b"")

    control.update_window.assert_called_once_with(17)


def test_handle_feedback_rejects_wrong_metadata_type():
    stream = _client_stream()
    with pytest.raises(exceptions.ClientDecodeError, match="feedback fields"):
        stream.handle_feedback(SimpleNamespace(stream_frame=object()))


@pytest.mark.asyncio
async def test_dispatch_recv_exception_synthesizes_close_frame():
    stream = _client_stream()
    stream.stream.recv.side_effect = RuntimeError("socket failed")

    await stream.dispatch()

    assert stream.server_closed is True
    rsp, err = await stream.recv_queue.pop()
    assert rsp == b""
    assert isinstance(err, stream_mod.ClientTimeoutError)
    assert stream.stream.close.call_count == 2


@pytest.mark.asyncio
async def test_feedback_sends_control_frame():
    stream = _client_stream()
    await stream.feedback(25)
    stream.stream.send.assert_awaited_once()


def test_set_window_zero_disables_both_controls():
    stream = _client_stream()
    stream.opts.r_control = object()
    stream.opts.s_control = object()

    stream.set_window(0)

    assert stream.opts.r_control is None
    assert stream.opts.s_control is None


@pytest.mark.asyncio
async def test_stream_filter_wraps_invoke_exception(monkeypatch):
    stream = _client_stream()

    class _Filters:
        async def handle(self, ctx, req, callback):
            return await callback(ctx, req)

    options = SimpleNamespace(enable_stream_filter=True, filters=_Filters())
    monkeypatch.setattr(stream_mod, "get_options", lambda *_: options)
    monkeypatch.setattr(stream_mod, "update_message", lambda *_: None)
    stream.invoke = AsyncMock(side_effect=RuntimeError("invoke failed"))
    ctx, msg = new_message(TrpcContext())

    result, err = await stream._rpc_stream(ctx, msg, [])

    assert result is None
    assert isinstance(err, RuntimeError)
