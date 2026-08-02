# -*- coding: utf-8 -*-
import pytest

from trpc_sdk.trpcpb import TrpcStreamCloseMeta
from trpc_sdk.trpcpb import TrpcStreamFeedBackMeta
from trpc_sdk.trpcpb import TrpcStreamInitMeta
from trpc_sdk import server
from trpc_sdk import stream
from trpc_sdk.codec import FrameHead
from trpc_sdk.codec import Message
from trpc_sdk.codec import ServerCodec
from trpc_sdk.codec.message import get_message  # pylint: disable=no-name-in-module
from trpc_sdk.context import TrpcContext
from trpc_sdk.server import ServiceOption
from trpc_sdk.stream.server import ServerStream
from trpc_sdk.stream.server import StreamDispatcher
from trpc_sdk.stream.server import new_stream_dispatcher
from trpc_sdk.transport.server_transport_stream import TrpcServerStreamTransport

stream_spec = server.StreamDesc("test", None, True)
stream_dispatcher = stream.new_stream_dispatcher()


class MockStreamTransport(TrpcServerStreamTransport):
    """Mock stream transport"""

    def send(self, ctx: TrpcContext, req_data: bytes, stream_id: int):
        return

    def close(self, ctx: TrpcContext):
        return


def new_test_server_stream() -> ServerStream:
    """Create test server stream"""
    opts = ServiceOption()
    opts.codec = ServerCodec()
    opts.stream_transport = MockStreamTransport()
    msg = Message()
    msg.stream_id = 100
    ctx = TrpcContext()
    ctx.msg = msg

    return ServerStream(ctx, 1, opts)


@pytest.mark.asyncio
async def test_server_stream():
    """Test server stream"""
    server_stream = new_test_server_stream()
    await server_stream.feedback(10)

    msg = server_stream.context().msg
    msg.stream_frame = TrpcStreamInitMeta()
    msg.stream_frame.init_window_size = 0
    server_stream.set_send_control(msg)
    msg.stream_frame.init_window_size = 100
    server_stream.set_send_control(msg)

    msg.stream_frame = None
    try:
        server_stream.set_send_control(msg)
    except Exception as err:  # pylint: disable=broad-except
        assert str(err) == 'stream frame assert failed: TrpcStreamInitMeta'

    await server_stream.send_msg(None)

    server_stream.close_send(0, 0, 'test msg')

    server_stream.recv_queue.push((b'\n\x0ctrpc-go_4075', None))
    message = None
    await server_stream.recv_msg(message)

    server_stream.recv_queue.push((b'', EOFError))
    message = None
    try:
        await server_stream.recv_msg(message)
    except EOFError:  # pylint: disable=broad-except
        assert True
    except Exception:  # pylint: disable=broad-except
        assert False

    server_stream.recv_queue.push((b'', Exception('test exception')))
    message = None
    try:
        await server_stream.recv_msg(message)
    except Exception as err:  # pylint: disable=broad-except
        assert str(err) == 'stream is already closed'

    assert server_stream.context() is not None


@pytest.mark.asyncio
async def test_stream_dispatcher():
    """Test stream dispatcher"""
    server_dispatcher = new_stream_dispatcher()
    opts = ServiceOption()
    opts.transport = TrpcServerStreamTransport()
    server_dispatcher.init(opts)

    try:
        opts.transport = None
        server_dispatcher.init(opts)
    except Exception as err:  # pylint: disable=broad-except
        assert str(err) == 'server StreamTransport is not implemented'

    server_dispatcher.handle_error(None, None)

    # server_stream = new_test_server_stream()
    ctx = TrpcContext()
    msg = get_message(ctx)
    msg.stream_id = 100
    ctx.with_meta_data("session_id", 1)
    server_dispatcher.stream_handle_func(ctx, None, None)

    try:
        msg.frame_head = None
        server_dispatcher.stream_handle_func(ctx, None, None)
    except Exception as err:  # pylint: disable=broad-except
        assert str(err) == 'frameHead is not contained in msg'

    opts = ServiceOption()
    opts.codec = ServerCodec()
    opts.stream_transport = MockStreamTransport()
    server_dispatcher.opts = opts
    msg.stream_frame = TrpcStreamInitMeta()
    msg.frame_head = FrameHead()
    msg.frame_head.frame_state = 1
    await server_dispatcher.handle_by_stream_frame_type(ctx, None, b'')

    msg.frame_head.frame_state = 2
    await server_dispatcher.handle_by_stream_frame_type(ctx, None, b'')

    msg.stream_frame = TrpcStreamFeedBackMeta()
    msg.frame_head.frame_state = 3
    await server_dispatcher.handle_by_stream_frame_type(ctx, None, b'')

    msg.stream_frame = TrpcStreamCloseMeta()
    msg.frame_head.frame_state = 4
    await server_dispatcher.handle_by_stream_frame_type(ctx, None, b'')

    try:
        msg.frame_head.frame_state = 0
        await server_dispatcher.handle_by_stream_frame_type(ctx, None, b'')
    except Exception as err:  # pylint: disable=broad-except
        assert str(err) == 'unknown stream frame type 0'


def test_stream_transport_send():
    """Test stream transport send"""
    stream_transport = TrpcServerStreamTransport()
    ctx = TrpcContext()
    ctx.with_meta_data("session_id", 1)
    ctx.with_meta_data("fd", 1)
    with pytest.raises(ValueError):
        stream_transport.send(ctx, b'', 1)


class _MockServerStream(ServerStream):
    """Mock ServerStream"""

    def close_send(self, code: int, reason: int, msg: str):
        return

    def close_control(self):
        return

    def close_send(self, code: int, reason: int, msg: str):
        return


@pytest.mark.asyncio
async def test_filter_stream():
    """Test filter stream"""
    mock_dispatcher = StreamDispatcher()

    async def _stream_handle(server_stream):
        return

    ctx = TrpcContext()
    get_message(ctx)

    opts = ServiceOption()
    mock_stream = _MockServerStream(ctx, 1, opts)
    await mock_dispatcher.start_stream_handler(ctx, mock_stream, _stream_handle)
    opts.enable_stream_filter = True
    await mock_dispatcher.start_stream_handler(ctx, mock_stream, _stream_handle)
