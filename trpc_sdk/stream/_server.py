# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Server half of the tRPC streaming protocol.

:class:`ServerStream` is one accepted call. ``send_msg`` encodes a DATA
frame and writes the transport directly. ``recv_msg`` never reads the
socket; it pops a queue filled by :class:`StreamDispatcher` when a DATA
or CLOSE frame for this ``(session_id, stream_id)`` arrives through
``Service.handle``.

:class:`StreamDispatcher` is the per-service map of live streams and the
INIT / DATA / CLOSE / FEEDBACK switch.
"""

from __future__ import annotations

from collections import defaultdict
from typing import Awaitable
from typing import Callable
from typing import Dict
from typing import Optional

from trpc_sdk.abc import BaseServerStream
from trpc_sdk.abc import TrpcContext
from trpc_sdk.codec import Message
from trpc_sdk.codec import clone_context_and_message
from trpc_sdk.codec import compress
from trpc_sdk.codec import create_frame_head
from trpc_sdk.codec import decompress
from trpc_sdk.codec import deserialize
from trpc_sdk.codec import get_message
from trpc_sdk.codec import serialize
from trpc_sdk.common import get_or_create_event_loop
from trpc_sdk.exceptions import ServerSystemError
from trpc_sdk.exceptions import TrpcErrorCode
from trpc_sdk.log import logger
from trpc_sdk.utils import Queue
from trpc_sdk._inner import trpc_pb2
from ._flow_control import ReceiveControl
from ._flow_control import SendControl
from ._options import get_window_size


class ServerStream(BaseServerStream):
    """One accepted streaming RPC: write TCP, read from ``recv_queue``.

    ``s_control`` is send-side credit taken from the client's INIT window.
    ``r_control`` is receive-side accounting of our own window; it fires
    :meth:`feedback` (a FEEDBACK frame) after a quarter of the buffer.
    """

    def __init__(self, ctx: TrpcContext, stream_id: int, opts) -> None:
        # opts is a trpc_sdk.server.ServiceOption; left untyped to
        # avoid a runtime import cycle with trpc_sdk.server.
        self.ctx = ctx
        self.opts = opts
        self.stream_id = stream_id
        self.recv_queue = Queue()
        self.client_close = False
        self.s_control: Optional[SendControl] = None
        self.r_control: Optional[ReceiveControl] = None

    async def feedback(self, window: int) -> None:
        """Write a FEEDBACK frame advertising ``window`` more recv credit.

        Args:
            window: Bytes the client may add to its send window.
        """
        ctx, msg = clone_context_and_message(self.ctx)

        frame_head = create_frame_head(trpc_pb2.TRPC_STREAM_FRAME, trpc_pb2.TRPC_STREAM_FRAME_FEEDBACK, self.stream_id)
        msg.set_frame_head(frame_head)
        msg.set_stream_id(self.stream_id)

        feedback_meta = trpc_pb2.TrpcStreamFeedBackMeta()
        feedback_meta.window_size_increment = window
        msg.set_stream_frame(feedback_meta)

        buf_data = self.opts.codec.encode(msg, None)
        self.opts.stream_transport.send(ctx, buf_data, self.stream_id)

    def set_send_control(self, msg: Message) -> int:
        """Install send-side flow control from the client's INIT window.

        A client window of ``0`` means the client opted out: both
        ``s_control`` and ``r_control`` are cleared so this stream
        neither gates sends nor emits FEEDBACK.

        Args:
            msg: INIT message whose ``stream_frame`` is
                :class:`trpc_pb2.TrpcStreamInitMeta`.

        Returns:
            The client's ``init_window_size``.

        Raises:
            ServerSystemError: ``stream_frame`` is not an init meta.
        """
        if not isinstance(msg.stream_frame, trpc_pb2.TrpcStreamInitMeta):
            raise ServerSystemError("stream frame assert failed: TrpcStreamInitMeta")

        init_window_size = msg.stream_frame.init_window_size

        if init_window_size == 0:
            self.s_control = None
            self.r_control = None
        else:
            self.s_control = SendControl(init_window_size)
        return init_window_size

    def compress_and_serialize(self, msg: Message, body: object) -> bytes:
        """Serialize then compress ``body`` using ``msg`` (or option overrides).

        Args:
            msg: Message supplying serialization / compress types.
            body: Object to encode as a DATA payload.

        Returns:
            Compressed payload bytes.
        """
        serialization_type = msg.serialization_type
        compress_type = msg.compress_type
        if self.opts.current_serialization_type >= 0:
            serialization_type = self.opts.current_serialization_type
        if self.opts.current_compress_type >= 0:
            compress_type = self.opts.current_compress_type
        req_body_buf = serialize(serialization_type, body)
        req_body = compress(compress_type, req_body_buf)
        return req_body

    def decompress_and_deserialize(self, rsp: bytes, body: object):
        """Decompress then deserialize ``rsp`` into ``body``.

        Types come from the *client* message on ``self.ctx`` (the INIT
        that opened this stream), not from the DATA frame itself.

        Args:
            rsp: Compressed DATA payload.
            body: Empty object filled in place.
        """
        msg = self.ctx.get_client_message()
        compress_type = msg.compress_type
        if self.opts.current_compress_type >= 0:
            compress_type = self.opts.current_compress_type
        serialization_type = msg.serialization_type
        if self.opts.current_serialization_type >= 0:
            serialization_type = self.opts.current_serialization_type
        rsp_body = decompress(compress_type, rsp)
        deserialize(serialization_type, rsp_body, body)

    async def send_msg(self, body: object) -> None:
        """Encode ``body`` as DATA and write the transport.

        Flow control applies only to the serialized payload, not the
        frame header: :meth:`SendControl.acquire_window` sees ``len(buf)``.

        Args:
            body: Object to send.
        """
        msg = self.ctx.get_client_message()
        new_ctx, new_msg = clone_context_and_message(self.ctx)

        frame_head = create_frame_head(trpc_pb2.TRPC_STREAM_FRAME, trpc_pb2.TRPC_STREAM_FRAME_DATA, self.stream_id)
        new_msg.set_frame_head(frame_head)
        new_msg.set_stream_id(msg.stream_id)
        buf = self.compress_and_serialize(new_msg, body)

        if buf is not None and self.s_control is not None:
            await self.s_control.acquire_window(len(buf))

        buf_data = self.opts.codec.encode(new_msg, buf)
        self.opts.stream_transport.send(new_ctx, buf_data, self.stream_id)

    async def recv_msg(self, body: object):
        """Pop the recv queue and deserialize into ``body``.

        Args:
            body: Empty object filled in place.

        Raises:
            EOFError: The client sent a successful CLOSE.
            ServerSystemError: The client sent a failed CLOSE, or the
                stream was reset.
        """
        rsp, err = await self.recv_queue.pop()
        if err is EOFError:
            raise err
        if err is not None:
            raise ServerSystemError("stream is already closed")

        if self.r_control is not None:
            await self.r_control.on_recv(len(rsp))

        return self.decompress_and_deserialize(rsp, body)

    def close_send(self, close_type: int, ret_code: int, ret_msg: str) -> None:
        """Write a CLOSE frame; does not drop the TCP connection.

        Args:
            close_type: ``trpc_pb2.TRPC_STREAM_CLOSE`` or ``trpc_pb2.TRPC_STREAM_RESET``.
            ret_code: Framework / business return code on the close meta.
            ret_msg: Human-readable close message, encoded as UTF-8.
        """
        msg = self.ctx.get_client_message()
        new_ctx, new_msg = clone_context_and_message(self.ctx)

        frame_head = create_frame_head(trpc_pb2.TRPC_STREAM_FRAME, trpc_pb2.TRPC_STREAM_FRAME_CLOSE, self.stream_id)
        new_msg.set_frame_head(frame_head)
        new_msg.set_stream_id(msg.stream_id)

        close = trpc_pb2.TrpcStreamCloseMeta()
        close.close_type = close_type
        close.ret = ret_code
        close.msg = str.encode(ret_msg)
        new_msg.set_stream_frame(close)

        buf_data = self.opts.codec.encode(new_msg, None)
        self.opts.stream_transport.send(new_ctx, buf_data, self.stream_id)

    def context(self) -> TrpcContext:
        """Return the context this stream was created with.

        Returns:
            The :class:`~trpc_sdk.abc.TrpcContext` bound at construction.
        """
        return self.ctx

    def push_to_recv_queue(self, data: tuple) -> None:
        """Push ``(payload, err)`` for :meth:`recv_msg` to pop.

        Args:
            data: ``(bytes, None)`` for DATA, ``(None, EOFError)`` for a
                successful CLOSE, or ``(None, err)`` for a failed CLOSE.
        """
        self.recv_queue.push(data)


ServerStreamHandle = Callable[[ServerStream], Awaitable]


class StreamDispatcher:
    """Per-service map of live streams and the INIT/DATA/CLOSE/FEEDBACK switch.

    ``stream_to_ss[session_id][stream_id]`` is the :class:`ServerStream`
    created at INIT. Subsequent DATA / CLOSE / FEEDBACK frames look the
    same pair up. ``is_task`` selects the inner worker-to-task transport
    at :meth:`init` time.
    """

    def __init__(self, is_task=False) -> None:
        self.stream_to_ss: Dict[int, Dict[int, ServerStream]] = defaultdict(dict)
        self.opts = None
        self._is_task = is_task

    @property
    def is_task(self) -> bool:
        """Return whether this dispatcher serves the inner task stream."""
        return self._is_task

    def init(self, opts) -> None:
        """Attach ``opts.transport`` as the stream write path.

        Imported here, not at module level: ``trpc_sdk.stream._server`` is loaded
        while ``trpc_sdk.server._task`` is still importing this package, and
        ``trpc_sdk.transport`` reaches ``trpc_sdk.task`` and then this module again.

        Args:
            opts: A :class:`~trpc_sdk.server.ServiceOption` whose
                ``transport`` must already be a
                :class:`~trpc_sdk.transport.TrpcServerStreamTransport`.
                Left untyped to avoid a runtime import cycle with
                ``trpc_sdk.server``.

        Raises:
            TypeError: ``opts.transport`` is not a stream transport.
        """
        from trpc_sdk.transport import TrpcServerStreamTransport
        self.opts = opts
        if not isinstance(opts.transport, TrpcServerStreamTransport):
            raise TypeError("server StreamTransport is not implemented")
        opts.stream_transport = opts.transport

    def _error_msg(self, info: str, session_id: int, stream_id: int) -> str:
        """Build a dispatcher error string that includes session / stream ids.

        Args:
            info: What failed (duplicate, missing, ...).
            session_id: Connection-lifetime id of the client TCP.
            stream_id: Stream id from the tRPC frame head.
        """
        return f"{info} session_id: {session_id}, stream_id: {stream_id}, is_task: {self._is_task}"

    def store_server_stream(self, ctx: TrpcContext, server_stream: ServerStream) -> None:
        """Remember ``server_stream`` under ``(session_id, stream_id)``.

        Args:
            ctx: INIT context whose message carries the ids.
            server_stream: Stream created for this call.

        Raises:
            ServerSystemError: That pair is already in the map.
        """
        msg: Message = get_message(ctx)
        session_id, stream_id = msg.session_id, msg.stream_id
        if stream_id in self.stream_to_ss[session_id]:
            raise ServerSystemError(self._error_msg("[server stream] duplicate", session_id, stream_id))
        self.stream_to_ss[session_id][stream_id] = server_stream

    def delete_server_stream(self, ctx: TrpcContext) -> None:
        """Drop the stream for ``ctx``; log if it was already gone.

        Args:
            ctx: TrpcContext whose message carries the ids to remove.
        """
        msg: Message = get_message(ctx)
        session_id, stream_id = msg.session_id, msg.stream_id
        if stream_id not in self.stream_to_ss[session_id]:
            logger.error(self._error_msg("[server stream] delete stream error, not found", session_id, stream_id))
        else:
            self.stream_to_ss[session_id].pop(stream_id)
        if len(self.stream_to_ss[session_id]) == 0:
            del self.stream_to_ss[session_id]

    def get_server_stream(self, ctx: TrpcContext) -> ServerStream:
        """Look up the stream for ``ctx``.

        Args:
            ctx: TrpcContext whose message carries session / stream ids.

        Returns:
            The stored :class:`ServerStream`.

        Raises:
            ServerSystemError: No stream for that pair.
        """
        msg: Message = get_message(ctx)
        session_id, stream_id = msg.session_id, msg.stream_id
        if stream_id not in self.stream_to_ss[session_id]:
            info = f"[server stream] get stream error, frame_state: {msg.frame_head.frame_state}"
            raise ServerSystemError(self._error_msg(info, session_id, stream_id))

        return self.stream_to_ss[session_id][stream_id]

    def handle_error(self, ctx: TrpcContext, req_buf: bytes) -> None:
        """Hook for a connection-level error with no frame head.

        The default is a no-op; subclasses may notify the upper service.
        """
        # pylint: disable=unnecessary-pass
        pass

    async def start_stream_handler(
            self,
            ctx: TrpcContext,
            server_stream: ServerStream,
            stream_handle: ServerStreamHandle,
    ) -> None:
        """Run the business handler, then CLOSE or RESET the stream.

        The stream is *not* deleted in ``finally`` as soon as the handler
        returns: a CLOSE (or DATA) still in flight would miss the map.
        Cleanup waits until ``client_close`` so trailing frames can still
        look the stream up.

        Args:
            ctx: INIT context that opened this stream.
            server_stream: Stream the handler reads and writes.
            stream_handle: Business coroutine, ``async def(stream)``.
        """
        try:

            async def _wrapper(ctx, req):
                await stream_handle(server_stream)
                return None, None

            if self.opts.enable_stream_filter:
                await self.opts.filters.handle(ctx, None, _wrapper, -1)
            else:
                await stream_handle(server_stream)
            server_stream.close_send(trpc_pb2.TRPC_STREAM_CLOSE, 0, "")
        except Exception as ex:  # pylint: disable=broad-except
            logger.error(f"stream_handle failed: {ex}")
            server_stream.close_send(trpc_pb2.TRPC_STREAM_RESET, 0, str(ex))
        finally:
            if server_stream.s_control:
                server_stream.s_control.close_control()
            if server_stream.client_close:
                self.delete_server_stream(ctx)
                self.opts.stream_transport.delete_stream_conn(ctx)

    async def handle_init(self, ctx: TrpcContext, stream_handle: ServerStreamHandle) -> bytes:
        """Accept INIT: create the stream, reply with our window, start the handler.

        The INIT response window is ``0`` when the client opted out
        (client window ``<= 0``); otherwise it is this service's clamped
        ``max_window_size``. Receive control is installed *before*
        :meth:`ServerStream.set_send_control`, which will clear it again
        if the client sent window ``0``.

        Args:
            ctx: INIT context.
            stream_handle: Business coroutine scheduled on the event loop.
        """
        msg: Message = get_message(ctx)
        stream_id = msg.stream_id
        server_stream = ServerStream(ctx, stream_id, self.opts)
        self.store_server_stream(ctx, server_stream)
        self.opts.stream_transport.store_stream_conn(ctx)

        new_ctx, new_msg = clone_context_and_message(ctx)

        frame_head = create_frame_head(trpc_pb2.TRPC_STREAM_FRAME, trpc_pb2.TRPC_STREAM_FRAME_INIT, stream_id)
        new_msg.set_frame_head(frame_head)
        new_msg.set_stream_id(stream_id)

        server_window = get_window_size(self.opts.max_window_size)
        server_stream.r_control = ReceiveControl(window_size=server_window, send_feedback=server_stream.feedback)

        client_window_size = server_stream.set_send_control(msg)

        init_meta = trpc_pb2.TrpcStreamInitMeta()
        if client_window_size <= 0:
            init_meta.init_window_size = 0
        else:
            init_meta.init_window_size = server_window
        new_msg.set_stream_frame(init_meta)

        rsp_buf = self.opts.codec.encode(new_msg, None)
        self.opts.stream_transport.send(new_ctx, rsp_buf, stream_id)

        loop = get_or_create_event_loop()
        loop.create_task(self.start_stream_handler(ctx, server_stream, stream_handle))

    def handle_data(self, ctx: TrpcContext, req_buf: bytes) -> bytes:
        """Queue a DATA payload for the matching stream's ``recv_msg``.

        Args:
            ctx: DATA context.
            req_buf: Decoded body bytes.
        """
        server_stream: ServerStream = self.get_server_stream(ctx)
        server_stream.push_to_recv_queue((req_buf, None))

    def handle_close(self, ctx: TrpcContext) -> None:
        """Mark ``client_close`` and wake ``recv_msg`` with EOF or the error.

        Args:
            ctx: CLOSE context.
        """
        server_stream: ServerStream = self.get_server_stream(ctx)
        server_stream.client_close = True
        msg: Message = get_message(ctx)
        if msg.server_rsp_err.code != TrpcErrorCode.TRPC_INVOKE_SUCCESS:
            server_stream.push_to_recv_queue((None, msg.server_rsp_err))
            return

        server_stream.push_to_recv_queue((None, EOFError))

    def handle_feedback(self, ctx: TrpcContext) -> None:
        """Apply a FEEDBACK frame to the matching stream's send window.

        Args:
            ctx: FEEDBACK context.

        Raises:
            ServerSystemError: ``stream_frame`` is the wrong type.
        """
        msg = get_message(ctx)
        if not isinstance(msg.stream_frame, trpc_pb2.TrpcStreamFeedBackMeta):
            raise ServerSystemError("msg.stream_frame feedback fields must be type of TrpcStreamFeedBackMeta")

        server_stream: ServerStream = self.get_server_stream(ctx)
        if server_stream.s_control:
            server_stream.s_control.update_window(msg.stream_frame.window_size_increment)

    async def handle_by_stream_frame_type(
            self,
            ctx: TrpcContext,
            stream_handle: ServerStreamHandle,
            body: bytes,
    ) -> bytes:
        """Switch on ``frame_state`` and handle INIT / DATA / CLOSE / FEEDBACK.

        Non-INIT frames clear ``aio_transport`` on ``ctx`` so a cloned
        context cannot keep a stale write handle.

        Args:
            ctx: Frame context.
            stream_handle: Business handler, used only for INIT.
            body: Decoded DATA payload; ignored for other frame types.

        Raises:
            ValueError: Unknown ``frame_state``.
        """
        msg = get_message(ctx)
        frame_state = msg.frame_head.frame_state
        if frame_state == trpc_pb2.TRPC_STREAM_FRAME_INIT:
            return await self.handle_init(ctx, stream_handle)
        ctx.with_meta_data("aio_transport", None)
        if frame_state == trpc_pb2.TRPC_STREAM_FRAME_DATA:
            return self.handle_data(ctx, body)
        if frame_state == trpc_pb2.TRPC_STREAM_FRAME_CLOSE:
            return self.handle_close(ctx)
        if frame_state == trpc_pb2.TRPC_STREAM_FRAME_FEEDBACK:
            return self.handle_feedback(ctx)
        raise ValueError(f"unknown stream frame type {frame_state}")

    def stream_handle_func(self, ctx: TrpcContext, stream_handle: ServerStreamHandle, req_buf: bytes):
        """Entry from ``Service.handle``: require a frame head, then dispatch.

        A missing frame head plus ``server_rsp_err`` is a connection-level
        failure and goes to :meth:`handle_error` instead of raising.

        Args:
            ctx: Request context.
            stream_handle: Business coroutine registered for this RPC.
            req_buf: Decoded body.

        Raises:
            ServerSystemError: No frame head and no ``server_rsp_err``.
        """
        msg: Message = get_message(ctx)
        if msg.frame_head is None:
            if msg.server_rsp_err:
                return self.handle_error(ctx, req_buf)
            raise ServerSystemError("frameHead is not contained in msg")

        return self.handle_by_stream_frame_type(ctx, stream_handle, req_buf)


def create_stream_dispatcher(is_task: bool = False) -> StreamDispatcher:
    """Build a :class:`StreamDispatcher`.

    Args:
        is_task: ``True`` for the inner worker-to-task stream.

    Returns:
        A new dispatcher with an empty stream map.
    """
    return StreamDispatcher(is_task)


def create_server_stream(ctx: TrpcContext, stream_id: int, opts) -> ServerStream:
    """Build a :class:`ServerStream` that is not yet in a dispatcher map.

    Args:
        ctx: Call context.
        stream_id: Stream id from the INIT frame.
        opts: A :class:`~trpc_sdk.server.ServiceOption` (codec, transport,
            window). Left untyped to avoid a runtime import cycle with
            ``trpc_sdk.server``.

    Returns:
        A new :class:`ServerStream`.
    """
    return ServerStream(ctx, stream_id, opts)
