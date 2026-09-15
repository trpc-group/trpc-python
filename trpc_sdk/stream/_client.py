# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Client half of the tRPC streaming protocol.

A :class:`ClientStream` is one RPC call. ``send_msg`` encodes a DATA
frame and writes the TCP connection directly. ``recv_msg`` never reads
the socket; it pops a queue filled by the background :meth:`ClientStream.dispatch`
task that is the only reader of the client transport.

Stream ids ``0..99`` are reserved; :func:`create_stream_client` starts at
:data:`STREAM_ID` so the first public call is ``100``. The inner
worker-to-task client starts at :data:`TASK_CLIENT_STREAM_ID` so the
two id spaces cannot collide on a shared connection.
"""

from __future__ import annotations

from typing import Any
from typing import Callable
from typing import List
from typing import Optional

from trpc_sdk.abc import BaseClientStream
from trpc_sdk.abc import TrpcContext
from trpc_sdk.client import Options
from trpc_sdk.client import create_stream
from trpc_sdk.client import create_task_client_stream
from trpc_sdk.client import get_options
from trpc_sdk.client import normalize_options
from trpc_sdk.client import update_message
from trpc_sdk.codec import Message
from trpc_sdk.codec import clone_context_and_message
from trpc_sdk.codec import create_frame_head
from trpc_sdk.codec import decompress
from trpc_sdk.codec import deserialize
from trpc_sdk.common import get_or_create_event_loop
from trpc_sdk.exceptions import ClientDecodeError
from trpc_sdk.exceptions import ClientTimeoutError
from trpc_sdk.exceptions import ServerSystemError
from trpc_sdk.log import logger
from trpc_sdk.utils import Queue
from trpc_sdk._inner import trpc_pb2
from ._constants import DEFAULT_INIT_WINDOW_SIZE
from ._constants import STREAM_ID
from ._constants import TASK_CLIENT_STREAM_ID
from ._flow_control import ReceiveControl
from ._flow_control import SendControl
from ._options import get_window_size
from ._types import ClientStreamDesc


# pylint: disable=too-many-instance-attributes
class ClientStream(BaseClientStream):
    """One client streaming RPC: write TCP, read from ``recv_queue``.

    ``opts`` stays ``None`` until :meth:`init_stream` finishes the INIT
    handshake. ``client_closed`` is our CLOSE; ``server_closed`` is the
    peer's CLOSE (or a recv failure treated as CLOSE).
    """

    def __init__(self, ctx: TrpcContext, desc: ClientStreamDesc, stream_id: int, method: str) -> None:
        self.ctx = ctx  # Call context bound at construction; not replaced later.
        self.opts: Optional[Options] = None  # Resolved in init_stream; None before the INIT handshake.
        self.desc = desc  # Method descriptor (RPC name plus client-streams flag).
        self.method = method  # RPC name stamped on INIT / DATA / FEEDBACK frames.
        self.stream_id = stream_id  # Allocated by Client; 0..99 are reserved.
        self.stream = create_stream()  # Transport-level stream; _TaskClientStream swaps in the unix one.
        self.recv_queue = Queue()  # Filled only by dispatch(); recv_msg never touches the socket.
        self.client_closed = False  # We sent CLOSE.
        self.server_closed = False  # Peer sent CLOSE, or a recv failure was treated as one.

    def context(self) -> TrpcContext:
        """Return the context this stream was created with.

        Returns:
            The :class:`~trpc_sdk.abc.TrpcContext` bound at construction.
        """
        return self.ctx

    async def close_send(self) -> None:
        """Send a CLOSE frame (``ret=0``); further ``send_msg`` calls fail.

        A CLOSE already received from the server is a no-op: there is no
        peer left to tell. Dispatch keeps running so ``recv_msg`` can
        still drain the queue.

        Raises:
            ServerSystemError: This side already sent CLOSE.
        """
        if self.server_closed:
            logger.warning("stream has been closed by the server")
            return

        if self.client_closed:
            raise ServerSystemError("Close of closed stream")

        ctx, msg = clone_context_and_message(self.ctx)
        frame_head = create_frame_head(trpc_pb2.TRPC_STREAM_FRAME, trpc_pb2.TRPC_STREAM_FRAME_CLOSE, self.stream_id)
        msg.set_frame_head(frame_head)
        msg.set_stream_id(self.stream_id)

        close = trpc_pb2.TrpcStreamCloseMeta()
        close.close_type = trpc_pb2.TRPC_STREAM_CLOSE
        close.ret = 0
        msg.set_stream_frame(close)
        self.client_closed = True

        await self.stream.send(ctx, None)

    async def recv_msg(self, msg: Any) -> None:
        """Pop the recv queue and deserialize into ``msg``.

        Flow-control accounting runs on the payload length *before*
        decompress, matching the window the peer counted when it sent.

        Args:
            msg: Empty response object filled in place.

        Raises:
            BaseException: The queue item's error (EOF, timeout, ...).
        """
        rsp, err = await self.recv_queue.pop()

        if err is not None:
            raise err

        if len(rsp) > 0:
            if self.opts.r_control is not None:
                await self.opts.r_control.on_recv(len(rsp))

            client_msg = self.ctx.get_client_message()
            # Prefer per-call overrides; otherwise take compress / serialization
            # from the backend's INIT/DATA response metadata.
            compress_type = client_msg.compress_type
            if self.opts.current_compress_type >= 0:
                compress_type = self.opts.current_compress_type
            serialization_type = client_msg.serialization_type
            if self.opts.current_serialization_type >= 0:
                serialization_type = self.opts.current_serialization_type
            rsp_body = decompress(compress_type, rsp)
            deserialize(serialization_type, rsp_body, msg)

    async def send_msg(self, msg: Any) -> None:
        """Encode ``msg`` as a DATA frame and write the transport.

        Args:
            msg: Request object to serialize.

        Raises:
            ServerSystemError: Either side has already closed the stream.
        """
        if self.client_closed or self.server_closed:
            raise ServerSystemError("Cannot write into closed conn")

        ctx, client_msg = clone_context_and_message(self.ctx)

        frame_head = create_frame_head(trpc_pb2.TRPC_STREAM_FRAME, trpc_pb2.TRPC_STREAM_FRAME_DATA, self.stream_id)
        client_msg.set_frame_head(frame_head)
        client_msg.set_stream_id(self.stream_id)
        client_msg.set_client_rpc_name(self.method)

        await self.stream.send(ctx, msg)

    def handle_frame(self, ctx: TrpcContext, rsp_data: bytes) -> None:
        """Dispatch one received frame into the queue or flow-control.

        DATA is queued for :meth:`recv_msg`. CLOSE ends the recv loop and
        wakes :class:`SendControl`. FEEDBACK only enlarges the send window.

        Args:
            ctx: Context whose client message holds the decoded frame head.
            rsp_data: DATA payload, ignored for CLOSE / FEEDBACK.
        """
        msg: Message = ctx.get_client_message()
        frame_state = msg.frame_head.frame_state

        if frame_state == trpc_pb2.TRPC_STREAM_FRAME_DATA:
            self.recv_queue.push((rsp_data, None))
            return
    
        if frame_state == trpc_pb2.TRPC_STREAM_FRAME_CLOSE:
            err = msg.client_rsp_err or EOFError
            self.recv_queue.push((b"", err), True)
            self.stream.close(ctx)

            if self.opts.s_control:
                self.opts.s_control.close_control()
            self.server_closed = True
            return
    
        if frame_state == trpc_pb2.TRPC_STREAM_FRAME_FEEDBACK:
            self.handle_feedback(msg)

    def handle_feedback(self, msg: Message) -> None:
        """Apply a FEEDBACK frame to the send window.

        Args:
            msg: Message whose ``stream_frame`` must be
                :class:`trpc_pb2.TrpcStreamFeedBackMeta`.

        Raises:
            ClientDecodeError: ``stream_frame`` is the wrong type.
        """
        if not isinstance(msg.stream_frame, trpc_pb2.TrpcStreamFeedBackMeta):
            raise ClientDecodeError(
                "msg.stream_frame feedback fields must be type of TrpcStreamFeedBackMeta")

        if self.opts.s_control:
            self.opts.s_control.update_window(msg.stream_frame.window_size_increment)

    async def dispatch(self) -> None:
        """Background reader: pull frames from the transport until CLOSE.

        Recv failures are turned into a synthetic CLOSE with
        :class:`~trpc_sdk.exceptions.ClientTimeoutError` so ``recv_msg``
        unblocks rather than hanging on an empty queue.
        """
        while not self.server_closed:
            ctx, msg = clone_context_and_message(self.ctx)
            msg.set_stream_id(self.stream_id)
            try:
                rsp_data = await self.stream.recv(ctx)
            except Exception as err:  # pylint: disable=broad-except
                logger.error(f"recv frame from conn failed, err: {repr(err)}")
                self.server_closed = True

                ctx.get_client_message().frame_head.frame_state = trpc_pb2.TRPC_STREAM_FRAME_CLOSE
                ctx.get_client_message().client_rsp_err = ClientTimeoutError("recv frame from conn timeout")
                self.handle_frame(ctx, None)
                break

            self.handle_frame(ctx, rsp_data)

        if self.server_closed:
            self.stream.close(self.ctx)

    async def feedback(self, increment: int) -> None:
        """Write a FEEDBACK frame advertising ``increment`` more recv credit.

        Args:
            increment: Bytes the peer may add to its send window.
        """
        ctx, msg = clone_context_and_message(self.ctx)
        frame_head = create_frame_head(trpc_pb2.TRPC_STREAM_FRAME, trpc_pb2.TRPC_STREAM_FRAME_FEEDBACK, self.stream_id)
        msg.set_frame_head(frame_head)
        msg.set_stream_id(self.stream_id)
        msg.set_client_rpc_name(self.method)

        feedback_meta = trpc_pb2.TrpcStreamFeedBackMeta()
        feedback_meta.window_size_increment = increment
        msg.set_stream_frame(feedback_meta)

        await self.stream.send(ctx, None)

    def set_send_control(self, window_size: int) -> None:
        """Install send-side flow control from the server's INIT window.

        ``0`` disables both send and receive control, matching a peer
        that opted out. Receive control is created earlier in
        :meth:`invoke` from *our* INIT window.

        Args:
            window_size: ``trpc_pb2.TrpcStreamInitMeta.init_window_size`` from the
                server, or :data:`DEFAULT_INIT_WINDOW_SIZE` when the
                INIT body was missing.
        """
        if window_size == 0:
            self.opts.r_control = None
            self.opts.s_control = None
            return

        self.opts.s_control = SendControl(window_size)

    async def init_stream(self, options: List[Callable], is_task_stream: bool = False) -> None:
        """Send INIT and start :meth:`dispatch`.

        Public RPC streams go through client filters and naming;
        inner task streams skip those and only apply option callables
        plus metadata.

        Args:
            options: Client option callables for this call.
            is_task_stream: ``True`` selects the inner worker-to-task path.
        """
        ctx, msg = clone_context_and_message(self.ctx)
        frame_head = create_frame_head(trpc_pb2.TRPC_STREAM_FRAME, trpc_pb2.TRPC_STREAM_FRAME_INIT, self.stream_id)
        msg.set_frame_head(frame_head)
        msg.set_stream_id(self.stream_id)
        msg.set_client_rpc_name(self.method)

        if not is_task_stream:
            await self._rpc_stream(ctx, msg, options)
        else:
            await self._task_stream(ctx, msg, options)

    async def _rpc_stream(self, ctx, msg, options: List[Callable]) -> None:
        """INIT handshake for a public ``trpc_sdk.stream`` call."""
        client_options: Options = get_options(msg, options)
        update_message(msg, client_options)

        async def _wrapper_filter(ctx: TrpcContext, req):
            try:
                await self.invoke(ctx, msg, client_options)
            except Exception as e:
                return None, e
            return None, None

        if client_options.enable_stream_filter:
            return await client_options.filters.handle(ctx, None, _wrapper_filter)
        await self.invoke(ctx, msg, client_options)

    async def _task_stream(self, ctx, msg, options: List[Callable]) -> None:
        """INIT handshake for the inner worker-to-task stream.

        Naming / filters are skipped: the Unix path is already on the
        options. Metadata from ``options`` is merged onto the cloned
        client message before INIT.
        """
        client_options = Options()
        for func in normalize_options(options):
            func(client_options)
        if client_options.metadata:
            meta_data = msg.client_meta_data or {}
            meta_data.update(client_options.metadata)
            msg.set_client_meta_data(meta_data)
        await self.invoke(ctx, msg, client_options)

    async def invoke(self, ctx, msg, client_options: Options) -> None:
        """Dial, send INIT, apply the peer window, and start dispatch.

        Uses the cloned ``msg`` (not ``self.ctx``) so the INIT frame does
        not mutate the caller's original message.

        Args:
            ctx: Cloned context used for this handshake.
            msg: Cloned client message carrying the INIT frame head.
            client_options: Resolved options; stored on ``self.opts``.

        Raises:
            Exception: The server's INIT response set ``client_rsp_err``.
        """
        self.opts = await self.stream.start(ctx, client_options)

        client_window_size = get_window_size(self.opts.max_window_size)
        init_meta = trpc_pb2.TrpcStreamInitMeta()
        init_meta.init_window_size = client_window_size
        msg.set_stream_frame(init_meta)

        self.opts.r_control = ReceiveControl(client_window_size, self.feedback)
        await self.stream.send(ctx, None)
        await self.stream.recv(ctx)

        if msg.client_rsp_err is not None:
            raise Exception(msg.client_rsp_err)  # pylint: disable=broad-exception-raised

        if isinstance(msg.stream_frame, trpc_pb2.TrpcStreamInitMeta):
            self.set_send_control(msg.stream_frame.init_window_size)
        else:
            self.set_send_control(DEFAULT_INIT_WINDOW_SIZE)

        loop = get_or_create_event_loop()
        loop.create_task(self.dispatch())


class _TaskClientStream(ClientStream):
    """:class:`ClientStream` that dials the inner worker-to-task transport."""

    def __init__(self, ctx: TrpcContext, desc: ClientStreamDesc, stream_id: int, method: str) -> None:
        super().__init__(ctx, desc, stream_id, method)
        self.stream = create_task_client_stream()


# pylint: disable=too-few-public-methods
class Client:
    """Allocates monotonically increasing stream ids and opens streams."""

    def __init__(self, stream_id: int, is_task_stream: bool = False) -> None:
        self.stream_id = stream_id
        self._is_task_stream = is_task_stream

    async def create_client_stream(
            self,
            ctx: TrpcContext,
            desc: ClientStreamDesc,
            method: str,
            options: List[Callable],
    ) -> ClientStream:
        """Allocate a stream id, construct the stream, and run INIT.

        Args:
            ctx: Call context.
            desc: Method descriptor (name plus client-streams flag).
            method: RPC name stamped on INIT / DATA frames.
            options: Client option callables.

        Returns:
            An initialized :class:`ClientStream` (or the inner-task
            variant when this client was built with ``is_task_stream``).
        """
        self.stream_id += 1
        if self._is_task_stream:
            client_stream = _TaskClientStream(ctx, desc, self.stream_id, method)
        else:
            client_stream = ClientStream(ctx, desc, self.stream_id, method)

        await client_stream.init_stream(options, self._is_task_stream)
        return client_stream


def create_stream_client() -> Client:
    """Build a public-protocol stream client starting at :data:`STREAM_ID`.

    Returns:
        A :class:`Client` whose first allocated id is ``100``.
    """
    return Client(STREAM_ID)


def create_task_stream_client() -> Client:
    """Build an inner worker-to-task stream client.

    Returns:
        A :class:`Client` whose first allocated id is
        ``TASK_CLIENT_STREAM_ID + 1``.
    """
    return Client(TASK_CLIENT_STREAM_ID, is_task_stream=True)


default_stream_client = create_stream_client()

default_task_stream_client = create_task_stream_client()
