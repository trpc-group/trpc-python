# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Task-process side of the inner worker-to-task stream.

:class:`TaskStreamServiceAdapter` sits in front of a
:class:`BaseTaskStreamServiceHandler`: it decodes each frame, looks the
RPC name up in :class:`TaskStreamMethodType`, and feeds the matching
handler through :class:`~trpc_sdk.stream.StreamDispatcher`.

Default method wrappers enable only the send/recv operations legal for
each kind. Application code subclasses the handler ABC (defined in
:mod:`trpc_sdk.abc`).

This module is framework-internal IPC between a worker and a task
process, not the public ``trpc_sdk.stream`` protocol.
"""

from __future__ import annotations

from typing import Any

from trpc_sdk.abc import BaseCodec
from trpc_sdk.abc import BaseTaskStreamServerMethod
from trpc_sdk.abc import BaseTaskStreamServiceHandler
from trpc_sdk.abc import TaskStreamMethodType
from trpc_sdk.abc import TrpcContext
from trpc_sdk.abc import TrpcMessage
from trpc_sdk.abc import get_message
from trpc_sdk.codec import INNER_TASK_STREAM
from trpc_sdk.codec import TaskStreamClientReadFrameError
from trpc_sdk.codec import get_server_codec
from trpc_sdk.log import logger
from trpc_sdk._inner import trpc_pb2

from ._server import ServerStream
from ._server import StreamDispatcher
from ._server import create_stream_dispatcher


class _DefaultServerReqMethod(BaseTaskStreamServerMethod):
    """Server-side client-streaming wrapper: ``recv`` then ``send_and_close``."""

    def __init__(self, stream: ServerStream, service: BaseTaskStreamServiceHandler) -> None:
        super().__init__(stream)
        self.service = service

    async def send_and_close(self, rsp: Any):
        """Send ``rsp`` as the last DATA frame.

        The dispatcher CLOSE is written after the handler returns; this
        method only sends the payload.

        Args:
            rsp: Response object.
        """
        return await self.server_stream.send_msg(rsp)

    async def recv(self) -> Any:
        """Receive the next request.

        Returns:
            A request object from :meth:`BaseTaskStreamServiceHandler.create_req_obj`.
        """
        req = self.service.create_req_obj()
        await self.server_stream.recv_msg(req)
        return req

    async def send(self, req: Any):
        """Not supported on a server req stream.

        Raises:
            NotImplementedError: Always.
        """
        raise NotImplementedError("not support 'send' in server req stream")


class _DefaultServerRspMethod(BaseTaskStreamServerMethod):
    """Server-side server-streaming wrapper: only ``send``."""

    def __init__(self, stream: ServerStream, service: BaseTaskStreamServiceHandler) -> None:
        super().__init__(stream)
        self.service = service

    async def send(self, req: Any):
        """Send one response DATA frame.

        Args:
            req: Response object (named ``req`` for historical reasons).
        """
        return await self.server_stream.send_msg(req)

    async def recv(self) -> Any:
        """Not supported on a server rsp stream; the request was already read.

        Raises:
            NotImplementedError: Always.
        """
        raise NotImplementedError("not support 'recv' in server rsp stream")

    async def send_and_close(self, rsp: Any):
        """Not supported on a server rsp stream.

        Raises:
            NotImplementedError: Always.
        """
        raise NotImplementedError("not support 'send_and_close' in server rsp stream")


class _DefaultServerBidiMethod(BaseTaskStreamServerMethod):
    """Server-side bidirectional wrapper: ``send`` and ``recv``."""

    def __init__(self, stream: ServerStream, service: BaseTaskStreamServiceHandler) -> None:
        super().__init__(stream)
        self.service = service

    async def send(self, req: Any):
        """Send one DATA frame.

        Args:
            req: Object to send.
        """
        return await self.server_stream.send_msg(req)

    async def recv(self) -> Any:
        """Receive the next request.

        Returns:
            A request object from :meth:`BaseTaskStreamServiceHandler.create_req_obj`.
        """
        req = self.service.create_req_obj()
        await self.server_stream.recv_msg(req)
        return req

    async def send_and_close(self, rsp: Any):
        """Not supported on a server bidi stream.

        Raises:
            NotImplementedError: Always.
        """
        raise NotImplementedError("not support 'send_and_close' in server bidi stream")


class _TaskStreamServiceHandler:
    """Dispatch table from :class:`TaskStreamMethodType` to handler methods."""

    def __init__(self, task_service: BaseTaskStreamServiceHandler) -> None:
        self.task_service = task_service
        self._handle_map = {
            TaskStreamMethodType.CLIENT: self._handle_req,
            TaskStreamMethodType.SERVER: self._handle_rsp,
            TaskStreamMethodType.BIDI: self._handle_bidi,
        }

    def get_task_stream_handler(self, name: str):
        """Return the handler for RPC name ``name``, or ``None``.

        Args:
            name: A :class:`TaskStreamMethodType` value from the frame.
        """
        return self._handle_map.get(name, None)

    async def _handle_req(self, stream: ServerStream):
        """Forward a client-streaming call to the application handler."""
        return await self.task_service.req_method_handle(stream)

    async def _handle_rsp(self, stream: ServerStream):
        """Read the single request, then forward to the rsp handler."""
        req = self.task_service.create_req_obj()
        await stream.recv_msg(req)
        return await self.task_service.rsp_method_handle(req, stream)

    async def _handle_bidi(self, stream: ServerStream):
        """Forward a bidirectional call to the application handler."""
        return await self.task_service.bidi_method_handle(stream)


class TaskStreamServiceAdapter:
    """Codec + dispatcher facade used by the task process.

    If the handler's :class:`~trpc_sdk.server.ServiceOption` has no codec or
    stream transport, the adapter installs ``INNER_TASK_STREAM`` and
    :class:`~trpc_sdk.transport.TaskServerStreamTransport`.
    """

    def __init__(self, task_service: BaseTaskStreamServiceHandler) -> None:
        self._stream_handler = _TaskStreamServiceHandler(task_service)
        self._dispatcher: StreamDispatcher = create_stream_dispatcher(is_task=True)
        self._options = task_service.options
        if self._options.codec is None:
            self._options.protocol = INNER_TASK_STREAM
            self._options.codec = get_server_codec(self._options.protocol)
        self._codec: BaseCodec = self._options.codec
        if self._options.stream_transport is None:
            # Imported here so loading this module during ``trpc_sdk.server._task``
            # init does not pull ``trpc_sdk.transport`` (and ``trpc_sdk.task``) while
            # ``trpc_sdk.stream._task_service`` is still initializing.
            from trpc_sdk.transport import TaskServerStreamTransport
            self._options.transport = TaskServerStreamTransport(is_task=True)
            self._options.stream_transport = self._options.transport
            logger.debug("create default task server stream transport")
        self._dispatcher.init(self._options)

    @property
    def service_opts(self):
        """Return the :class:`~trpc_sdk.server.ServiceOption` used by this adapter."""
        return self._options

    def decode_header(self, ctx: TrpcContext, req_buf: bytes) -> bytes:
        """Decode ``req_buf`` into ``ctx``'s message after stamping ``session_id``.

        Args:
            ctx: Must carry meta ``session_id`` (the Unix-socket session).
            req_buf: Raw bytes from the worker.

        Returns:
            The decoded body.

        Raises:
            AssertionError: ``session_id`` is missing from ``ctx``.
        """
        msg: TrpcMessage = get_message(ctx)
        session_id = ctx.get_meta_data("session_id", None)
        assert session_id is not None, "session_id is None"
        msg.set_session_id(int(session_id))
        return self._codec.decode_data(ctx, req_buf)

    async def handle(self, ctx: TrpcContext, req_buf: bytes) -> bytes:
        """Dispatch one already-decoded stream frame.

        Args:
            ctx: Context whose message has a stream frame head.
            req_buf: Decoded body.

        Raises:
            TaskStreamClientReadFrameError: The frame is not a stream frame.
        """
        msg: TrpcMessage = get_message(ctx)
        if msg.frame_head.frame_type != trpc_pb2.TRPC_STREAM_FRAME:
            raise TaskStreamClientReadFrameError(f"msg frame type {msg.frame_head.frame_type} != {trpc_pb2.TRPC_STREAM_FRAME}")
        task_handle = self._stream_handler.get_task_stream_handler(msg.server_rpc_name)
        await self._dispatcher.stream_handle_func(ctx, task_handle, req_buf)
