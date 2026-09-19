# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Streaming server transport that maps (session, stream) to a write path.

Inherits :class:`TrpcServerTransport` so listen / process-model registration
is the same as unary. The extra state is a per-stream write handle:

- ``stream_conns[session_id][stream_id]`` is the asyncio Transport used to
  write a response. In dispatcher+worker mode that transport is the worker
  IPC pipe; in executor / task-stream mode it is the client TCP itself.
- ``stream_client_fds`` stores the client fd when executor-mode streaming is
  on, because ``handle_init`` clones a context that no longer carries ``fd``.
  ``inner_encode_rsp`` needs that fd to tag the IPC frame so the dispatcher
  can look the client up.

``TaskServerStreamTransport`` is the task-process variant: it never listens
and always writes the raw buffer, because the peer is already a worker or
task client rather than a dispatcher pipe.
"""

from __future__ import annotations

from collections import defaultdict
from typing import Dict

from trpc_sdk.abc import BaseServerTransportOption
from trpc_sdk.abc import BaseTrpcServerEngine
from trpc_sdk.abc import TrpcContext
from trpc_sdk.abc import TrpcMessage
from trpc_sdk.abc import get_message
from trpc_sdk.config import TrpcConfig
from trpc_sdk.exceptions import ServerSystemError
from trpc_sdk.log import logger
from ._cython import inner_encode_rsp
from ._transport import TrpcServerTransport


class TrpcServerStreamTransport(TrpcServerTransport):
    """Unary listen path plus a (session, stream) -> write-handle map."""

    def __init__(self, is_task: bool = False) -> None:
        super().__init__()
        # session_id -> stream_id -> asyncio Transport used to write rsp.
        self.stream_conns: Dict[int, Dict[int, object]] = defaultdict(dict)
        # session_id -> stream_id -> client fd. Only filled when executor-mode
        # streaming is on; the cloned handle_init context does not carry fd.
        self.stream_client_fds: Dict[int, Dict[int, object]] = defaultdict(dict)
        self._is_task = is_task
        # Off: historical dispatcher+worker-only path; send always wraps the
        # buffer with inner_encode_rsp. On: also persist fd, and fall back to
        # writing the raw buffer when there is no fd (executor holds TCP).
        self._enable_executor_mode = bool(
            getattr(TrpcConfig.server_config(), "enable_stream_executor_mode", False))

    def _format_error(self, info: str, session_id: int, stream_id: int) -> str:
        """Build a stream-transport error string that includes ids.

        Args:
            info: What failed (duplicate, missing, closed, ...).
            session_id: Connection-lifetime id of the client TCP.
            stream_id: Stream id from the tRPC frame head.
        """
        return (f"[transport stream] {info} session_id: {session_id}, "
                f"stream_id: {stream_id} is_task: {self._is_task}")

    def store_stream_conn(self, ctx: TrpcContext) -> None:
        """Remember the write handle for the stream in ``ctx``.

        The transport is taken from ``ctx`` meta ``aio_transport`` and then
        cleared so later clones do not keep a stale reference.

        Args:
            ctx: Request context that already has session / stream ids on
                its message and ``aio_transport`` (and optionally ``fd``)
                in metadata.

        Raises:
            ServerSystemError: A handle for this (session, stream) already
                exists.
        """
        msg: TrpcMessage = get_message(ctx)
        session_id, stream_id = msg.session_id, msg.stream_id
        if stream_id in self.stream_conns[session_id]:
            err_msg = self._format_error("failed to store stream conn error, duplicate", session_id, stream_id)
            logger.error(err_msg)
            raise ServerSystemError(err_msg)

        aio_transport = ctx.get_meta_data("aio_transport")
        self.stream_conns[session_id][stream_id] = aio_transport

        if self._enable_executor_mode:
            self.stream_client_fds[session_id][stream_id] = ctx.get_meta_data("fd")

        ctx.with_meta_data("aio_transport", None)

    def delete_stream_conn(self, ctx: TrpcContext) -> None:
        """Drop the write handle for the stream in ``ctx``.

        Missing entries are logged and ignored so a double-close is safe.
        Empty session maps are deleted to avoid leaking keys.

        Args:
            ctx: Request context whose message carries session / stream ids.
        """
        msg: TrpcMessage = get_message(ctx)
        session_id, stream_id = msg.session_id, msg.stream_id
        if stream_id not in self.stream_conns[session_id]:
            logger.error(self._format_error("failed to delete stream conn: not found", session_id, stream_id))
        else:
            self.stream_conns[session_id].pop(stream_id)
        if self._enable_executor_mode:
            if stream_id in self.stream_client_fds.get(session_id, {}):
                self.stream_client_fds[session_id].pop(stream_id)
        if len(self.stream_conns[session_id]) == 0:
            del self.stream_conns[session_id]
        if self._enable_executor_mode:
            if session_id in self.stream_client_fds and len(self.stream_client_fds[session_id]) == 0:
                del self.stream_client_fds[session_id]

    def send(self, ctx: TrpcContext, rsp_buf: bytes, stream_id: int) -> None:
        """Write one stream frame to the stored handle.

        Dispatcher+worker (or executor-mode with a saved fd) wraps ``rsp_buf``
        in ``inner_encode_rsp`` so the dispatcher can look up the client.
        Executor / multi-listener with no fd writes the raw buffer onto the
        client TCP the executor already holds.

        Args:
            ctx: Request context; session / stream ids come from its message.
            rsp_buf: Encoded stream frame to write.
            stream_id: Unused; the message's stream id is authoritative.

        Raises:
            ValueError: No handle is stored for this (session, stream).
        """
        msg: TrpcMessage = get_message(ctx)
        session_id, stream_id = msg.session_id, msg.stream_id
        if stream_id not in self.stream_conns[session_id]:
            err_msg = self._format_error("the stream does not exist or has been closed", session_id, stream_id)
            logger.error(err_msg)
            raise ValueError(err_msg)

        if not self._enable_executor_mode:
            rsp_body_buf = inner_encode_rsp(ctx.get_meta_data("fd"), session_id, 0, rsp_buf)
            self.stream_conns[session_id][stream_id].write(rsp_body_buf)
            return

        fd = self.stream_client_fds.get(session_id, {}).get(stream_id)
        if fd is None:
            fd = ctx.get_meta_data("fd")

        if fd is not None:
            rsp_body_buf = inner_encode_rsp(fd, session_id, 0, rsp_buf)
            self.stream_conns[session_id][stream_id].write(rsp_body_buf)
        else:
            self.stream_conns[session_id][stream_id].write(rsp_buf)


class TaskServerStreamTransport(TrpcServerStreamTransport):
    """Stream write path used inside a task process.

    A task process never listens and never talks to a dispatcher, so the
    listen / close methods raise. ``send`` writes the raw buffer to the
    stored transport (a socketpair end back to the worker or task client).
    """

    def listen_and_serve(self, opts: BaseServerTransportOption, server_engine: BaseTrpcServerEngine) -> None:
        """Task processes do not listen; raise instead of forking children."""
        raise NotImplementedError("TaskServerStreamTransport not support 'listen_and_serve'")

    def close(self) -> None:
        """Task processes do not own the engine lifecycle."""
        raise NotImplementedError("TaskServerStreamTransport not support 'close'")

    def send(self, ctx: TrpcContext, rsp_buf: bytes, stream_id: int) -> None:
        """Write a raw stream frame to the stored task-side transport.

        Args:
            ctx: Request context; session / stream ids come from its message.
            rsp_buf: Encoded stream frame to write.
            stream_id: Unused; the message's stream id is authoritative.

        Raises:
            ValueError: No handle is stored for this (session, stream).
        """
        msg: TrpcMessage = get_message(ctx)
        session_id, stream_id = msg.session_id, msg.stream_id
        if stream_id not in self.stream_conns[session_id]:
            err_msg = self._format_error("task stream does not exist or has been closed", session_id, stream_id)
            raise ValueError(err_msg)

        self.stream_conns[session_id][stream_id].write(rsp_buf)
