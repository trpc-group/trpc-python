# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Streaming server transport that maps (session, stream) to a write handle.

Inherits :class:`TrpcServerTransport`, so ``bind`` (pre-fork, options only)
and ``listen`` (post-fork, listen socket on the current loop) are the same as
unary; ``bind`` additionally marks the options ``is_stream``. The extra state is a per-stream write handle:
``stream_conns[session_id][stream_id]`` is the ``stream_writer`` the
protocol hung on the INIT frame's context, and :meth:`send` only calls its
``write``. Where the bytes go, and whether they are wrapped, is the handle's
business:

- merge mode (every thread model): the connection's
  :class:`~trpc_sdk.transport.ConnWriter`, which writes the raw frame on the
  client TCP from whichever thread the stream runs on, and drops writes once
  the connection is closed;
- ``server.mode: separate``: an :class:`IpcStreamWriter` on the rpc task's
  pipe, which wraps each frame in the inner IPC head with the client fd
  saved at INIT, so the worker can route it back to the client.

``stream_conns`` is one map per thread (one per serving loop): every frame of
a stream, and its handler, run on one loop, and a map shared by loops would
race when one empties a session entry another is adding to.
"""

from __future__ import annotations

import threading
from typing import Any
from typing import Dict

from trpc_sdk.abc import STREAM_WRITER_CTX_KEY
from trpc_sdk.abc import BaseServerTransportOption
from trpc_sdk.abc import TrpcContext
from trpc_sdk.abc import TrpcMessage
from trpc_sdk.abc import get_message
from trpc_sdk.exceptions import ServerSystemError
from trpc_sdk.log import logger
from .._cython import inner_encode_rsp
from ._trpc_transport import TrpcServerTransport


class IpcStreamWriter:
    """Stream write handle of ``server.mode: separate``: frames go back over the worker pipe.

    The client fd is captured when the stream's INIT frame arrives. The
    contexts a stream later clones for each DATA / CLOSE / FEEDBACK do not
    carry it, and the inner IPC head needs it so the worker can find the
    client connection.

    Attributes:
        transport: The rpc task's end of the worker pipe.
        fd: Client connection fd on the worker side.
        session_id: Connection id the worker assigned.
        closed: Always False: a client disconnect is handled by the worker,
            not seen here.
    """

    __slots__ = ("transport", "fd", "session_id")

    closed = False

    def __init__(self, transport: Any, fd: int, session_id: int) -> None:
        self.transport = transport
        self.fd = fd
        self.session_id = session_id

    def write(self, data: bytes, ctx: Any = None) -> None:  # pylint: disable=unused-argument
        """Wrap ``data`` in the inner IPC head and write it to the worker pipe.

        Args:
            data: Encoded stream frame.
            ctx: Unused; present so both stream write handles take the same
                arguments.
        """
        self.transport.write(inner_encode_rsp(self.fd, self.session_id, 0, data))


class TrpcServerStreamTransport(TrpcServerTransport):
    """Unary listen path plus a per-thread (session, stream) -> write-handle map."""

    def __init__(self) -> None:
        super().__init__()
        self._local = threading.local()

    def bind(self, opts: BaseServerTransportOption) -> None:
        """Record the listener options and mark them as carrying streams.

        Args:
            opts: Listen address, protocol parser, handler, and timeouts.
        """
        super().bind(opts)
        opts.is_stream = True

    @property
    def stream_conns(self) -> Dict[int, Dict[int, Any]]:
        """``session_id -> stream_id -> stream_writer`` of the calling thread; created on first use."""
        conns = getattr(self._local, "stream_conns", None)
        if conns is None:
            conns = {}
            self._local.stream_conns = conns
        return conns

    @stream_conns.setter
    def stream_conns(self, value: Dict[int, Dict[int, Any]]) -> None:
        self._local.stream_conns = value

    def _format_error(self, info: str, session_id: int, stream_id: int) -> str:
        """Build a stream-transport error string that includes ids.

        Args:
            info: What failed (duplicate, missing, closed, ...).
            session_id: Connection-lifetime id of the client TCP.
            stream_id: Stream id from the tRPC frame head.
        """
        return f"[transport stream] {info} session_id: {session_id}, stream_id: {stream_id}"

    def store_stream_conn(self, ctx: TrpcContext) -> None:
        """Remember the write handle for the stream in ``ctx``.

        The handle is taken from ``ctx`` meta ``stream_writer`` and then
        cleared so later clones do not keep a reference.

        Args:
            ctx: INIT context that already has session / stream ids on its
                message and ``stream_writer`` in metadata.

        Raises:
            ServerSystemError: A handle for this (session, stream) already
                exists.
        """
        msg: TrpcMessage = get_message(ctx)
        session_id, stream_id = msg.session_id, msg.stream_id
        session = self.stream_conns.setdefault(session_id, {})
        if stream_id in session:
            err_msg = self._format_error("failed to store stream conn error, duplicate", session_id, stream_id)
            logger.error(err_msg)
            raise ServerSystemError(err_msg)

        session[stream_id] = ctx.get_meta_data(STREAM_WRITER_CTX_KEY)
        ctx.with_meta_data(STREAM_WRITER_CTX_KEY, None)

    def delete_stream_conn(self, ctx: TrpcContext) -> None:
        """Drop the write handle for the stream in ``ctx``.

        Missing entries are logged and ignored so a double-close is safe.
        Empty session maps are deleted to avoid leaking keys.

        Args:
            ctx: Request context whose message carries session / stream ids.
        """
        msg: TrpcMessage = get_message(ctx)
        session_id, stream_id = msg.session_id, msg.stream_id
        conns = self.stream_conns
        session = conns.get(session_id)
        if session is None or stream_id not in session:
            logger.error(self._format_error("failed to delete stream conn: not found", session_id, stream_id))
        else:
            session.pop(stream_id)
        if session is not None and not session:
            del conns[session_id]

    def send(self, ctx: TrpcContext, rsp_buf: bytes, stream_id: int) -> None:
        """Write one stream frame through the stream's stored handle.

        Args:
            ctx: Request context; session / stream ids come from its message.
            rsp_buf: Encoded stream frame to write.
            stream_id: Unused; the message's stream id is authoritative.

        Raises:
            ValueError: No handle is stored for this (session, stream).
        """
        msg: TrpcMessage = get_message(ctx)
        session_id, stream_id = msg.session_id, msg.stream_id
        session = self.stream_conns.get(session_id)
        if session is None or stream_id not in session:
            err_msg = self._format_error("the stream does not exist or has been closed", session_id, stream_id)
            logger.error(err_msg)
            raise ValueError(err_msg)
        session[stream_id].write(rsp_buf)
