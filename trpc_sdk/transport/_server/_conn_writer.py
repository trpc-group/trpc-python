# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Write-back handle that answers on a connection's transport from any thread.

An asyncio transport may only be touched from the loop that created it.
:class:`ConnWriter` wraps one transport with that loop: a write from the
owning thread goes straight to the transport, a write from any other thread
is posted with ``call_soon_threadsafe``.

Two users:

* Under ``io_handle_type: separate`` a request is read on an I/O loop and
  handled on a handle thread. The I/O loop creates one writer per TCP
  connection, and one per UDP request (bound to the datagram's source
  address), and the handle thread answers through it.
* Stream frames answer through the connection's writer in every merge thread
  model, even when the handler runs on the owning loop: a stream handler can
  keep writing (DATA, CLOSE, RESET) after the client left, and the writer's
  ``closed`` flag is what drops those writes.

It lives beside the TCP protocol because that protocol builds the writer
itself. The Cython module imports it with a relative import and does not
import :mod:`trpc_sdk.transport`: that package's ``__init__`` loads this
module.

Every cross-thread write costs one ``call_soon_threadsafe``; wake-ups are not
coalesced in this version.
"""

from __future__ import annotations

import asyncio
import threading
from typing import Any
from typing import Optional

from trpc_sdk.log import logger
from trpc_sdk.rpcz import RPCZ_SERVER_RSP_SIZE_CTX_KEY
from trpc_sdk.rpcz import submit_server_span_on_executor_post_io
from trpc_sdk.telemetry import report_attr
from trpc_sdk.utils import now_us


class ConnWriter:
    """Writes responses on the loop that owns one transport, from any thread.

    ``closed`` is only written by the owning loop (from ``connection_lost``).
    Other threads may read it (a stream INIT checks it before creating the
    stream); a bool read under the GIL is never torn, so it needs no lock.

    Attributes:
        loop: Loop that owns ``transport``.
        transport: Stream transport, or the datagram transport of a UDP
            endpoint.
        addr: Datagram destination; ``None`` for a stream transport.
        closed: Set once the connection is gone; later writes are dropped.
    """

    __slots__ = ("loop", "transport", "addr", "closed", "_owner_ident")

    def __init__(self,
                 loop: asyncio.AbstractEventLoop,
                 transport: asyncio.BaseTransport,
                 addr: Optional[Any] = None) -> None:
        """Bind the writer to the calling thread's loop and one transport.

        Must be constructed on the owning loop's thread.

        Args:
            loop: Loop that owns ``transport``.
            transport: Transport responses are written to.
            addr: Peer address for ``sendto``; ``None`` for TCP.
        """
        self.loop = loop
        self.transport = transport
        self.addr = addr
        self.closed = False
        self._owner_ident = threading.get_ident()

    def write(self, data: bytes, ctx: Optional[Any] = None) -> None:
        """Write ``data`` on the owning loop; callable from any thread, never raises.

        Args:
            data: Encoded response frame.
            ctx: Request context, for the rpcz write-side timestamps; ``None``
                skips rpcz. Not touched by the caller afterwards.
        """
        if threading.get_ident() == self._owner_ident:
            self._write_on_owner(data, ctx)
            return
        try:
            self.loop.call_soon_threadsafe(self._write_on_owner, data, ctx)
        except RuntimeError:
            # The I/O loop is already closed: the process is shutting down.
            report_attr("trpc_conn_writer_drop", 1)

    def close(self) -> None:
        """Mark the connection gone; called from ``connection_lost`` on the owning loop."""
        self.closed = True

    def _write_on_owner(self, data: bytes, ctx: Optional[Any]) -> None:
        """Write on the owning loop, unless the connection went away meanwhile.

        Args:
            data: Encoded response frame.
            ctx: Request context, or ``None``.
        """
        if self.closed or self.transport.is_closing():
            # The client left before its answer was ready; nothing to report.
            report_attr("trpc_conn_writer_drop", 1)
            return
        try:
            if ctx is not None:
                ctx.rpcz_data.server_pre_io_send_msg_us = now_us()
                ctx.with_meta_data(RPCZ_SERVER_RSP_SIZE_CTX_KEY, len(data))
            if self.addr is None:
                self.transport.write(data)
            else:
                self.transport.sendto(data, self.addr)
            if ctx is not None:
                ctx.rpcz_data.server_post_io_send_msg_us = now_us()
                submit_server_span_on_executor_post_io(ctx)
        except Exception as err:  # pylint: disable=broad-except
            report_attr("trpc_conn_writer_err", 1)
            logger.error("[conn writer] failed to write response: %s", repr(err), exc_info=True)
