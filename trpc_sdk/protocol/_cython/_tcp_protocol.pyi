# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Type hints for the merge / executor TCP asyncio Protocol.

``TCPConnection`` is the Protocol bound to one accepted client socket.
``handle_request`` is the per-frame task body that runs the service
handler and writes the unary response. C-only fields on
``TCPConnection`` (idle timeout, closed / paused flags, stream-executor
mode) are not part of the Python surface.
"""

from __future__ import annotations

from typing import Any
from typing import Awaitable
from typing import Callable
from typing import Optional

from trpc_sdk.abc import TrpcContext


class TCPConnection:
    """asyncio Protocol for one accepted client TCP connection.

    ``checker`` is a factory; ``__init__`` calls ``checker.create()``
    and stores the per-connection instance. ``handle`` is the unary
    service entry (typically :meth:`trpc.server.Service.handle`).
    ``aio_connection`` stays ``None`` until ``connection_made``.
    """

    app: Any
    handle: Callable[..., Awaitable[bytes]]
    aio_connection: Optional[Any]  # asyncio.Transport, set by connection_made
    checker: Any  # per-conn protocol checker instance

    def __init__(
        self,
        app: Any,
        checker: Any,
        handle: Callable[..., Awaitable[bytes]],
        timeout: int,
    ) -> None:
        """Bind app, checker factory, unary handle, and idle timeout (ms).

        ``timeout`` is copied onto each request ``ctx.timeout``. Stream
        executor mode is read once from server config here so
        ``data_received`` does not query global config on the hot path.
        """
        ...

    def connection_made(self, transport: Any) -> None:
        """asyncio callback: store the transport as ``aio_connection``."""
        ...

    def data_received(self, data: bytes) -> None:
        """asyncio callback: feed bytes, split frames, schedule handle tasks.

        Decode errors close the connection. In stream-executor mode the
        transport is hung on ``ctx`` as ``aio_transport`` so stream
        frames can write back without going through ``handle_request``.
        """
        ...

    def connection_lost(self, exc: Optional[BaseException]) -> None:
        """asyncio callback: close the Protocol (``exc`` is unused)."""
        ...

    def eof_received(self, *args: Any) -> None:
        """asyncio callback: no-op; FIN is handled by ``connection_lost``."""
        ...

    def pause_writing(self) -> None:
        """asyncio callback: mark the write buffer as high-water."""
        ...

    def resume_writing(self) -> None:
        """asyncio callback: mark the write buffer as drained."""
        ...

    def close(self) -> None:
        """Idempotent close: shut the transport and reset the checker."""
        ...


async def handle_request(
    handle: Callable[..., Awaitable[Optional[bytes]]],
    req_buf: bytes,
    conn: Any,
    ctx: TrpcContext,
    skip_empty_write: bool = False,
) -> None:
    """Run one unary handler and write the response on ``conn``.

    ``conn`` is the asyncio ``Transport``. Never raises: handler and
    write failures are caught, counted with ``report_attr``, and
    logged. When ``skip_empty_write`` is True (executor stream mode)
    and ``handle`` returns a false value, this returns without
    ``conn.write``; stream frames are sent later by
    ``TrpcServerStreamTransport.send``.
    """
    ...
