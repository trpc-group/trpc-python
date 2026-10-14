# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Type hints for the merge / executor TCP asyncio Protocol.

``TCPConnection`` is the Protocol bound to one accepted client socket.
``handle_request`` is the per-frame task body that runs the service
handler and writes the response. C-only fields on ``TCPConnection``
(idle timeout, closed / paused flags) are not part of the Python surface.
"""

from __future__ import annotations

from typing import TYPE_CHECKING
from typing import Any
from typing import Awaitable
from typing import Callable
from typing import Optional
from typing import Set

from trpc_sdk.abc import TrpcContext

if TYPE_CHECKING:
    from trpc_sdk.transport import ConnectionOption


def next_session_id() -> int:
    """Return the next connection id: 31 bits, never 0, unique in the process."""
    ...


class TCPConnection:
    """asyncio Protocol for one accepted client TCP connection.

    Built from a :class:`~trpc_sdk.transport.ConnectionOption`:
    ``__init__`` calls ``option.parser.create()`` and stores the
    per-connection instance as ``parser``; ``handle`` is
    ``option.rpc_handler``, the service entry (typically
    :meth:`trpc_sdk.server.ServiceAdapter.handle`).
    ``aio_connection`` stays ``None`` until ``connection_made``.
    """

    handle: Callable[..., Awaitable[Optional[bytes]]]
    aio_connection: Optional[Any]  # asyncio.Transport, set by connection_made
    parser: Any  # per-conn protocol parser instance
    shared_pool: Optional[Any]  # SharedHandlerPool under io_handle_type: separate
    # ConnWriter: set by connection_made under shared_pool, else by the first stream frame
    writer: Optional[Any]
    session_id: int  # read-only; process-unique and non-zero once connection_made ran
    has_streams: bool  # read-only; set by the first stream frame
    stream_dispatcher: Optional[Any]  # read-only; StreamDispatcher of the service
    stream_loops: Optional[Set[int]]  # read-only; handle threads streams were pinned to

    def __init__(self, option: ConnectionOption) -> None:
        """Copy the listener's options into this connection's slots.

        ``option.timeout`` (ms) is copied onto each request ``ctx.timeout``.
        ``option.shared_pool`` hands frames to handle threads instead of
        running them on this loop.
        """
        ...

    def connection_made(self, transport: Any) -> None:
        """asyncio callback: store the transport and take a ``session_id``."""
        ...

    def data_received(self, data: bytes) -> None:
        """asyncio callback: feed bytes, split frames, schedule handle tasks.

        Decode errors close the connection. Stream frames carry the
        connection's ``ConnWriter`` on ``ctx`` as ``stream_writer``.
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
        """Idempotent close: end the streams, shut the transport, reset the parser."""
        ...


async def handle_request(
    handle: Callable[..., Awaitable[Optional[bytes]]],
    req_buf: bytes,
    conn: Any,
    ctx: TrpcContext,
) -> None:
    """Run one handler and write the response on ``conn``.

    ``conn`` is the asyncio ``Transport``. Never raises: handler and
    write failures are caught, counted with ``report_attr``, and
    logged. A false return value (a stream frame, which answers through
    its ``stream_writer``) writes nothing.
    """
    ...


async def handle_request_via_writer(
    handle: Callable[..., Awaitable[Optional[bytes]]],
    req_buf: bytes,
    writer: Any,
    ctx: TrpcContext,
) -> None:
    """Run one handler on a handle thread and answer through ``writer``.

    ``writer`` is the connection's ``ConnWriter``; the write itself happens
    on the I/O loop that owns the transport. Never raises.
    """
    ...
