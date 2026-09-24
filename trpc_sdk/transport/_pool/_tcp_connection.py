# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Open a TCP or Unix stream and wrap it as :class:`Connection`.

:func:`open_connection` and :func:`open_unix_connection` are thin
wrappers over the stdlib helpers of the same name. They apply the TCP
socket options the pool needs and pair the ``(reader, writer)`` tuple
into a single :class:`Connection`, which is what
:class:`~trpc_sdk.transport._pool.PooledConnection` reads and writes
through. Callers in :mod:`trpc_sdk.transport._pool._dial` use these
helpers instead of ``loop.create_connection`` directly.
"""

from __future__ import annotations

import asyncio
import socket
from asyncio import BaseEventLoop
from asyncio import StreamReader
from asyncio import StreamWriter
from typing import Optional

from trpc_sdk.common import TransportSocket
from trpc_sdk.common import get_or_create_event_loop
from trpc_sdk.utils import is_valid_socket
from trpc_sdk.utils import set_no_close_wait
from trpc_sdk.utils import set_tcp_keep_alive
from trpc_sdk.utils import set_tcp_no_delay

from ._constants import DEFAULT_STREAM_LIMIT


async def open_connection(host: str = None,
                          port: int = None,
                          *,
                          loop: BaseEventLoop = None,
                          limit: int = DEFAULT_STREAM_LIMIT,
                          **kwds) -> "Connection":
    """Dial a TCP stream and return it as a :class:`Connection`.

    Every keyword the stdlib ``asyncio.open_connection`` forwards to
    ``loop.create_connection`` works here too, notably ``sock``,
    ``family`` and ``local_addr``.

    Args:
        host: Remote host, or ``None`` when ``sock`` is given.
        port: Remote port, or ``None`` when ``sock`` is given.
        loop: Loop recorded on the connection. The dial itself always
            runs on the loop awaiting this coroutine.
        limit: Read buffer half-limit for the stream reader.
        **kwds: Forwarded to ``loop.create_connection``.

    Returns:
        A connected :class:`Connection`.
    """
    reader, writer = await asyncio.open_connection(host, port, limit=limit, **kwds)
    return _wrap(reader, writer, loop)


async def open_unix_connection(host: str = None,
                               *,
                               loop: BaseEventLoop = None,
                               limit: int = DEFAULT_STREAM_LIMIT,
                               **kwds) -> "Connection":
    """Dial a Unix stream and return it as a :class:`Connection`.

    Args:
        host: Socket path, or ``None`` when ``sock`` is given.
        loop: Loop recorded on the connection. The dial itself always
            runs on the loop awaiting this coroutine.
        limit: Read buffer half-limit for the stream reader.
        **kwds: Forwarded to ``loop.create_unix_connection``.

    Returns:
        A connected :class:`Connection`.
    """
    reader, writer = await asyncio.open_unix_connection(host, limit=limit, **kwds)
    return _wrap(reader, writer, loop)


def _wrap(reader: StreamReader, writer: StreamWriter, loop: Optional[BaseEventLoop]) -> "Connection":
    """Apply the pool's TCP socket options and pair the streams.

    Args:
        reader: Reader half returned by the stdlib helper.
        writer: Writer half returned by the stdlib helper.
        loop: Loop to record, or ``None`` to pick the current one.

    Returns:
        A :class:`Connection` over ``reader`` and ``writer``.
    """
    sock: TransportSocket = writer.get_extra_info("socket")
    if is_valid_socket(sock) and sock.proto == socket.IPPROTO_TCP:
        set_no_close_wait(sock)
        set_tcp_keep_alive(sock)
        set_tcp_no_delay(sock)
    return Connection(reader, writer, loop or get_or_create_event_loop())


class Connection:
    """A dialed stream, presented as one read/write object.

    The pool needs both halves of an asyncio stream on a single object:
    ``PooledConnection`` writes frames and reads them back, while the
    reuse check needs reader state (``exception``, ``at_eof``, leftover
    buffer) and writer state (``is_closing``, the socket).

    Args:
        reader: Reader half of the dialed stream.
        writer: Writer half of the dialed stream.
        loop: Event loop this connection belongs to.
    """

    def __init__(self, reader: StreamReader, writer: StreamWriter, loop: BaseEventLoop):
        self._reader = reader
        self._writer = writer
        self._loop = loop

    def __repr__(self):
        return f"<Connection transport={self._writer.transport} reader={self._reader}>"

    @property
    def loop(self) -> BaseEventLoop:
        """Event loop this connection was dialed on."""
        return self._loop

    @property
    def transport(self):
        """Underlying asyncio transport."""
        return self._writer.transport

    @property
    def sock(self) -> TransportSocket:
        """Socket behind the transport."""
        return self._writer.get_extra_info("socket")

    def get_extra_info(self, name: str, default=None):
        """Get optional transport information.

        Args:
            name: Transport info key, e.g. ``peername``.
            default: Returned when the key is absent.

        Returns:
            The transport value, or ``default``.
        """
        return self._writer.get_extra_info(name, default)

    # writer
    def write(self, data: bytes):
        """Write ``data`` to the transport."""
        self._writer.write(data)

    def write_eof(self):
        """Half-close the write side."""
        return self._writer.write_eof()

    def can_write_eof(self) -> bool:
        """Return True if the transport supports ``write_eof``."""
        return self._writer.can_write_eof()

    def close(self):
        """Close the transport."""
        return self._writer.close()

    def is_closing(self) -> bool:
        """Return True if the transport is closing or closed."""
        return self._writer.is_closing()

    async def wait_closed(self):
        """Wait until the transport is fully closed."""
        await self._writer.wait_closed()

    async def drain(self):
        """Flush the write buffer, honoring flow control."""
        await self._writer.drain()

    # reader
    def exception(self):
        """Exception recorded on the read side, or ``None``."""
        return self._reader.exception()

    def at_eof(self) -> bool:
        """Return True once the peer sent EOF and the buffer is drained."""
        return self._reader.at_eof()

    def buffer_size(self) -> int:
        """Bytes sitting unread in the reader buffer.

        ``StreamReader`` has no public accessor for this, so the
        private buffer is read directly. A missing attribute degrades
        to "nothing buffered" rather than breaking the idle check in
        :meth:`~trpc_sdk.transport._pool.PooledConnection.check_conn_buff`.
        """
        return len(getattr(self._reader, "_buffer", b""))

    async def readexactly(self, num: int) -> bytes:
        """Read exactly ``num`` bytes.

        Args:
            num: Exact byte count to wait for.

        Returns:
            ``num`` bytes.

        Raises:
            IncompleteReadError: EOF arrived before ``num`` bytes.
        """
        return await self._reader.readexactly(num)
