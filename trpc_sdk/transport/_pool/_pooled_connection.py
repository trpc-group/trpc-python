# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Pooled wrapper around a dialed asyncio stream.

Callers receive a :class:`PooledConnection` from
:func:`~trpc_sdk.transport._pool.get_pool_connection`. ``close()``
returns it to the pool; ``force_close()`` tears down the socket.
``read`` / ``write`` failures call ``pool.put(self, True)`` before
re-raising so a broken conn is not reused.
"""

from __future__ import annotations

import time
from asyncio import BaseEventLoop
from asyncio import StreamReader
from asyncio import StreamWriter

from trpc_sdk.abc import AddrType
from trpc_sdk.common import TransportSocket
from trpc_sdk.exceptions import ClientPoolConnClosedError
from trpc_sdk.log import logger
from trpc_sdk.telemetry import report_attr
from trpc_sdk.utils import close_socket
from trpc_sdk.utils import is_close_wait
from trpc_sdk.utils import is_valid_socket

from ._base import BasePool


class PooledConnection:
    """One TCP (or Unix) connection borrowed from a client pool.

    ``begin_time`` is stamped when the conn is put back idle; the pool
    uses it for ``idle_timeout``. ``created_time`` is set once at wrap
    and used for ``max_conn_lifetime``.

    Args:
        reader: Reader half from
            :func:`~trpc_sdk.transport._pool._dial.dial_connection`.
        writer: Writer half of the same stream.
        loop: Event loop the stream was dialed on.
        pool: Pool (or short-conn factory) that will take this conn
            back on ``close`` / I/O error.
        address: Remote this conn was dialed to, used in close logs.
        is_short: True when this conn is not pooled and must be
            force-closed on return.
    """

    def __init__(
            self,
            reader: StreamReader,
            writer: StreamWriter,
            loop: BaseEventLoop,
            pool: BasePool,
            address: AddrType,
            is_short: bool):
        self._reader = reader
        self._writer = writer
        self._created_time = time.time()
        self._pool = pool
        self._closed = False
        self._address = address
        self._loop = loop
        self.is_short = is_short
        self.begin_time = 0

    @property
    def created_time(self) -> float:
        """Unix timestamp when this wrapper was created."""
        return self._created_time

    @property
    def transport(self):
        """Underlying asyncio transport."""
        return self._writer.transport

    @property
    def _sock(self) -> TransportSocket:
        """Socket behind the transport."""
        return self._writer.get_extra_info("socket")

    def check_remote_error(self) -> bool:
        """Return True when this conn must not be reused.

        CLOSE_WAIT is treated as dead. An exception, a closing
        transport, peer EOF, or leftover read-buffer bytes also fail
        the check: an idle conn should not have readable data; leftover
        bytes mean the caller did not finish a sticky-packet frame.

        Returns:
            True if the pool should discard this connection.
        """
        sock = self._sock
        if is_valid_socket(sock) and is_close_wait(sock):
            logger.warning(f"conn is close wait, need close force {sock}")
            return True

        ret = self._reader.exception() or self._writer.is_closing() or self._reader.at_eof() or self.check_conn_buff()
        if ret:
            logger.error(f"remote conn err, at_of: {self._reader.at_eof()} ,is_closing: {self._writer.is_closing()},\
                        exception: {self._reader.exception()} is_short: {self.is_short}")
        return ret

    def check_conn_buff(self) -> bool:
        """Return True if the transport still has unread bytes.

        Idle reuse requires an empty buffer; leftover data is treated
        as a protocol error by :meth:`check_remote_error`.

        ``StreamReader`` has no public accessor for buffered bytes, so the
        private ``_buffer`` is read directly. A missing attribute degrades
        to "nothing buffered" rather than breaking the idle check.

        Returns:
            True when the reader buffer is not empty.
        """
        return len(getattr(self._reader, "_buffer", b"")) > 0

    async def read(self, num: int) -> bytes:
        """Read exactly ``num`` bytes from the underlying stream.

        On any exception the conn is returned to the pool with
        ``force_close=True`` so it cannot be reused, then the error is
        re-raised.

        Args:
            num: Exact byte count to wait for.

        Returns:
            ``num`` bytes.

        Raises:
            ClientPoolConnClosedError: This wrapper was already closed.
            Exception: Propagated from ``readexactly`` after the conn
                is discarded.
        """
        if self._closed:
            raise ClientPoolConnClosedError("ErrConnClosed")
        try:
            return await self._reader.readexactly(num)
        except Exception as err:  # pylint: disable=broad-except
            report_attr("trpc_client_reading_exception", 1)
            self._pool.put(self, True)
            raise err

    def write(self, data: bytes) -> int:
        """Write ``data`` on the underlying stream.

        On any exception the conn is returned with ``force_close=True``
        before the error is re-raised.

        Args:
            data: Bytes to send.

        Returns:
            Value returned by the underlying ``write``.

        Raises:
            ClientPoolConnClosedError: This wrapper was already closed.
            Exception: Propagated from ``write`` after the conn is
                discarded.
        """
        if self._closed:
            raise ClientPoolConnClosedError("ErrConnClosed")

        try:
            return self._writer.write(data)
        except Exception as err:  # pylint: disable=broad-except
            self._pool.put(self, True)
            raise err

    def force_close(self) -> None:
        """Tear down the transport and socket; do not return to the pool.

        Prefer :meth:`close` with ``force=True``, which goes through
        ``pool.put``. This method is what the pool calls after it has
        already dropped the conn from idle / active sets.

        Args:
            None.
        """
        self._closed = True

        try:
            if self._loop.is_running():
                self._writer.close()
        except Exception as err:  # pylint: disable=broad-except
            logger.error(f"address {self._address}, conn.close failed: {err} in `force_close`")

        sock = self._sock
        if is_valid_socket(sock):
            close_socket(sock)

    def close(self, force: bool = False) -> None:
        """Return this conn to the pool, or discard it.

        If the wrapper is already marked closed, :meth:`force_close` is
        called again. Otherwise ``pool.put(self, force)`` decides
        whether to cache it as idle.

        Args:
            force: If True, the pool closes the socket instead of
                queuing the conn.
        """
        if self._closed:
            self.force_close()
            return
        self._pool.put(self, force)

    def __del__(self):
        """Close the OS socket if the wrapper is collected still open."""
        sock = self._sock
        if is_valid_socket(sock):
            close_socket(sock)
