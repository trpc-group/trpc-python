# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Custom asyncio stream reader / writer used by client TCP connections.

This is a trimmed copy of :mod:`asyncio.streams`. Customizations:

- :meth:`StreamReaderProtocol.connection_made` applies TCP keepalive,
  nodelay, and no-close-wait on a valid TCP socket.
- :class:`StreamReader` only implements :meth:`StreamReader.readexactly`;
  the pool never needs ``readline`` / ``readuntil``.

:class:`FlowControlMixin` is the :meth:`StreamWriter.drain` waiter.
:class:`~trpc_sdk.pool.Connection` subclasses :class:`StreamWriter`
and delegates reads to :class:`StreamReader`.
"""

from __future__ import annotations

import socket
from asyncio import BaseEventLoop
from asyncio import Future
from asyncio import IncompleteReadError
from asyncio import coroutines
from asyncio import protocols
from asyncio.transports import ReadTransport
from asyncio.transports import Transport
from asyncio.transports import WriteTransport
from typing import Any
from typing import Callable

from trpc_sdk.common import TransportSocket
from trpc_sdk.common import get_or_create_event_loop
from trpc_sdk.common import sleep_by_loop
from trpc_sdk.log import logger
from trpc_sdk.utils import is_valid_socket
from trpc_sdk.utils import set_no_close_wait
from trpc_sdk.utils import set_tcp_keep_alive
from trpc_sdk.utils import set_tcp_no_delay
from ._constants import DEFAULT_STREAM_LIMIT


class FlowControlMixin(protocols.Protocol):
    """Reusable flow control logic for StreamWriter.drain().

    This implements the protocol methods pause_writing(),
    resume_writing() and connection_lost().  If the subclass overrides
    these it must call the super methods.

    StreamWriter.drain() must wait for _drain_helper() coroutine.
    """

    def __init__(self, loop=None):
        self._loop = loop or get_or_create_event_loop()
        self._paused = False
        self._drain_waiter = None
        self._connection_lost = False

    def pause_writing(self):
        """Pause writing"""
        assert not self._paused
        self._paused = True
        if self._loop.get_debug():
            logger.debug("%r pauses writing", self)

    def resume_writing(self):
        """Resume writing"""
        assert self._paused
        self._paused = False
        if self._loop.get_debug():
            logger.debug("%r resumes writing", self)

        waiter = self._drain_waiter
        if waiter is not None:
            self._drain_waiter = None
            if not waiter.done():
                waiter.set_result(None)

    def connection_lost(self, exc):
        """Connection lost"""
        self._connection_lost = True
        # Wake up the writer if currently paused.
        if not self._paused:
            return
        waiter = self._drain_waiter
        if waiter is None:
            return
        self._drain_waiter = None
        if waiter.done():
            return
        if exc is None:
            waiter.set_result(None)
        else:
            waiter.set_exception(exc)

    async def _drain_helper(self):
        if self._connection_lost:
            raise ConnectionResetError('Connection lost')
        if not self._paused:
            return
        waiter = self._drain_waiter
        assert waiter is None or waiter.cancelled()
        waiter = self._loop.create_future()
        self._drain_waiter = waiter
        await waiter


class StreamReaderProtocol(FlowControlMixin):
    """Helper class to adapt between Protocol and StreamReader.

    (This is a helper class instead of making StreamReader itself a
    Protocol subclass, because the StreamReader has other potential
    uses, and to prevent the user of the StreamReader to accidentally
    call inappropriate methods of the protocol.)
    """

    def __init__(self,
                 stream_reader: 'StreamReader',
                 client_connected_cb: Callable[['StreamReader', 'StreamWriter'], Any] = None,
                 loop: BaseEventLoop = None):
        super().__init__(loop=loop)
        self._stream_reader = stream_reader
        self._stream_writer = None
        self._client_connected_cb = client_connected_cb
        self._over_ssl = False
        self._closed = self._loop.create_future()

    @property
    def stream_reader(self):
        """Get reader of stream"""
        return self._stream_reader

    async def drain_helper(self):
        """Drain  helper"""
        await self._drain_helper()

    @property
    def close_future(self) -> Future:
        """Get close future"""
        return self._closed

    def connection_made(self, transport: Transport):
        """Make connection"""
        self._stream_reader.set_transport(transport)
        self._over_ssl = transport.get_extra_info('sslcontext') is not None
        sock: TransportSocket = transport.get_extra_info('socket')
        if is_valid_socket(sock) and sock.proto == socket.IPPROTO_TCP:
            set_no_close_wait(sock)
            set_tcp_keep_alive(sock)
            set_tcp_no_delay(sock)
        if self._client_connected_cb is not None:
            self._stream_writer = StreamWriter(transport, self, self._stream_reader, self._loop)
            res = self._client_connected_cb(self._stream_reader, self._stream_writer)
            if coroutines.iscoroutine(res):
                self._loop.create_task(res)

    def connection_lost(self, exc):
        if self._stream_reader is not None:
            if exc is None:
                self._stream_reader.feed_eof()
            else:
                self._stream_reader.set_exception(exc)
        if not self._closed.done():
            if exc is None:
                self._closed.set_result(None)
            else:
                self._closed.set_exception(exc)
        super().connection_lost(exc)
        self._stream_reader = None
        self._stream_writer = None

    def data_received(self, data: bytes):
        """Receive data"""
        self._stream_reader.feed_data(data)

    def eof_received(self):
        """End of receive"""
        self._stream_reader.feed_eof()
        if self._over_ssl:
            # Prevent a warning in SSLProtocol.eof_received:
            # "returning true from eof_received()
            # has no effect when using ssl"
            return False
        return True

    def __del__(self):
        # Prevent reports about unhandled exceptions.
        # Better than self._closed._log_traceback = False hack
        closed = self._closed
        if closed.done() and not closed.cancelled():
            closed.exception()


class StreamWriter:
    """Wraps a Transport.

    This exposes write(), writelines(), [can_]write_eof(),
    get_extra_info() and close().  It adds drain() which returns an
    optional Future on which you can wait for flow control.  It also
    adds a transport property which references the Transport
    directly.
    """

    def __init__(self, transport: WriteTransport, protocol: StreamReaderProtocol, reader: 'StreamReader',
                 loop: BaseEventLoop):
        self._transport = transport
        self._protocol = protocol
        # drain() expects that the reader has an exception() method
        assert reader is None or isinstance(reader, StreamReader)
        self._reader = reader
        self._loop = loop

    def __repr__(self):
        info = [self.__class__.__name__, f'transport={self._transport}']
        if self._reader is not None:
            info.append(f'reader={self._reader}')
        return f"<{' '.join(info)}>"

    @property
    def loop(self) -> BaseEventLoop:
        """Get transport"""
        return self._loop

    @property
    def transport(self):
        """Get transport"""
        return self._transport

    @property
    def sock(self) -> TransportSocket:
        """Get socket of transport"""
        return self._transport.get_extra_info('socket')

    def write(self, data: bytes):
        """Write data"""
        self._transport.write(data)

    def write_eof(self):
        """Write eof"""
        return self._transport.write_eof()

    def can_write_eof(self):
        """Return True if this transport supports write_eof(), False if not."""
        return self._transport.can_write_eof()

    def close(self):
        """Close the transport."""
        return self._transport.close()

    def is_closing(self):
        """Return True if the transport is closing or closed."""
        return self._transport.is_closing()

    async def wait_closed(self):
        """Wait closing"""
        await self._protocol.close_future

    def get_extra_info(self, name: str, default=None):
        """Get optional transport information."""
        return self._transport.get_extra_info(name, default)

    async def drain(self):
        """Flush the write buffer.

        The intended use is to write

          w.write(data)
          await w.drain()
        """
        if self._reader is not None:
            exc = self._reader.exception()
            if exc is not None:
                raise exc
        if self._transport.is_closing():
            # Yield to the event loop so connection_lost() may be
            # called.  Without this, _drain_helper() would return
            # immediately, and code that calls
            #     write(...); await drain()
            # in a loop would never call connection_lost(), so it
            # would not see an error when the socket is closed.
            await sleep_by_loop(0, loop=self._loop)
        await self._protocol.drain_helper()


class StreamReader:
    """Stream reader"""

    def __init__(self, limit: int = DEFAULT_STREAM_LIMIT, loop: BaseEventLoop = None):
        # The line length limit is  a security feature;
        # it also doubles as half the buffer limit.

        if limit <= 0:
            raise ValueError('Limit cannot be <= 0')

        self._limit = limit
        self._loop = loop or get_or_create_event_loop()
        self._buffer = bytearray()
        self._eof = False  # Whether we're done.
        self._waiter = None  # A future used by _wait_for_data()
        self._exception = None
        self._transport = None
        self._paused = False

    @property
    def buffer(self) -> bytearray:
        """Get buffer"""
        return self._buffer

    def __repr__(self):
        info = ['StreamReader']
        if self._buffer:
            info.append(f'{len(self._buffer)} bytes')
        if self._eof:
            info.append('eof')
        if self._limit != DEFAULT_STREAM_LIMIT:
            info.append(f'limit={self._limit}')
        if self._waiter:
            info.append(f'waiter={self._waiter}')
        if self._exception:
            info.append(f'exception={self._exception}')
        if self._transport:
            info.append(f'transport={self._transport}')
        if self._paused:
            info.append('paused')
        return f"<{' '.join(info)}>"

    def exception(self):
        """Get exception"""
        return self._exception

    def set_exception(self, exc):
        """Set exception"""
        self._exception = exc

        waiter = self._waiter
        if waiter is not None:
            self._waiter = None
            if not waiter.cancelled():
                waiter.set_exception(exc)

    def _wakeup_waiter(self):
        """Wakeup read*() functions waiting for data or EOF."""
        waiter = self._waiter
        if waiter is not None:
            self._waiter = None
            if not waiter.cancelled():
                waiter.set_result(None)

    def set_transport(self, transport: ReadTransport):
        """Set read transport"""
        assert self._transport is None, 'Transport already set'
        self._transport = transport

    def _maybe_resume_transport(self):
        if self._paused and len(self._buffer) <= self._limit:
            self._paused = False
            self._transport.resume_reading()

    def feed_eof(self):
        """Feed eof"""
        self._eof = True
        self._wakeup_waiter()

    def at_eof(self):
        """Return True if the buffer is empty and 'feed_eof' was called."""
        return self._eof and not self._buffer

    def feed_data(self, data: bytes):
        """Feed data"""
        assert not self._eof, 'feed_data after feed_eof'

        if not data:
            return

        self._buffer.extend(data)
        self._wakeup_waiter()

        if (self._transport is not None and not self._paused and len(self._buffer) > 2 * self._limit):
            try:
                self._transport.pause_reading()
            except NotImplementedError:
                # The transport can't be paused.
                # We'll just have to buffer all data.
                # Forget the transport so we don't keep trying.
                self._transport = None
            else:
                self._paused = True

    async def _wait_for_data(self, func_name):
        """Wait until feed_data() or feed_eof() is called.

        If stream was paused, automatically resume it.
        """
        # StreamReader uses a future to link the protocol feed_data() method
        # to a read coroutine. Running two read coroutines at the same time
        # would have an unexpected behaviour. It would not possible to know
        # which coroutine would get the next data.
        if self._waiter is not None:
            raise RuntimeError(f'{func_name}() called while another coroutine is already waiting for incoming data')

        assert not self._eof, '_wait_for_data after EOF'

        # Waiting for data while paused will make deadlock, so prevent it.
        # This is essential for readexactly(n) for case when n > self._limit.
        if self._paused:
            self._paused = False
            self._transport.resume_reading()

        self._waiter = self._loop.create_future()
        try:
            await self._waiter
        finally:
            self._waiter = None

    async def readexactly(self, n):
        """Read exactly `n` bytes.

        Raise an IncompleteReadError if EOF is reached before `n` bytes can be
        read. The IncompleteReadError.partial attribute of the exception will
        contain the partial read bytes.

        if n is zero, return empty bytes object.

        Returned value is not limited with limit, configured at stream
        creation.

        If stream was paused, this function will automatically resume it if
        needed.
        """
        if n < 0:
            raise ValueError('readexactly size can not be less than zero')

        if self._exception is not None:
            raise self._exception

        if n == 0:
            return b''

        while len(self._buffer) < n:
            if self._eof:
                incomplete = bytes(self._buffer)
                self._buffer.clear()
                raise IncompleteReadError(incomplete, n)

            await self._wait_for_data('readexactly')

        if len(self._buffer) == n:
            data = bytes(self._buffer)
            self._buffer.clear()
        else:
            data = bytes(self._buffer[:n])
            del self._buffer[:n]
        self._maybe_resume_transport()
        return data
