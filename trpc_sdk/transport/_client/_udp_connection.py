# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Connected UDP endpoint used by the client UDP round trip.

:func:`open_datagram_endpoint` builds a remote-addr-connected datagram
socket and wraps it in :class:`PacketConnection`. That object is what
:func:`udp_round_trip` writes to and reads from.

The split is the usual asyncio one: :class:`PacketReaderProtocol` is
the callback side (``datagram_received`` / ``error_received`` /
``connection_lost``), :class:`PacketReader` is the buffer a coroutine
waits on, :class:`PacketWriter` exposes ``write`` / ``close``, and
:class:`PacketConnection` combines reader and writer into one handle.

Public symbols are re-exported from :mod:`trpc_sdk.transport`.
"""

from __future__ import annotations

import socket
from asyncio import DatagramProtocol

from trpc_sdk.common import get_or_create_event_loop
from trpc_sdk.utils import is_ipv6


async def open_datagram_endpoint(host=None, port=None, *, loop=None, **kwds):
    """Open a connected UDP socket and wrap it in a :class:`PacketConnection`.

    ``remote_addr`` is set at create time, so later ``sendto`` needs no
    address. IPv6 hosts force ``AF_INET6``.

    Args:
        host: Remote host.
        port: Remote port.
        loop: Event loop; defaults to :func:`get_or_create_event_loop`.
        **kwds: Extra kwargs forwarded to ``create_datagram_endpoint``.

    Returns:
        A :class:`PacketConnection` ready to ``write`` and ``receive``.
    """
    if loop is None:
        loop = get_or_create_event_loop()

    reader = PacketReader(loop=loop)
    protocol = PacketReaderProtocol(reader, loop=loop)
    if is_ipv6(host):
        transport, _ = await loop.create_datagram_endpoint(lambda: protocol,
                                                           remote_addr=(host, port),
                                                           family=socket.AF_INET6,
                                                           **kwds)
    else:
        transport, _ = await loop.create_datagram_endpoint(lambda: protocol, remote_addr=(host, port), **kwds)
    conn = PacketConnection(transport, protocol, reader, loop)
    return conn


class PacketReaderProtocol(DatagramProtocol):
    """Datagram protocol that feeds :class:`PacketReader`.

    A connected datagram endpoint does not call ``connection_made``
    with a useful transport for our purposes, so the reader is wired
    in at construction time instead.
    """

    def __init__(self, packet_reader, loop=None):
        super().__init__()
        self._loop = loop
        self._packet_reader = packet_reader
        self._packet_writer = None
        self._closed = self._loop.create_future()

    def datagram_received(self, data, addr):
        """Push one datagram onto the reader.

        Args:
            data: Packet bytes.
            addr: Source address.
        """
        self._packet_reader.feed_data(data, addr)

    def error_received(self, exc):
        """Record a socket error on the reader.

        Args:
            exc: Exception from the datagram transport.
        """
        self._packet_reader.set_exception(exc)

    def connection_lost(self, exc):
        """Complete ``_closed`` and detach the reader.

        Args:
            exc: None on a clean close; otherwise the loss reason.
        """
        if self._packet_reader is not None:
            if exc:
                self._packet_reader.set_exception(exc)
        if not self._closed.done():
            if exc is None:
                self._closed.set_result(None)
            else:
                self._closed.set_exception(exc)
        super().connection_lost(exc)
        self._packet_reader = None
        self._packet_writer = None

    def __del__(self):
        # Drain an unretrieved exception so the interpreter does not
        # warn about an unhandled Future. Safer than the
        # ``_closed._log_traceback = False`` hack.
        closed = self._closed
        if closed.done() and not closed.cancelled():
            closed.exception()


class PacketReader:
    """Buffer of datagrams waiting for a ``receive`` coroutine.

    ``feed_data`` (called from the protocol) appends; ``receive``
    pops. Only one coroutine may wait at a time — two concurrent
    ``receive`` calls cannot tell which packet belongs to which waiter.
    """

    def __init__(self, loop=None):
        self._loop = loop if loop else get_or_create_event_loop()
        self._packet = []
        self._waiter = None  # A future used by _wait_for_data()
        self._exception = None
        self._transport = None

    def __repr__(self):
        info = ['PacketReader']
        if self._packet:
            info.append('{} packets'.format(len(self._packet)))
        if self._waiter:
            info.append('waiter={!r}'.format(self._waiter))
        if self._exception:
            info.append('exception={!r}'.format(self._exception))
        return '<{}>'.format(' '.join(info))

    def exception(self):
        """Return the exception set by the protocol, or None."""
        return self._exception

    def set_exception(self, exc):
        """Record ``exc`` and fail the waiter if one is pending.

        Args:
            exc: Exception from the protocol, or None to clear.
        """
        self._exception = exc

        waiter = self._waiter
        if waiter is not None:
            self._waiter = None
            if not waiter.cancelled():
                waiter.set_exception(exc)

    def _wakeup_waiter(self):
        """Wake a ``receive`` coroutine that is waiting for a packet."""
        waiter = self._waiter
        if waiter is not None:
            self._waiter = None
            if not waiter.cancelled():
                waiter.set_result(None)

    def feed_data(self, data, addr):
        """Append one datagram and wake a waiter if any.

        Empty ``data`` is ignored.

        Args:
            data: Packet bytes.
            addr: Source address stored alongside the bytes.
        """
        if not data:
            return

        self._packet.append((data, addr))
        self._wakeup_waiter()

    async def _wait_for_data(self, func_name):
        """Wait until :meth:`feed_data` appends a packet.

        PacketReader uses a future to link the protocol's
        ``feed_data`` to a read coroutine. Running two read coroutines
        at once would make it impossible to know which one gets the
        next packet.

        Args:
            func_name: Caller name used in the "already waiting" error.

        Raises:
            RuntimeError: Another coroutine is already waiting.
        """
        if self._waiter is not None:
            raise RuntimeError(f'{func_name}() called while another coroutine is already waiting for incoming data')

        self._waiter = self._loop.create_future()
        try:
            await self._waiter
        finally:
            self._waiter = None

    async def receive(self):
        """Pop one ``(data, addr)`` packet, waiting if the buffer is empty.

        Returns:
            ``(data, addr)`` from the most recently fed packet.

        Raises:
            The exception previously set by :meth:`set_exception`.
        """
        if self._exception is not None:
            raise self._exception

        if not self._packet:
            await self._wait_for_data('read')

        data = self._packet.pop()
        return data


class PacketWriter:
    """Thin wrapper around a datagram transport.

    Exposes ``write``, ``close``, ``wait_closed``, and
    ``get_extra_info``. ``write`` is ``sendto`` with no address because
    the endpoint was created connected.
    """

    def __init__(self, transport, protocol, reader, loop):
        self._transport = transport
        self._protocol = protocol
        assert reader is None or isinstance(reader, PacketReader)
        self._reader = reader
        self._loop = loop

    def __repr__(self):
        info = [self.__class__.__name__, 'transport={!r}'.format(self._transport)]
        if self._reader is not None:
            info.append('reader={!r}'.format(self._reader))
        return '<{}>'.format(' '.join(info))

    @property
    def transport(self):
        """Underlying asyncio datagram transport."""
        return self._transport

    def write(self, data):
        """Send ``data`` to the connected remote address.

        Args:
            data: Packet bytes.
        """
        self._transport.sendto(data)

    def close(self):
        """Close the datagram transport."""
        return self._transport.close()

    def is_closing(self):
        """Return whether the transport is already closing."""
        return self._transport.is_closing()

    async def wait_closed(self):
        """Wait until :meth:`PacketReaderProtocol.connection_lost` fires."""
        await self._protocol._closed

    def get_extra_info(self, name, default=None):
        """Forward to the transport's extra info.

        Args:
            name: Extra-info key.
            default: Value when the key is missing.
        """
        return self._transport.get_extra_info(name, default)


class PacketConnection(PacketWriter):
    """Read/write handle returned by :func:`open_datagram_endpoint`."""

    def __init__(self, transport, protocol, reader, loop):
        super().__init__(transport, protocol, reader, loop)

    def exception(self):
        """Return the reader's exception, or None."""
        return self._reader.exception()

    async def receive(self):
        """Receive one datagram from the reader.

        Returns:
            ``(data, addr)`` as stored by :meth:`PacketReader.feed_data`.
        """
        return await self._reader.receive()
