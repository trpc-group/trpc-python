# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Datagram protocol of a tRPC-over-UDP listen socket.

:meth:`~trpc_sdk.transport.TrpcServerTransport.datagram_protocol_factory`
builds one :class:`DefaultDatagramConnection` per serving loop, after fork.
Unlike the stream connections in :mod:`._stream_connection`, one protocol object
serves every peer of its socket: there is no per-peer connection to open, idle
out or close.

Each datagram gets its own immutable :class:`DatagramPeer`, and that peer is
the ``conn`` its frames are handed to ``msg_handler`` with. A handler reading
``conn.remote_addr`` after any number of awaits, or from a handle thread,
still sees its own datagram's source and answers to it.
"""

from __future__ import annotations

import asyncio
from functools import partial
from typing import Any
from typing import Optional
from typing import Tuple

from trpc_sdk.exceptions import ServerDecodeError
from trpc_sdk.log import logger
from trpc_sdk.telemetry import report_attr

from ._conn_writer import ConnWriter
from ._stream_connection import ConnectionOption
from ._stream_connection import _to_host_port


class DatagramPeer:
    """The ``conn`` of the frames split out of one datagram; never mutated.

    Built on the socket's loop in ``datagram_received``, so its
    :class:`ConnWriter` is owned by that loop: :meth:`write` sends directly
    there and posts with ``call_soon_threadsafe`` from a handle thread.

    Attributes:
        aio_conn: The listen socket's datagram transport, shared by every peer.
        local_addr: Local ``(host, port)`` of the listen socket.
        remote_addr: Source ``(host, port)`` of this datagram.
    """

    __slots__ = ("aio_conn", "local_addr", "remote_addr", "_writer")

    def __init__(self,
                 loop: asyncio.AbstractEventLoop,
                 aio_conn: asyncio.DatagramTransport,
                 local_addr: Tuple[str, int],
                 remote_addr: Tuple[str, int],
                 source: Any) -> None:
        """Bind the peer to one datagram's source.

        Args:
            loop: Loop that owns ``aio_conn``; must be the calling thread's.
            aio_conn: Datagram transport responses are sent on.
            local_addr: Local ``(host, port)``.
            remote_addr: ``source`` as ``(host, port)``.
            source: Address exactly as ``datagram_received`` gave it, used as
                the ``sendto`` destination (an IPv6 source keeps its flow
                info and scope id).
        """
        self.aio_conn = aio_conn
        self.local_addr = local_addr
        self.remote_addr = remote_addr
        self._writer = ConnWriter(loop, aio_conn, source)

    def write(self, data: bytes) -> None:
        """Send ``data`` to this datagram's source; callable from any thread."""
        self._writer.write(data)

    def is_closing(self) -> bool:
        """Whether the listen socket is closing."""
        return self.aio_conn.is_closing()


class DefaultDatagramConnection(asyncio.DatagramProtocol):
    """Split each datagram into frames and hand each one to ``option.msg_handler``.

    Built from a :class:`~trpc_sdk.transport.ConnectionOption` shared by every
    protocol object of the listen socket. Only ``parser``, ``msg_handler`` and
    ``shared_pool`` are read; the lifecycle callbacks are for stream
    connections and stay unused.

    ``parser.create()`` runs once, in ``__init__``, so each serving loop gets
    its own splitter. ``msg_handler`` is ``async (conn, frame) -> None`` and
    receives a :class:`DatagramPeer` as ``conn``. When ``shared_pool`` is set
    it runs on a handle thread and the response comes back to this loop
    through the peer's :class:`ConnWriter`; otherwise it
    runs as a task on this loop. Exceptions it lets escape are counted and
    logged.

    A decode error drops the rest of that datagram and resets the parser;
    the socket keeps serving, since one bad datagram says nothing about the
    next peer.

    Attributes:
        option: The listener's shared :class:`~trpc_sdk.transport.ConnectionOption`.
        parser: This protocol object's frame splitter.
        aio_conn: The datagram transport, set in ``connection_made``.
        local_addr: Local ``(host, port)``, set in ``connection_made``.
    """

    def __init__(self, option: ConnectionOption) -> None:
        self.option = option
        self.parser = option.parser.create()
        self.aio_conn: Optional[asyncio.DatagramTransport] = None
        self.local_addr: Tuple[str, int] = _to_host_port(None)
        self._loop: Optional[asyncio.AbstractEventLoop] = None

    def connection_made(self, transport: asyncio.BaseTransport) -> None:
        """Keep the datagram transport, its loop and the local address."""
        self.aio_conn = transport
        self.local_addr = _to_host_port(transport.get_extra_info("sockname"))
        self._loop = asyncio.get_event_loop()

    def datagram_received(self, data: bytes, addr: Any) -> None:
        """Split one datagram and start one handler per complete frame.

        Args:
            data: Datagram payload.
            addr: Source address as the transport reports it.
        """
        self.parser.feed_data(data)
        peer = None
        while True:
            try:
                frames = self.parser.read()
            except ServerDecodeError as err:
                report_attr("trpc_datagram_connection_read_err", 1)
                logger.error(f"[datagram_connection] failed to decode datagram, remote_addr: {addr}, err: {err}",
                             exc_info=True)
                self.parser.reset()
                return
            except Exception as err:  # pylint: disable=broad-except
                report_attr("trpc_datagram_connection_read_fatal_err", 1)
                logger.error(f"[datagram_connection] fatal err while decoding datagram, remote_addr: {addr}, "
                             f"err: {err}", exc_info=True)
                self.parser.reset()
                return
            if not frames:
                return
            if peer is None:
                peer = DatagramPeer(self._loop, self.aio_conn, self.local_addr, _to_host_port(addr), addr)
            for frame in frames:
                self._dispatch(peer, frame)

    def _dispatch(self, peer: DatagramPeer, frame: bytes) -> None:
        """Run one frame's handler on the shared pool, or as a task on this loop."""
        shared_pool = self.option.shared_pool
        if shared_pool is not None:
            shared_pool.submit(partial(self._run_msg, peer, frame))
            return
        self._loop.create_task(self._run_msg(peer, frame))

    async def _run_msg(self, peer: DatagramPeer, frame: bytes) -> None:
        """Await ``msg_handler`` for one frame; its errors are only logged."""
        try:
            await self.option.msg_handler(peer, frame)
        except Exception as err:  # pylint: disable=broad-except
            report_attr("trpc_datagram_connection_msg_handler_err", 1)
            logger.error(f"[datagram_connection] msg handler failed, remote_addr: {peer.remote_addr}, err: {err}",
                         exc_info=True)

    def error_received(self, exc: Exception) -> None:
        """A send or receive failed on the socket (e.g. ICMP port unreachable); keep serving."""
        report_attr("trpc_datagram_connection_socket_err", 1)
        logger.warning(f"[datagram_connection] socket error: {exc!r}")
