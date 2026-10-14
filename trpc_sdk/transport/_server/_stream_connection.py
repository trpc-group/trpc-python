# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Accepted stream connections of a server listen socket, and their options.

A listen socket's protocol factory is one of two shapes, chosen by
:meth:`~trpc_sdk.transport.TrpcServerTransport.protocol_factory`:

* ``partial(TCPConnection, TcpConnectionOption(...))`` — the Cython tRPC
  connection; it copies the option's fields into C-level slots in
  ``__init__`` and serves unary, stream and ``shared_pool`` requests itself.
* ``partial(DefaultIoConnection, ConnectionOption(...))`` — a pure-Python
  connection that only frames bytes and manages its own lifecycle; what a
  frame means is decided by ``ConnectionOption.msg_handler``.

The same :class:`ConnectionOption` is what
:class:`~trpc_sdk.transport.DefaultDatagramConnection` is built from. A
datagram socket has no per-peer connection to open, idle out or close, so
that protocol reads only ``parser``, ``msg_handler`` and ``shared_pool``.

Either option is built once per listen socket, after fork, and shared by every
connection that socket accepts, so a connection never writes into it.

:class:`ServerConnection` is the peer view ``TrpcServerTransport._msg_handler``
is given: a :class:`DefaultIoConnection` is one itself, and a UDP listen socket
builds one immutable ``DatagramPeer`` per datagram.
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from dataclasses import field
from typing import Any
from typing import Awaitable
from typing import Callable
from typing import Dict
from typing import Optional
from typing import Protocol
from typing import Tuple

from trpc_sdk.abc import BaseServerProtocolParser
from trpc_sdk.exceptions import ServerDecodeError
from trpc_sdk.log import logger
from trpc_sdk.telemetry import report_attr

# Async; awaited once per complete frame, in its own task, and responsible for
# writing the response back with ``conn.write``.
MsgHandler = Callable[[Any, bytes], Awaitable[None]]
# Called by the idle timer; True closes the connection, False re-arms the timer.
ConnectionIdleCleanHandler = Callable[[Any], bool]
ConnectionInitHandler = Callable[[Any], None]
ConnectionCloseHandler = Callable[[Any], None]

_UNKNOWN_ADDR = ("0.0.0.0", 0)


def _to_host_port(addr: Any) -> Tuple[str, int]:
    """Coerce a ``sockname`` / ``peername`` / datagram source to ``(host, port)``.

    A UNIX socket path becomes ``(path, 0)``; an IPv6 4-tuple keeps only host
    and port; a missing address becomes ``("0.0.0.0", 0)``.
    """
    if not addr:
        return _UNKNOWN_ADDR
    if isinstance(addr, str):
        return (addr, 0)
    if isinstance(addr, (tuple, list)) and len(addr) >= 2:
        return (addr[0], addr[1])
    return _UNKNOWN_ADDR


class ServerConnection(Protocol):
    """What ``TrpcServerTransport._msg_handler`` reads from ``conn``.

    For TCP it is the :class:`DefaultIoConnection` itself; one connection 
    has one peer, so its addresses never change. 
    For UDP it is a per-datagram ``DatagramPeer``, so a handler always sees 
    its own datagram's source no matter how many datagrams arrived after it.

    Attributes:
        aio_conn: The asyncio transport; for UDP, the listen socket's
            datagram transport shared by every peer.
        local_addr: Local ``(host, port)``.
        remote_addr: Peer ``(host, port)``.
    """

    aio_conn: Any
    local_addr: Tuple[str, int]
    remote_addr: Tuple[str, int]

    def write(self, data: bytes) -> None:
        """Send ``data`` to this peer."""

    def is_closing(self) -> bool:
        """Whether a response can no longer be sent."""


@dataclass
class TcpConnectionOption:
    """What one listen socket's Cython ``TCPConnection`` objects are built from.

    ``TCPConnection`` reads these fields by name in ``__init__`` and never
    touches the object again.

    Attributes:
        parser: Frame-parser factory. Each connection calls ``parser.create()``
            once, so every connection gets its own splitter.
        rpc_handler: Service entry run for every complete frame, unary and
            stream alike; typically the bound ``ServiceAdapter.handle``.
        timeout: Request timeout in milliseconds, copied onto each frame's
            ``ctx.timeout``.
        shared_pool: ``SharedHandlerPool`` of ``io_handle_type: separate``.
            When set, the connection's loop only frames requests and writes
            responses; handlers run on the pool's handle threads.
    """

    parser: Any
    rpc_handler: Callable[..., Awaitable[Optional[bytes]]]
    timeout: int
    shared_pool: Optional[Any] = None


@dataclass
class ConnectionOption:
    """What one listen socket's Python connections need.

    Shared by :class:`DefaultIoConnection` (TCP) and
    :class:`~trpc_sdk.transport.DefaultDatagramConnection` (UDP). Built once
    per listen socket, after fork, and never written into.

    Not every field applies to both. The lifecycle callbacks and
    ``custom_data`` are TCP only: a datagram socket has no per-peer connection
    to open, idle out or close, so
    :class:`~trpc_sdk.transport.DefaultDatagramConnection` ignores them.
    ``shared_pool`` is UDP only.
    :meth:`~trpc_sdk.transport.TrpcServerTransport.protocol_factory` never
    sets it on the TCP path, because a subclass that overrides
    ``_msg_handler`` runs its handlers on the connection's own loop, and
    :class:`DefaultIoConnection` ignores the field.

    Every callback left ``None`` falls back to a default kept on the TCP
    connection itself; the shared option is never filled in.

    Attributes:
        parser: Frame-parser factory. Each connection calls ``parser.create()``
            once, so every connection gets its own splitter. For UDP that
            call happens once per serving loop, in the protocol object's
            ``__init__``.
        msg_handler: ``async (conn, frame) -> None`` run for every complete
            frame. On TCP, ``conn`` is the :class:`DefaultIoConnection` and
            the coroutine runs in its own task on the connection's loop. On
            UDP, ``conn`` is the per-datagram peer. Exceptions it lets escape
            are counted and logged, and do not close the connection.
        connection_idle_clean_handler: TCP only. ``(conn) -> bool`` asked by
            the idle timer whether to close; ``None`` closes when the
            transport is closing or nothing arrived for
            ``connection_idle_timeout_ms``.
        connection_init_handler: TCP only. ``(conn) -> None`` run in
            ``connection_made`` after ``aio_conn`` and the addresses are set;
            raising closes the connection.
        custom_data: TCP only. Free-form per-listener data for the handlers
            above.
        connection_idle_timeout_ms: TCP only. Idle timeout in milliseconds;
            ``0`` turns idle detection off.
        connection_close_handler: TCP only. ``(conn) -> None`` run once from
            ``close``, before the transport is closed.
        shared_pool: UDP only. ``SharedHandlerPool`` of ``io_handle_type:
            separate``. When set, ``msg_handler`` runs on a handle thread and
            the response comes back to the socket's loop through the peer's
            ``ConnWriter``.
    """

    parser: BaseServerProtocolParser
    msg_handler: MsgHandler
    connection_idle_clean_handler: Optional[ConnectionIdleCleanHandler] = None
    connection_init_handler: Optional[ConnectionInitHandler] = None
    custom_data: Dict[str, Any] = field(default_factory=dict)
    connection_idle_timeout_ms: float = 0
    connection_close_handler: Optional[ConnectionCloseHandler] = None
    shared_pool: Optional[Any] = None


class BaseConnection(asyncio.Protocol):
    """State every Python server connection keeps.

    Attributes:
        option: The listener's shared :class:`ConnectionOption`; read only.
        aio_conn: The asyncio transport, set in ``connection_made``.
        local_addr: Local ``(host, port)``, set in ``connection_made``; a
            UNIX socket path is ``(path, 0)``.
        remote_addr: Peer ``(host, port)``, set in ``connection_made``;
            ``("0.0.0.0", 0)`` before it or when the transport reports no
            peer (UNIX sockets).
        last_recv_time: ``time.monotonic()`` of the last ``data_received``;
            only maintained while idle detection is on.
        check_task: The idle-check task, or ``None`` when idle detection is
            off or the connection is closed.
    """

    def __init__(self, option: ConnectionOption) -> None:
        self.option = option
        self.aio_conn: Optional[asyncio.Transport] = None
        self.local_addr: Tuple[str, int] = _UNKNOWN_ADDR
        self.remote_addr: Tuple[str, int] = _UNKNOWN_ADDR
        self._is_closed = False
        self._paused = False
        self.last_recv_time = 0.0
        self.check_task: Optional[asyncio.Task] = None


class DefaultIoConnection(BaseConnection):
    """Split frames and hand each one to ``option.msg_handler`` in its own task.

    Holds no protocol knowledge: unlike the Cython ``TCPConnection`` it has no
    ``shared_pool`` hand-off and no tRPC stream bookkeeping. Everything that
    ends the connection — a decode error, the peer closing, the idle timer —
    goes through :meth:`close`, so the close callback runs exactly once.
    """

    def __init__(self, option: ConnectionOption) -> None:
        super().__init__(option)
        self.parser = option.parser.create()
        self._init_handler = option.connection_init_handler or self._default_connection_init_handler
        self._close_handler = option.connection_close_handler or self._default_connection_close_handler
        self._idle_clean_handler = (option.connection_idle_clean_handler
                                    or self._default_connection_idle_clean_handler)

    def connection_made(self, transport: asyncio.BaseTransport) -> None:
        """Record the transport and addresses, run the init callback, start the idle timer."""
        try:
            self.aio_conn = transport
            self.local_addr = _to_host_port(transport.get_extra_info("sockname"))
            self.remote_addr = _to_host_port(transport.get_extra_info("peername"))
            self._init_handler(self)
            if self.option.connection_idle_timeout_ms > 0:
                self.last_recv_time = time.monotonic()
                self.check_task = asyncio.get_event_loop().create_task(self.conn_idle_check())
        except Exception as err:  # pylint: disable=broad-except
            report_attr("trpc_io_connection_init_err", 1)
            logger.error(f"[io_connection] failed to make connection with client, err: {err}", exc_info=True)
            self.close()

    def data_received(self, data: bytes) -> None:
        """Feed ``data`` to the parser and start one task per complete frame.

        A decode error closes the connection; frames split before it in the
        same call are still handled.
        """
        if self.option.connection_idle_timeout_ms > 0:
            self.last_recv_time = time.monotonic()
        self.parser.feed_data(data)
        loop = asyncio.get_event_loop()
        while not self._is_closed:
            try:
                frames = self.parser.read()
            except ServerDecodeError as err:
                report_attr("trpc_io_connection_read_err", 1)
                logger.error(f"[io_connection] failed to decode in client conn, remote_addr: "
                             f"{self.remote_addr}, err: {err}", exc_info=True)
                self.close()
                return
            except Exception as err:  # pylint: disable=broad-except
                report_attr("trpc_io_connection_read_fatal_err", 1)
                logger.error(f"[io_connection] fatal err while decoding in client conn, remote_addr: "
                             f"{self.remote_addr}, err: {err}", exc_info=True)
                self.close()
                return
            if not frames:
                return
            for frame in frames:
                loop.create_task(self._run_msg(frame))

    async def _run_msg(self, frame: bytes) -> None:
        """Await ``msg_handler`` for one frame; its errors never close the connection."""
        try:
            await self.option.msg_handler(self, frame)
        except Exception as err:  # pylint: disable=broad-except
            report_attr("trpc_io_connection_msg_handler_err", 1)
            logger.error(f"[io_connection] msg handler failed, remote_addr: {self.remote_addr}, err: {err}",
                         exc_info=True)

    def write(self, data: bytes) -> None:
        """Write ``data`` on the transport; call on the connection's loop."""
        self.aio_conn.write(data)

    def is_closing(self) -> bool:
        """Whether the transport is missing or already closing."""
        return self.aio_conn is None or self.aio_conn.is_closing()

    def connection_lost(self, exc: Optional[Exception]) -> None:
        """The transport is gone; release the connection."""
        if exc:
            logger.warning(f"[io_connection] connection_lost, remote_addr: {self.remote_addr}, err: {exc}")
        self.close()

    async def conn_idle_check(self) -> None:
        """Ask the idle-clean callback every timeout period; close when it says so."""
        try:
            while not self._is_closed:
                await asyncio.sleep(self.option.connection_idle_timeout_ms / 1000)
                if self._idle_clean_handler(self):
                    # Detach first so ``close`` does not cancel the running task.
                    self.check_task = None
                    self.close()
                    return
        except asyncio.CancelledError:
            report_attr("conn_idle_check_cancel_err", 1)
            logger.debug("[io_connection] conn_idle_check CancelledError")
        finally:
            logger.debug("[io_connection] conn_idle_check exit")

    def close(self) -> None:
        """Close the connection once; later calls do nothing.

        Order: mark closed, run the close callback while the transport is
        still open, stop the idle timer, close the transport, reset the parser.
        """
        if self._is_closed:
            return
        self._is_closed = True
        try:
            self._close_handler(self)
        except Exception as err:  # pylint: disable=broad-except
            logger.error(f"[io_connection] close handler failed, remote_addr: {self.remote_addr}, err: {err}",
                         exc_info=True)
        if self.check_task is not None:
            self.check_task.cancel()
            self.check_task = None
        if self.aio_conn is not None and not self.aio_conn.is_closing():
            self.aio_conn.close()
        self.parser.reset()

    def pause_writing(self) -> None:
        """The transport's write buffer is above the high-water mark."""
        if self._paused:
            return
        self._paused = True

    def resume_writing(self) -> None:
        """The transport's write buffer drained below the low-water mark."""
        if not self._paused:
            return
        self._paused = False

    def _default_connection_init_handler(self, conn: DefaultIoConnection) -> None:
        logger.debug(f"[io_connection] connection init, remote_addr: {conn.remote_addr}")

    def _default_connection_close_handler(self, conn: DefaultIoConnection) -> None:
        logger.debug(f"[io_connection] connection close, remote_addr: {conn.remote_addr}")

    def _default_connection_idle_clean_handler(self, conn: DefaultIoConnection) -> bool:
        if conn.aio_conn is None or conn.aio_conn.is_closing():
            logger.debug(f"[io_connection] conn is closing, remote_addr: {conn.remote_addr}")
            return True
        idle_timeout_ms = conn.option.connection_idle_timeout_ms
        if (time.monotonic() - conn.last_recv_time) * 1000 > idle_timeout_ms:
            report_attr("conn_idle_check_timeout_err", 1)
            logger.warning(f"[io_connection] conn idle timeout, remote_addr: {conn.remote_addr}, "
                           f"idle_timeout: {idle_timeout_ms}")
            return True
        return False
