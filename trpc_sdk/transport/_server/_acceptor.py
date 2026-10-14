# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Main-thread accept loop that hands connections to serving loop threads.

A threaded worker binds each public address once and accepts on the main
thread only. ``asyncio.create_server`` cannot do this, because it builds the
transport on the same loop that accepted: it would pin every connection to the
main loop. So the listen socket is driven directly with ``add_reader`` and each
accepted fd is passed to a serving loop (a
:class:`~trpc_sdk.runtime._threadmodel._loop_thread.LoopThread`, or anything with the same
``attach_socket``) with ``connect_accepted_socket``, which is the supported way
to move an accepted socket onto another loop.

:class:`~trpc_sdk.transport._server.TrpcServerTransport` builds and owns the
acceptors; the runtime only says which loops are the targets.

Which thread gets a connection comes from
:func:`~trpc_sdk.transport.get_next_worker`, the same selector the dispatcher uses
to pick a worker process; here the returned index is read as a thread index.
That keeps one policy — including anything registered through
``register_dispatcher_worker_func`` — for both process models.

The selector's sticky-stream branch never fires here, and does not need to: it
exists so all frames of one stream reach one worker, and routing whole
connections already guarantees that. Streams do not span connections.
"""

from __future__ import annotations

import asyncio
import errno
import socket
from dataclasses import dataclass
from typing import Any
from typing import Callable
from typing import List
from typing import Optional
from typing import Sequence
from typing import Tuple

from trpc_sdk.log import logger
from .._scheduler import get_next_worker
from .._types import SchedulerParams
from ._constants import ACCEPT_BURST
from ._constants import ACCEPT_RETRY_DELAY

# errnos that mean "out of resources", not "this connection is broken".
_RESOURCE_ERRNOS = frozenset({errno.EMFILE, errno.ENFILE, errno.ENOBUFS, errno.ENOMEM})


@dataclass
class AcceptTarget:
    """One serving loop plus the protocol factory to use for sockets sent to it.

    The factory is per target rather than per acceptor because some listeners
    cannot share one across loops. An aiohttp ``AppRunner`` builds a ``Server``
    bound to whichever loop was running during ``setup()``, so each thread
    needs its own runner and therefore its own factory. Plain tRPC protocols
    have no such constraint and every target repeats the same factory.
    """

    # Owner of the loop that will own the transport for connections sent
    # here: a :class:`~trpc_sdk.runtime._threadmodel._loop_thread.LoopThread`, or anything
    # else with the same ``attach_socket(sock, factory, ssl_context)``, such
    # as the main loop under the separate thread model.
    thread: Any
    # Builds the protocol serving one connection on ``thread``'s loop.
    protocol_factory: Callable[[], asyncio.BaseProtocol]


class Acceptor:
    """Owns one listen socket; accepts on the main loop, serves elsewhere.

    One instance per listen endpoint. It is deliberately the only thing that
    touches the listen socket: a second accepter on the same fd would make the
    scheduler's view of load meaningless.

    Attributes:
        address: Bind address of this endpoint, kept for log messages.
    """

    def __init__(self,
                 sock: socket.socket,
                 targets: Sequence[AcceptTarget],
                 loop: asyncio.AbstractEventLoop,
                 address: Optional[Tuple[str, int]] = None,
                 ssl_context: Optional[Any] = None,
                 schedule_target: str = "") -> None:
        """Record the listen socket and its targets; accepting starts later.

        Args:
            sock: Non-blocking, already listening socket from
                ``create_new_socket``.
            targets: Loop threads this endpoint may hand connections to. Must
                not be empty; a worker with no loop threads serves on the main
                loop and does not use an acceptor at all.
            loop: Main loop that will watch ``sock`` for readability.
            address: Bind address, used only in log messages.
            ssl_context: Server-side ``SSLContext`` for a TLS endpoint, or
                ``None`` for plaintext.
            schedule_target: ``SchedulerParams.target`` passed to the
                selector; empty except under the separate thread model.

        Raises:
            ValueError: ``targets`` is empty.
        """
        if not targets:
            raise ValueError("acceptor needs at least one loop thread target")

        self.address: Optional[Tuple[str, int]] = address
        self._sock = sock
        self._targets: List[AcceptTarget] = list(targets)
        self._loop = loop
        self._ssl_context = ssl_context
        self._schedule_target = schedule_target
        # True while ``add_reader`` is armed. Tracked because the accept loop
        # disarms itself on resource exhaustion and ``close`` must know.
        self._reading: bool = False
        # Set by ``close``. Needed because the pause timer may still be pending
        # when the worker shuts down, and re-arming would then hand the loop a
        # closed socket's fd, which is -1.
        self._closed: bool = False
        # Monotonic per-connection id handed to the scheduler as ``session_id``,
        # so a custom hash-based selector has something stable to hash on. It is
        # local to this acceptor and never leaves the process.
        self._session_id: int = 0

    def start(self) -> None:
        """Arm the accept loop on the main event loop."""
        self._arm_reader()

    def close(self) -> None:
        """Stop accepting and close the listen socket.

        Connections already handed to a loop thread are not affected; they are
        owned by that thread's loop and closed when it stops.
        """
        self._closed = True
        self._disarm_reader()
        self._sock.close()

    def _arm_reader(self) -> None:
        """Start watching the listen socket for readability.

        Also the pause timer's callback, which is why it has to tolerate the
        acceptor having been closed in the meantime.
        """
        if self._closed or self._reading:
            return
        self._loop.add_reader(self._sock.fileno(), self._on_readable)
        self._reading = True

    def _disarm_reader(self) -> None:
        """Stop watching the listen socket, if it is currently watched."""
        if not self._reading:
            return
        self._loop.remove_reader(self._sock.fileno())
        self._reading = False

    def _on_readable(self) -> None:
        """Drain pending connections and route each to a loop thread.

        Accepts in a bounded burst rather than one per callback: ``add_reader``
        is level-triggered, so one-at-a-time would still make progress, but a
        connection spike would cost one loop iteration per connection. The
        bound keeps a spike from starving timers and the rest of the main loop.
        """
        for _ in range(ACCEPT_BURST):
            try:
                conn, addr = self._sock.accept()
            except (BlockingIOError, InterruptedError):
                # Backlog drained, or the syscall was interrupted; either way
                # the reader stays armed and we will be called again.
                return
            except ConnectionAbortedError:
                # The peer went away between the SYN and our accept. Skip it,
                # there may be healthy connections behind it.
                continue
            except OSError as err:
                if err.errno in _RESOURCE_ERRNOS:
                    self._pause_on_resource_error(err)
                else:
                    logger.error("[acceptor] accept on %s failed: %s", self.address, repr(err))
                return
            self._hand_off(conn, addr)

    def _pause_on_resource_error(self, err: OSError) -> None:
        """Disarm the accept loop for a while after running out of resources.

        The listen socket stays readable while the backlog is non-empty, so
        leaving the reader armed would busy-loop on the same failing
        ``accept()`` and burn the main thread. Existing connections keep being
        served in the meantime, which is what frees the descriptors.

        Args:
            err: The ``OSError`` that made ``accept()`` fail.
        """
        logger.error("[acceptor] accept on %s out of resources (%s); pausing %.1fs",
                     self.address, repr(err), ACCEPT_RETRY_DELAY)
        self._disarm_reader()
        self._loop.call_later(ACCEPT_RETRY_DELAY, self._arm_reader)

    def _hand_off(self, conn: socket.socket, addr: Any) -> None:
        """Pick a loop thread for ``conn`` and give the socket away.

        Args:
            conn: Freshly accepted socket. From here on it belongs to the
                selected thread's loop.
            addr: Peer address from ``accept()``: a ``(host, port)`` tuple for
                inet sockets, and an empty string for UNIX-domain ones.
        """
        conn.setblocking(False)
        self._session_id += 1

        schedule = SchedulerParams(worker_conn_num=len(self._targets),
                                   remote_ip=addr[0] if isinstance(addr, tuple) and addr else "",
                                   session_id=self._session_id,
                                   stream_id=0,
                                   target=self._schedule_target)
        # Modulo because the index may come from a user-registered selector;
        # an out-of-range value must not take the whole accept loop down.
        target = self._targets[get_next_worker(schedule) % len(self._targets)]
        target.thread.attach_socket(conn, target.protocol_factory, self._ssl_context)
