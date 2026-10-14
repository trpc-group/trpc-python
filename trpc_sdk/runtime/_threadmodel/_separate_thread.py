# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Worker of ``server.mode: merge`` with ``handle_thread_num > 0`` (``io_handle_type: separate``).

inside one process; not to be confused with ``server.mode: separate`` in
:mod:`~trpc_sdk.runtime._process._separate_worker`, which forks rpc task processes.
Here I/O and user code run on different threads of the same process:

* I/O loops (``io_thread_num``): the main loop, plus ``io_thread_num - 1``
  :class:`~trpc_sdk.runtime._threadmodel._loop_thread.LoopThread` instances named
  ``trpc-io-<k>``. They read, cut frames, stamp ``recv_time`` and write
  responses. They run no user code. The main loop also accepts, handles
  signals and heartbeats, and talks to the naming service.
* Handle threads (``handle_thread_num``), named ``trpc-handle-<j>``: they run
  every piece of user code — handlers, filters, thread hooks, timers, HTTP
  routes, user protocols.

A tRPC unary request crosses threads twice: the I/O loop hands it to a handle
thread through
:class:`~trpc_sdk.runtime._threadmodel._handler_pool.SharedHandlerPool`, and
the handle thread hands the response back through the connection's
:class:`~trpc_sdk.protocol.ConnWriter`. Requests of one connection
may therefore run on several handle threads and complete out of order, which
tRPC already allows: responses are matched by request id.

Streams are the exception to that spreading: every frame of one stream goes
to handle thread ``(session_id + stream_id) % handle_thread_num``, bypassing
the concurrency limit, so a stream's state and its handler live on one loop
and its frames arrive in order. Several streams of one connection may still
land on different handle threads. When the connection closes, the I/O loop
posts ``StreamDispatcher.abort_session`` to each handle thread its streams
were pinned to.

Everything that is not a tRPC frame stays on handle threads end to end: HTTP
connections are accepted on the main thread and handed straight to a handle
thread, and user protocol starters run on handle thread 0, which therefore
does that endpoint's I/O as well.

Handle threads are the "serving threads" of
:class:`~trpc_sdk.runtime._merge_worker.MergeWorker`, so this class reuses its
setup, HTTP, timer and shutdown machinery and only overrides what differs.
"""

from __future__ import annotations

import asyncio
import inspect
import socket
from typing import Any
from typing import Callable
from typing import List
from typing import Optional

from trpc_sdk.log import logger
from trpc_sdk.transport import ListenOption
from trpc_sdk.transport import TrpcServerTransport
from trpc_sdk.transport import release_loop_resources
from .._constants import AddressKind
from .._constants import LOOP_THREAD_STOP_TIMEOUT
from .._constants import SCHEDULE_TARGET_HANDLE_CONN
from .._constants import SCHEDULE_TARGET_IO_CONN
from .._listeners import classify_address
from .._listeners import pop_loop_resources
from .._merge_worker import MergeWorker
from ._handler_pool import HandleSlot
from ._handler_pool import SharedHandlerPool
from ._loop_thread import LoopThread
from ._loop_thread import connect_accepted
from ._loop_thread import stop_loop_threads


class _MainLoopTarget:
    """The main loop as a serving loop, beside the I/O threads; I/O loop 0.

    Has the members a transport uses on a serving loop: ``index``, ``loop``,
    ``submit`` and ``attach_socket``. The acceptor already runs on the main
    loop, so the hand-off is a direct call rather than a
    ``call_soon_threadsafe``.
    """

    index = 0

    def __init__(self, loop: asyncio.AbstractEventLoop) -> None:
        self.loop = loop

    def submit(self, func: Callable[[], Any], timeout: Optional[float] = None) -> Any:
        """Run ``func`` on the main loop, which is not running yet at bind time.

        Args:
            func: Callable; an awaitable result is driven to completion.
            timeout: Unused: the call runs on this thread.

        Returns:
            Whatever ``func`` returned, after awaiting it when applicable.
        """
        del timeout
        result = func()
        if inspect.isawaitable(result):
            result = self.loop.run_until_complete(result)
        return result

    def attach_socket(self,
                      sock: socket.socket,
                      protocol_factory: Callable[[], asyncio.BaseProtocol],
                      ssl_context: Optional[Any] = None) -> None:
        connect_accepted(self.loop, sock, protocol_factory, ssl_context, "main loop")


class SeparateThread(MergeWorker):
    """Merge-mode server process whose I/O loops and handle threads are apart.

    Attributes:
        thread_num: Handle threads (``handle_thread_num``); the serving loops
            of the base class.
        loop_threads: The handle threads.
        io_thread_num: I/O loops, the main loop included.
        io_threads: I/O loops other than the main one, ``1 .. io_thread_num - 1``.
        shared_pool: Hands unary requests to the handle threads; built once
            they are running.
    """

    _thread_name_prefix = "trpc-handle"
    _http_schedule_target = SCHEDULE_TARGET_HANDLE_CONN

    def __init__(self, engine, name: str = "separate_thread_worker") -> None:
        """Copy the endpoint tables out of the engine; nothing is bound yet.

        Args:
            engine: Engine holding ``process_info`` and ``opts``.
            name: Process name shown in logs and process listings.
        """
        super().__init__(engine, name)
        self.io_thread_num: int = max(1, engine.opts.io_thread_num)
        self.io_threads: List[LoopThread] = []
        self.shared_pool: Optional[SharedHandlerPool] = None
        # UDP transports, holding an endpoint per I/O loop; key 0 is the main loop.
        self._io_udp_transports: List[TrpcServerTransport] = []
        self._warn_io_threads_without_tls()

    @staticmethod
    def _resolve_thread_num(opts) -> int:
        """Number of serving loops: ``handle_thread_num`` in the separate thread model."""
        return max(1, opts.handle_thread_num)

    def _uses_loop_threads(self) -> bool:
        """Handle threads always exist here, even a single one."""
        return True

    def _warn_io_threads_without_tls(self) -> None:
        """Warn when extra I/O loops are configured but have little to do.

        Without TLS an I/O loop only frames bytes, which the main loop keeps
        up with on its own; the extra loops pay off for TLS handshakes and
        encryption.
        """
        if self.io_thread_num <= 1:
            return
        tcp = [i for i, _ in enumerate(self.addresses) if self._is_stream(i)]
        if any(self.tran_opts[i].tls_cert and self.tran_opts[i].tls_key for i in tcp):
            return
        logger.warning("[separate thread worker] io_thread_num=%s, but no tRPC TCP endpoint uses TLS; "
                       "extra I/O loops only pay off for TLS", self.io_thread_num)

    def _is_stream(self, index: int) -> bool:
        """Whether address ``index`` is a ``TCP`` one.

        Args:
            index: Position of the address in ``addresses``.
        """
        return classify_address(self, index) is AddressKind.TCP

    def _start_loop_threads(self) -> None:
        """Start the handle threads, build the shared pool, then start the I/O loops.

        Handle threads first: the pool needs each of their loops. Starting the
        I/O loops is quick, so doing it afterwards costs nothing; both share
        the ``ready_timeout`` deadline set by the base class.
        """
        super()._start_loop_threads()
        slots = [HandleSlot(thread.index, thread.loop) for thread in self.loop_threads]
        self.shared_pool = SharedHandlerPool(slots, self.handler_max_concurrency, self.handler_queue_size)

        for index in range(1, self.io_thread_num):
            thread = LoopThread(index, name_prefix="trpc-io")
            self.io_threads.append(thread)
            thread.start()
        if self.io_threads:
            self._wait_threads_ready(self.io_threads, "I/O")

    def _io_loops(self) -> List[Any]:
        """The I/O loops as serving loops: the main loop first, then the I/O threads."""
        return [_MainLoopTarget(self.loop), *self.io_threads]

    def _start_stream(self, index: int) -> None:
        """Serve tRPC over TCP on one address from the I/O loops.

        With the main loop as the only I/O loop the transport serves it with
        ``create_server``; otherwise it accepts on the main loop and hands
        each connection to an I/O loop. Either way the connections hand their
        requests to the handle threads through the shared pool.

        Args:
            index: Position of the address in ``addresses``.
        """
        self._listen_on(index,
                        ListenOption(loop=self.loop,
                                      serving_loops=self._io_loops() if self.io_threads else (),
                                      reuse_port=self.tcp_reuse_port,
                                      shared_pool=self.shared_pool,
                                      schedule_target=SCHEDULE_TARGET_IO_CONN,
                                      ready_timeout=self.ready_timeout))

    def _start_datagram(self, index: int) -> None:
        """Serve tRPC over UDP on one address, once per I/O loop.

        Args:
            index: Position of the address in ``addresses``.
        """
        # Several sockets on one address need SO_REUSEPORT; a single one
        # must not, so a second instance fails to bind instead of sharing.
        self._io_udp_transports.append(self.transports[index])
        self._listen_on(index,
                        ListenOption(loop=self.loop,
                                      serving_loops=self._io_loops(),
                                      reuse_port=self.io_thread_num > 1,
                                      shared_pool=self.shared_pool,
                                      ready_timeout=self.ready_timeout,
                                      worker=self,
                                      index=index))

    def _stop_serving_threads(self) -> None:
        """Stop the handle threads first, then the I/O loops.

        Responses a handle thread wrote before stopping are callbacks queued
        on the I/O loops. The main loop has already stopped by now, so it is
        turned once more to flush them; the other I/O loops run them before
        their own ``loop.stop`` callback, which is queued behind.
        """
        super()._stop_serving_threads()
        self._flush_main_loop()
        if self.io_threads:
            stop_loop_threads(self.io_threads, self._before_stop_io_thread, LOOP_THREAD_STOP_TIMEOUT)
            self.io_threads.clear()
        for endpoint in pop_loop_resources(self._io_udp_transports, _MainLoopTarget.index):
            endpoint.close()

    def _flush_main_loop(self) -> None:
        """Run the callbacks already queued on the stopped main loop, once."""
        if self.loop is None or self.loop.is_closed() or self.loop.is_running():
            return
        self.loop.call_soon(self.loop.stop)
        self.loop.run_forever()

    async def _before_stop_io_thread(self, index: int) -> None:
        """Close one I/O loop's UDP endpoints; runs on that loop.

        Args:
            index: Position of the I/O loop; ``1 .. io_thread_num - 1``.
        """
        await release_loop_resources(pop_loop_resources(self._io_udp_transports, index))
