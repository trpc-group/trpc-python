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

from typing import List
from typing import Optional

from trpc_sdk.log import logger
from .._constants import AddressKind
from .._constants import LOOP_THREAD_STOP_TIMEOUT
from .._constants import SCHEDULE_TARGET_HANDLE_CONN
from .._constants import SCHEDULE_TARGET_IO_CONN
from .._listeners import classify_address
from .._merge_worker import MergeWorker
from ._handler_pool import HandleSlot
from ._handler_pool import SharedHandlerPool
from ._loop_thread import LoopThread
from ._loop_thread import MainLoopReactor
from ._loop_thread import stop_loop_threads
from ._thread_model import ReactorGroup
from ._thread_model import ResourceKind
from ._thread_model import ThreadModel
from ._worker_view import WorkerLoopView


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

    def _build_thread_model(self) -> ThreadModel:
        """I/O on the main loop plus ``io_thread_num - 1`` I/O threads; handlers on the handle threads.

        The thread objects are created here, not started. ``handler_pool`` is
        filled by :meth:`_start_loop_threads`, once the handle loops run.
        """
        main = MainLoopReactor(self.loop)
        self.loop_threads = [LoopThread(i, name_prefix=self._thread_name_prefix) for i in range(self.thread_num)]
        self.io_threads = [LoopThread(i, name_prefix="trpc-io") for i in range(1, self.io_thread_num)]
        return ThreadModel(acceptor=main,
                           io=ReactorGroup([main, *self.io_threads], SCHEDULE_TARGET_IO_CONN),
                           handle=ReactorGroup(list(self.loop_threads), SCHEDULE_TARGET_HANDLE_CONN),
                           tcp_reuse_port=self.tcp_reuse_port,
                           ready_timeout=self.ready_timeout,
                           worker=self,
                           view_factory=WorkerLoopView)

    def _start_loop_threads(self) -> None:
        """Start the handle threads, build the shared pool, then start the I/O loops.

        Handle threads first: the pool needs each of their loops. Starting the
        I/O loops is quick, so doing it afterwards costs nothing; both share
        the ``ready_timeout`` deadline set by the base class.
        """
        super()._start_loop_threads()
        slots = [HandleSlot(thread.index, thread.loop) for thread in self.loop_threads]
        self.shared_pool = SharedHandlerPool(slots, self.handler_max_concurrency, self.handler_queue_size)
        self.thread_model.handler_pool = self.shared_pool

        for thread in self.io_threads:
            thread.start()
        if self.io_threads:
            self._wait_threads_ready(self.io_threads, "I/O")

    def _start_stream(self, index: int) -> None:
        """Serve tRPC over TCP on one address from the I/O loops.

        With the main loop as the only I/O loop the transport serves it with
        ``create_server``; otherwise it accepts on the main loop and hands
        each connection to an I/O loop. Either way the connections hand their
        requests to the handle threads through the shared pool.

        Args:
            index: Position of the address in ``addresses``.
        """
        self._listen_on(index)

    def _start_datagram(self, index: int) -> None:
        """Serve tRPC over UDP on one address, once per I/O loop.

        Several sockets on one address need ``SO_REUSEPORT``, which the
        transport sets; a single one does not get it, so a second instance
        fails to bind instead of sharing.

        Args:
            index: Position of the address in ``addresses``.
        """
        self._listen_on(index)

    async def _release_main_loop(self) -> None:
        """Release the main loop's user protocol transports and aiohttp runners; idempotent.

        Its UDP endpoints stay open until :meth:`_stop_serving_threads` has
        flushed the responses the handle threads queued on the main loop.
        """
        model = self.thread_model
        if model is None:
            return
        main = model.acceptor
        await self._destroy_transports(self._user_transports.pop(id(main), []))
        await model.release_resources(main, ResourceKind.RUNNER)

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
        model = self.thread_model
        if model is not None:
            for endpoint in model.pop_resources(model.acceptor, ResourceKind.ENDPOINT):
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
        await self.thread_model.release_resources(self.io_threads[index - 1], ResourceKind.ENDPOINT)
