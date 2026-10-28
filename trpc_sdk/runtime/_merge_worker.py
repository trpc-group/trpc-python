# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Worker of ``merge`` mode: one process accepts, decodes and runs handlers.

There is no dispatcher and no worker child process here. This worker *is* the
server process: it binds the public addresses, accepts on its main thread, and
serves each connection on one of ``io_thread_num`` event-loop threads. A
request therefore never crosses a process boundary, which is what removes the
IPC hop that ``separate`` mode pays.

Every endpoint is served by its own transport; this worker only decides on
which loops. The serving loops are named in the
:class:`~trpc_sdk.runtime.ThreadModel` this worker installs after fork, and
each transport's ``listen()`` reads them from there:

* tRPC over TCP and HTTP are served by the loop threads: the transport binds
  once, accepts on the main thread, and hands each connection to a loop
  thread picked by :func:`~trpc_sdk.transport.get_next_worker`.
* tRPC over UDP has nothing to accept, so each loop thread binds its own
  ``SO_REUSEPORT`` socket on the same address and the kernel spreads
  datagrams over them. That spreading is a Linux behaviour; BSD-derived
  kernels (macOS) deliver to the most recently bound socket instead, which
  makes UDP effectively single threaded there. It still serves correctly,
  just without the parallelism. ``register_dispatcher_worker_func`` does not
  apply to UDP.
* User protocols (a :class:`~trpc_sdk.transport.UserProtocolTransport`) and
  the timer executor run on serving thread 0, so every piece of user code
  runs on a serving thread and none on the main one. The user worker-exit
  callbacks are the exception: they run on the main thread, after every
  serving loop has finished draining.

Every serving thread is set up completely — task pipes, ``on_start``
hooks — before any address is bound, so no request can arrive on
a loop whose hooks have not run. Threads start in parallel and share one
``ready_timeout`` deadline.

``io_thread_num: 1`` skips all of that: no thread is created and every
listener is attached straight to the main loop, exactly as the multi-listener
executor did. This is both the cheapest configuration and the one that proves
the threaded paths did not change behaviour.

The GIL is the trade-off to understand before choosing this mode. Threads
overlap on I/O waits and on C extensions that release the GIL (protobuf, numpy,
inference runtimes), but Python bytecode stays serialized. A CPU-bound service
wants ``separate`` mode, where handlers run in separate rpc task processes.
"""

from __future__ import annotations

import asyncio
import time
from functools import partial
from typing import Any
from typing import Dict
from typing import Iterable
from typing import List
from typing import Optional
from typing import Sequence

from trpc_sdk.abc import BaseServerTransport
from trpc_sdk.abc import ProcessType
from trpc_sdk.log import logger
from trpc_sdk.transport import UserProtocolTransport
from trpc_sdk.utils import TimerExecutor
from ._constants import LOOP_THREAD_STOP_TIMEOUT
from ._threadmodel import HandlerThreadInfo
from ._threadmodel import LoopThread
from ._threadmodel import MainLoopReactor
from ._threadmodel import ReactorGroup
from ._threadmodel import ResourceKind
from ._threadmodel import ThreadModel
from ._threadmodel import WorkerLoopView
from ._threadmodel import run_start_hooks
from ._threadmodel import run_stop_hooks
from ._threadmodel import set_current_handler_thread
from ._threadmodel import stop_loop_threads
from ._worker import BaseWorker


class MergeWorker(BaseWorker):
    """The server process in ``merge`` mode: it listens and it handles.

    Not a ``Process`` subclass, unlike the executor it replaces: this object
    describes the process it runs in, so there is nobody to fork it.

    Attributes:
        thread_num: Serving loops, i.e. loops that run user code. ``1`` with
            the merge thread model means the main loop serves and no thread
            is created.
        loop_threads: The serving threads; empty when the main loop serves.
        ready_timeout: Seconds all threads get, together, to start.
    """

    # Prefix of the serving threads' OS names.
    _thread_name_prefix = "trpc-loop"
    # ``SchedulerParams.target`` of the HTTP acceptors.
    _http_schedule_target = ""

    def __init__(self, adapter, name: str = "merge_worker") -> None:
        """Copy the endpoint tables out of the server adapter; nothing is bound yet.

        Sockets, threads and the event loop are all created later, in
        :meth:`~trpc_sdk.runtime._worker.BaseWorker.run`. Nothing here may outlive a
        fork boundary either, since a merge worker forks its tasks after being
        constructed and before its loop exists.

        Args:
            adapter: Server adapter holding ``process_info`` (the per-endpoint
                tables filled during ``ServiceAdapter.init_transport``) and ``opts``.
            name: Process name shown in logs and process listings.
        """
        super().__init__(name, ProcessType.WORKER)
        self._bind_process_info(adapter)

        # Already resolved by ``server._option_resolver``; the clamp only
        # guards options built by hand.
        self.thread_num: int = self._resolve_thread_num(adapter.opts)
        self.handler_max_concurrency: int = adapter.opts.handler_max_concurrency
        self.handler_queue_size: int = adapter.opts.handler_queue_size
        self.tcp_reuse_port: bool = adapter.opts.tcp_reuse_port
        self.ready_timeout: float = max(0, adapter.opts.ready_timeout) / 1000.0
        self.loop_threads: List[LoopThread] = []

        # The main loop's identity when it is the serving loop; set once its
        # start hooks ran, so only then do its stop hooks run.
        self._main_thread_info: Optional[HandlerThreadInfo] = None

        # User protocol transports, keyed by ``id()`` of the serving reactor
        # and destroyed there after the hooks. UDP endpoints and aiohttp
        # runners are kept on the thread model instead.
        self._user_transports: Dict[int, List[BaseServerTransport]] = {}

        # Threaded path. Indexed by serving thread; an entry is only set once
        # that thread's start hooks ran, so only those threads run stop hooks.
        self._thread_infos: List[Optional[HandlerThreadInfo]] = [None] * self.thread_num
        # Monotonic deadline shared by every thread's startup.
        self._ready_deadline: float = 0.0
        # Set once the threads are being stopped on purpose, so their exit
        # is not taken for a death; see :meth:`_on_loop_thread_died`.
        self._stopping_threads: bool = False

    @staticmethod
    def _resolve_thread_num(opts) -> int:
        """Number of serving loops: ``io_thread_num`` in the merge thread model.

        Args:
            opts: The server adapter's :class:`~trpc_sdk.server.ServerOption`.
        """
        return max(1, opts.io_thread_num)

    def _uses_loop_threads(self) -> bool:
        """Whether user code runs on loop threads rather than the main loop."""
        return self.thread_num > 1

    def _build_thread_model(self) -> ThreadModel:
        """The main loop alone, or the main loop accepting for ``thread_num`` serving threads.

        The thread objects are created here, not started: thread 0 starts
        user protocols from its init, and they read this model.
        """
        if not self._uses_loop_threads():
            return ThreadModel.main_loop(self.loop,
                                         worker=self,
                                         reuse_port=self.tcp_reuse_port,
                                         ready_timeout=self.ready_timeout)
        main = MainLoopReactor(self.loop)
        self.loop_threads = self._new_loop_threads(range(self.thread_num), self._thread_name_prefix)
        return ThreadModel(acceptor=main,
                           io=ReactorGroup(list(self.loop_threads)),
                           handle=ReactorGroup(list(self.loop_threads), self._http_schedule_target),
                           reuse_port=self.tcp_reuse_port,
                           ready_timeout=self.ready_timeout,
                           worker=self,
                           view_factory=WorkerLoopView)

    def _new_loop_threads(self, indexes: Iterable[int], name_prefix: str) -> List[LoopThread]:
        """Create loop threads, not started, that report their exit to this worker.

        Args:
            indexes: Index of each thread, also its name suffix.
            name_prefix: Prefix of the OS thread names.
        """
        return [LoopThread(i, name_prefix=name_prefix, on_exit=self._on_loop_thread_exit) for i in indexes]

    def _on_loop_thread_exit(self, thread: LoopThread) -> None:
        """Pass a loop thread's exit on to the main loop; runs on the exiting thread.

        The exit template only runs on the main loop, so nothing is decided
        here. A main loop already closed means the process is on its way out.

        Args:
            thread: The thread whose loop just closed.
        """
        try:
            self.loop.call_soon_threadsafe(self._on_loop_thread_died, thread.name)
        except RuntimeError:
            pass

    def _on_loop_thread_died(self, name: str) -> None:
        """Shut the process down with status 1 when a loop thread exits while serving; on the main loop.

        A stop during shutdown, or while :meth:`_stop_serving_threads` undoes
        a failed startup, is expected and ignored. Otherwise the thread died:
        the acceptor would go on handing it connections that are dropped, so
        the process goes down the way it does for a dead child
        (``BaseWorker._replace_child``).

        Args:
            name: OS name of the thread, for the log line.
        """
        if self._exiting or self._stopping_threads:
            return
        logger.error("[merge worker] %s exited unexpectedly; shutting down", name)
        self._exit_code = 1
        self._handle_sigterm()

    def start_worker_process(self) -> None:
        """Serve every configured endpoint; the serving loops already exist.

        Called by :meth:`~trpc_sdk.runtime._worker.BaseWorker._prepare_to_serve`
        once plugins, task connections and every serving thread are up.

        User protocols are started from the serving loop itself: here on a
        single loop, from :meth:`_init_service_thread` on a threaded worker.
        Every other transport is just told to ``listen``; where it serves is
        its own business, read from the thread model. Unsupported networks
        were already rejected by ``check_endpoint`` before fork. A transport
        that fails to listen aborts startup with status 1 rather than
        leaving the process half-listening.
        """
        logger.info("[merge worker] %s start_worker_process(), serving loops: %s", type(self).__name__,
                    self.thread_num)
        for transport in self.transports:
            try:
                if isinstance(transport, UserProtocolTransport):
                    if not self.loop_threads:
                        self._listen_user(transport, self.thread_model.acceptor)
                    continue
                self._listen_on(transport)
            except ValueError as err:
                # A TLS certificate path that does not exist, for instance.
                logger.error("[merge worker] cannot serve %s: %s", transport.opts.address, err)
                self._abort_startup()
            except OSError as err:
                # Typically EADDRINUSE: another instance holds the port and
                # tcp_reuse_port is off. Also TimeoutError (an OSError) from a
                # serving thread that did not set its listener up in time.
                # Serving half the addresses would be worse than not starting.
                logger.error("[merge worker] failed to bind %s: %s", transport.opts.address, repr(err))
                self._abort_startup()

    def _abort_startup(self) -> None:
        """Undo a partial startup, then exit with status 1.

        Stops the transports already listening, stops the threads already
        started (running ``on_stop`` only where ``on_start`` completed), then
        stops the children.

        Raises:
            SystemExit: Always, with status 1.
        """
        self._stop_listening()
        self._stop_serving_threads()
        self._exit_now(1)

    def _task_senders(self) -> List[str]:
        """Return one sender identity per serving loop, so each loop gets its own task pipes.

        An asyncio transport may only be written from the loop that created it.
        Sized from ``thread_num`` because the pipes exist before fork and the
        threads only after it. Override this rather than ``_task_pipe_owners``,
        which defaults to it.
        """
        # Local import: this package's init loads the peer worker, which
        # imports this module. That only works after this class exists.
        from ._process import worker_sender_name

        return [worker_sender_name(index) for index in range(self.thread_num)]

    def _connect_tasks(self) -> None:
        """Set up every serving loop, task connections included.

        Single loop: the base behaviour on the main loop, then the main loop
        becomes the serving loop (identity and ``on_start`` hooks),
        before any listener exists. Threaded: start every serving thread;
        each connects its own task pipes from its own ``init``.
        """
        if not self._uses_loop_threads():
            super()._connect_tasks()
            self._init_main_serving_loop()
            return
        self._start_loop_threads()

    def _init_main_serving_loop(self) -> None:
        """Make the main loop the single serving loop.

        Runs on the main thread before the loop turns, which is why the
        start hooks are driven with ``run_until_complete``. A failing hook
        aborts startup with status 1.
        """
        info = self._main_loop_thread_info()
        set_current_handler_thread(info)
        try:
            self.loop.run_until_complete(run_start_hooks(info))
        except Exception as err:  # pylint: disable=broad-except
            logger.error("[merge worker] on_start thread hook failed: %s", repr(err), exc_info=True)
            self._exit_now(1)
        self._main_thread_info = info

    def _main_loop_thread_info(self) -> HandlerThreadInfo:
        """Identity of the main loop when it is the only serving loop.

        What ``on_start`` / ``on_stop`` hooks see as ``index`` and ``count``.
        A peer worker reports its process index among all peer workers.
        """
        return HandlerThreadInfo(index=0, count=1, loop=self.loop)

    def _start_loop_threads(self) -> None:
        """Start every serving thread in parallel and wait until all are ready.

        Each thread runs :meth:`_init_service_thread` before its loop turns.
        All of them share one deadline, ``ready_timeout`` from now; one that
        fails or misses it aborts startup. The thread objects already exist,
        created by :meth:`_build_thread_model`.
        """
        self._ready_deadline = time.monotonic() + self.ready_timeout
        for thread in self.loop_threads:
            thread.start(partial(self._init_service_thread, thread.index))
        self._wait_threads_ready(self.loop_threads, "serving")

    def _wait_threads_ready(self, threads: Sequence[LoopThread], kind: str) -> None:
        """Wait for ``threads`` against the shared deadline; abort on failure.

        Args:
            threads: Threads already started.
            kind: Which threads these are, for log messages.
        """
        began = time.monotonic()
        for thread in threads:
            try:
                thread.wait_ready(self._ready_deadline - time.monotonic())
            except TimeoutError:
                logger.error("[merge worker] %s not ready within ready_timeout=%.0f ms", thread.name,
                             self.ready_timeout * 1000)
                self._abort_startup()
            except BaseException as err:  # pylint: disable=broad-except
                logger.error("[merge worker] %s failed to start: %s", thread.name, repr(err))
                self._abort_startup()
        logger.info("[merge worker] %s %s thread(s) ready in %.0f ms", len(threads), kind,
                    (time.monotonic() - began) * 1000)

    def _init_service_thread(self, index: int, loop: asyncio.AbstractEventLoop) -> None:
        """Set one serving thread up; runs on that thread before its loop turns.

        The order matters. Identity first, then task pipes,
        so a hook may already produce to a task. ``on_start`` after that, and
        only once it succeeded is the thread recorded as one to run
        ``on_stop`` on. User protocols last, on thread 0 only; they see a
        :class:`~trpc_sdk.runtime._threadmodel.WorkerLoopView` as their worker.

        Args:
            index: Position of the thread among the serving loops.
            loop: The thread's loop, not running yet.

        Raises:
            Exception: Whatever a step raised; startup is aborted.
        """
        info = HandlerThreadInfo(index=index, count=self.thread_num, loop=loop)
        set_current_handler_thread(info)
        if self.task_ipc_mesh is not None:
            from ._process import worker_sender_name  # local: see _task_senders
            pipes = self.task_ipc_mesh.sender_pipes(worker_sender_name(index))
            loop.run_until_complete(self._connect_tasks_on(loop, pipes))
        loop.run_until_complete(run_start_hooks(info))
        self._thread_infos[index] = info

        if index == 0:
            for transport in self.transports:
                if isinstance(transport, UserProtocolTransport):
                    self._listen_user(transport, self.loop_threads[index])

    def _listen_user(self, transport: BaseServerTransport, reactor: Any) -> None:
        """Start one user protocol on ``reactor``'s thread and remember where to destroy its transport.

        Args:
            transport: The endpoint's
                :class:`~trpc_sdk.transport.UserProtocolTransport`.
            reactor: The serving reactor whose thread is calling; the
                starter sees the worker as viewed from its loop.
        """
        self._user_transports.setdefault(id(reactor), []).append(self._listen_on(transport, reactor.loop))

    def _start_timers(self) -> None:
        """Start the timer executor, on serving thread 0 when there are threads.

        One executor per process, as on the single-loop path, so a timer
        without a target name still runs once and not once per thread. Its
        schedules (``call_later``, ``aiocron``, ``call_at``) must be created on
        the loop that runs them, hence ``submit`` rather than ``run()`` here.
        """
        if not self.loop_threads:
            super()._start_timers()
            return
        thread = self.loop_threads[0]
        self.timer = TimerExecutor(loop=thread.loop, name=f"worker:{self.worker_id}")
        try:
            thread.submit(self.timer.run, timeout=self.ready_timeout)
        except Exception as err:  # pylint: disable=broad-except
            logger.error("[merge worker] failed to start timers on %s: %s", thread.name, repr(err))
            self._abort_startup()

    def _stop_accepting(self) -> None:
        """Stop every transport from accepting; open connections keep being served.

        UDP endpoints stay open: there is no connection to refuse, and
        in-flight datagrams are answered on them. They are closed with their
        loops.
        """
        self._stop_listening()

    def _stop_serving_threads(self) -> None:
        """Stop every serving thread in parallel under one shared deadline.

        Each thread runs :meth:`_before_stop_service_thread` on its own loop
        first. A thread that does not exit in time is logged and left behind.
        From here on a thread exit is expected, not a death.
        """
        self._stopping_threads = True
        if not self.loop_threads:
            return
        stop_loop_threads(self.loop_threads, self._before_stop_service_thread, LOOP_THREAD_STOP_TIMEOUT)
        self.loop_threads.clear()

    async def _before_stop_service_thread(self, index: int) -> None:
        """Release one serving thread's resources; runs on that thread.

        The timer first (thread 0), so no new timer callback starts while
        the rest is torn down; then the UDP endpoints, the ``on_stop`` hooks,
        the user protocol transports (which may want to finish what the hooks
        flushed), and the aiohttp runners last, since cleaning a runner up
        closes its connections. Handlers still in flight are cancelled
        afterwards, when the loop is shut down.

        Args:
            index: Position of the thread among the serving loops.
        """
        reactor = self.loop_threads[index]
        model = self.thread_model
        if index == 0 and self.timer is not None:
            self.timer.stop()
        await model.release_resources(reactor, ResourceKind.ENDPOINT)
        info, self._thread_infos[index] = self._thread_infos[index], None
        if info is not None:
            await run_stop_hooks(info)
        await self._destroy_transports(self._user_transports.pop(id(reactor), []))
        await model.release_resources(reactor, ResourceKind.RUNNER)

    async def _release_main_loop(self) -> None:
        """Release what was served on the main loop; idempotent.

        User protocol transports first, then aiohttp runners, then the UDP
        endpoints.
        """
        model = self.thread_model
        if model is None:
            return
        main = model.acceptor
        await self._destroy_transports(self._user_transports.pop(id(main), []))
        await model.release_resources(main, ResourceKind.RUNNER)
        await model.release_resources(main, ResourceKind.ENDPOINT)

    def _on_graceful_exit(self) -> None:
        """Stop accepting, then release the main loop and shut the serving loops down.

        Order matters: an acceptor closed after its targets would hand sockets
        to loops that are no longer turning, and those connections would hang
        until the process died rather than being refused.

        When the main loop is the serving loop, its ``on_stop`` thread hooks
        run next, then what it served is released. The loop has stopped by
        now, so ``run_until_complete`` is allowed again. Every step takes what
        it releases out of ``self`` first, so a second call does nothing.
        """
        self._stop_listening()
        if self._main_thread_info is not None:
            info, self._main_thread_info = self._main_thread_info, None
            self.loop.run_until_complete(run_stop_hooks(info))
        self.loop.run_until_complete(self._release_main_loop())

        self._stop_serving_threads()
