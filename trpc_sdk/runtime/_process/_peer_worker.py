# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Worker of ``peer`` mode: N equal processes, each a complete single-loop server.

The process the application started is worker-0. Before its event loop exists
it forks worker-1 .. worker-(N-1); every one of them then binds every listen
address itself with ``SO_REUSEPORT`` (TCP, UDP, HTTP and user protocols
alike), so the kernel, not a parent, spreads connections and datagrams over
them. Nothing is forwarded between processes. Inside, each worker is merge
mode's single-loop form: :class:`PeerWorker` is a
:class:`~trpc_sdk.runtime._merge_worker.MergeWorker` with one serving loop and
``SO_REUSEPORT`` forced on, so the listeners and the thread hooks are merge
mode's.

Startup. worker-0 creates one ready flag per worker before forking (see
:mod:`~trpc_sdk.runtime._common._readiness`). Each worker runs its ``on_start`` hooks
with ``index`` = its worker index and ``count`` = ``worker_num``, binds,
starts its timers (``worker:<index>``) and sets its flag. worker-0 sets its
own once it is bound, waits for the others, and only then runs
``ready_callbacks``, so the naming service is told once, after every worker
serves. Any failure stops every child and exits with status 1.

Shutdown. worker-1 .. worker-(N-1) are controlled children: they ignore
SIGTERM and SIGINT, so a signal sent to the whole process group only reaches
worker-0, and they stop on SIGUSR1 (see :mod:`~trpc_sdk.runtime._common._shutdown`).
worker-0 runs the exit template: deregister, close its listeners and send the
children their first SIGUSR1 (each closes its own and drains
``close_wait_time`` alongside), drain (a second SIGTERM cuts it short and
sends SIGUSR1 again), post-drain callbacks, wait for the children or SIGKILL
them, ``master_handle``, then its own ``worker_handle`` and ``on_stop``.

Not supported yet: restarting one worker. A worker that dies outside shutdown
takes the whole tree down with status 1, for the supervisor to restart, as in
the other modes. Tasks are rejected before the fork.
"""

from __future__ import annotations

import sys
import traceback
from functools import partial
from multiprocessing import Process
from typing import Any
from typing import List

from trpc_sdk.config import default_worker_num
from trpc_sdk.log import flush_logs
from trpc_sdk.log import logger
from trpc_sdk.plugin import get_registered_tasks
from trpc_sdk.telemetry import report_attr
from trpc_sdk.transport import has_custom_scheduler
from .._common import ChildReadiness
from .._common import warn_unnamed_timers
from .._constants import AddressKind
from .._constants import CHILD_EXIT_GRACE_SECONDS
from .._constants import PEER_STARTUP_WARNING
from .._listeners import classify_address
from .._merge_worker import MergeWorker
from .._threadmodel import HandlerThreadInfo
from .._types import ChildProcessInfo


class PeerWorker(MergeWorker):
    """One peer worker's serving side: merge form A with every endpoint on SO_REUSEPORT.

    Every peer worker binds every listen address itself, so the kernel, not
    a parent, spreads connections and datagrams over them. Nothing here
    knows about the other workers: forking and the startup wait live in
    :class:`PeerMainWorker`, the controlled-child setup in
    :class:`PeerWorkerProcess`.

    Attributes:
        index: This worker's position, 0 for the process the application
            started.
        worker_num: Number of peer workers, worker-0 included.
    """

    _main_loop_only = True

    def __init__(self, adapter, index: int, worker_num: int, name: str) -> None:
        """Copy the endpoint tables out of the server adapter; nothing is bound yet.

        Constructed in worker-0 for every worker, before any fork, so nothing
        here may hold a loop or a socket.

        Args:
            adapter: Server adapter holding ``process_info`` and ``opts``.
            index: Position of this worker among the peer workers.
            worker_num: Number of peer workers, worker-0 included.
            name: Process name shown in logs and process listings.
        """
        super().__init__(adapter, name)
        self.index: int = index
        # Timers run as ``worker:<index>``.
        self.worker_id = index
        self.worker_num: int = worker_num
        # Whatever ``server.tcp_reuse_port`` says: every worker binds every
        # address, TCP and UDP alike.
        self.tcp_reuse_port = True

    @staticmethod
    def _resolve_thread_num(opts) -> int:
        """One serving loop, the main one: peer runs no loop threads.

        Args:
            opts: The server adapter's :class:`~trpc_sdk.server.ServerOption`; unused.
        """
        del opts
        return 1

    def _main_loop_thread_info(self) -> HandlerThreadInfo:
        """What ``on_start`` / ``on_stop`` see: this worker's index among all peer workers."""
        return HandlerThreadInfo(index=self.index, count=self.worker_num, loop=self.loop)

    def _on_graceful_exit(self) -> None:
        """Stop the timers, then run merge mode's exit: ``on_stop``, the HTTP runners, the UDP endpoints.

        Runs after the loop stopped. Every step takes what it releases out of
        ``self`` first, so a second call does nothing.
        """
        timer, self.timer = self.timer, None
        if timer is not None:
            timer.stop()
        try:
            super()._on_graceful_exit()
        except Exception as err:  # pylint: disable=broad-except
            logger.error("[%s] exit failed: %s", self.name, repr(err), exc_info=True)


class PeerWorkerProcess(PeerWorker, Process):
    """Worker ``index`` (1 .. worker_num-1), forked by worker-0 before any loop exists.

    Serves exactly like worker-0. It only differs in who decides when it
    stops: it is a controlled child, so SIGTERM and SIGINT are ignored and
    worker-0 sends SIGUSR1 (see :mod:`~trpc_sdk.runtime._common._shutdown`). It
    registers nothing with the naming service: its callback lists are empty,
    since it was constructed in worker-0, not copied from the object
    ``Server.serve`` filled.
    """

    def __init__(self, adapter, index: int, worker_num: int, ready_flags: Any) -> None:
        """Record this child's index and the shared ready flags; runs in worker-0, before fork.

        Args:
            adapter: Server adapter holding ``process_info`` and ``opts``.
            index: Position of this worker, 1 .. ``worker_num`` - 1.
            worker_num: Number of peer workers, worker-0 included.
            ready_flags: worker-0's shared array; this child sets slot ``index``.
        """
        super().__init__(adapter, index, worker_num, f"worker:{index}")
        self._ready_flags = ready_flags
        self._controlled = True
        self.drain_seconds = max(0, int(getattr(adapter.opts, "close_wait_time", 0) or 0)) / 1000.0

    def _init_plugins(self) -> None:
        """Initialize every plugin; exit with status 1 on failure."""
        try:
            super()._init_plugins()
        except Exception as err:  # pylint: disable=broad-except
            self._fail_startup("plugin initialization failed", err)

    def _prepare_to_serve(self) -> None:
        """Wire task IPC, run ``on_start``, bind every address, start timers, then set the ready flag.

        Replaces the base template rather than extending it: the base runs
        ``ready_callbacks``, and only worker-0 registers. A stop signal that
        arrived during startup skips the bind; ``_register_signal_handlers``
        then shuts down at once. ``on_start`` and bind failures exit with
        status 1 from inside ``MergeWorker``, after logging the cause, which
        is flushed here before the process goes; worker-0 sees this child's
        sentinel.
        """
        try:
            self._install_thread_model()
            self._restore_task_mesh_after_fork()
            self._close_unowned_task_pipes()
            self._connect_tasks()
            if self._early_exit:
                return
            self.start_worker_process()
            self._start_timers()
            self._ready_flags[self.index] = 1
        except SystemExit:
            flush_logs()
            raise
        except Exception as err:  # pylint: disable=broad-except
            self._fail_startup("startup failed", err)

    def _fail_startup(self, what: str, err: BaseException) -> None:
        """Log a startup failure and exit with status 1.

        Args:
            what: Which step failed, for the log line.
            err: The exception it raised.
        """
        logger.error("[%s] %s: %r, trace: %s", self.name, what, err, traceback.format_exc())
        flush_logs()
        sys.exit(1)


class PeerMainWorker(ChildReadiness, PeerWorker):
    """Worker-0: the process the application started, serving and supervising.

    Forks worker-1 .. worker-(N-1) before its own loop exists, serves like
    any of them, registers with the naming service once every worker has
    set its ready flag, and orders the shutdown through the exit template
    hooks. A worker that dies outside shutdown brings the whole tree down
    with status 1; restarting one worker is not supported yet.

    Not a controlled child: it stops on SIGTERM, and ``drain_seconds`` and
    the four callback lists are set by ``Server.serve``.
    """

    def __init__(self, adapter, name: str = "worker:0") -> None:
        """Read ``worker_num`` from the server adapter; nothing is forked yet.

        Args:
            adapter: Server adapter holding ``process_info`` and ``opts``.
            name: Process name shown in logs and process listings.
        """
        # Already resolved by ``server._option_resolver``; the fallback only
        # guards options built by hand.
        configured = int(adapter.opts.worker_num)
        super().__init__(adapter, 0, configured if configured > 0 else default_worker_num(), name)
        # worker-1 .. worker-(N-1), in fork order.
        self._worker_children: List[ChildProcessInfo] = []

    def _spawn_children(self) -> None:
        """Validate, create the ready flags, then fork worker-1 .. worker-(N-1).

        The base class runs first for its pre-fork warnings and the (empty)
        task mesh; no task is forked, since tasks were rejected already.
        """
        self._check_before_fork()
        super()._spawn_children()
        self._create_ready_flags(self.worker_num)
        for index in range(1, self.worker_num):
            self._spawn(f"worker:{index}",
                        partial(PeerWorkerProcess, self.server_adapter, index, self.worker_num, self._ready_flags))
        self._worker_children = list(self.children)

    def _check_before_fork(self) -> None:
        """Fail on what peer mode cannot serve, warn about what behaves differently; nothing is forked yet.

        Raises:
            ValueError: A Task is registered, or an address matches no listener.
        """
        tasks = [task.name for task in get_registered_tasks()]
        if tasks:
            logger.warning("[%s] peer mode does not support Task; registered: %s", self.name, tasks)
            raise ValueError(f"peer mode does not support Task (registered: {tasks})")
        for i, address in enumerate(self.addresses):
            if classify_address(self, i) is AddressKind.UNKNOWN:
                raise ValueError(f"peer mode: unsupported network {self.networks[i]!r} for address {address}")
        warn_unnamed_timers(self.name, self.worker_num, "peer workers")
        if has_custom_scheduler():
            logger.warning("[%s] register_dispatcher_worker_func only applies to separate mode; ignored in peer mode",
                           self.name)
        logger.warning(PEER_STARTUP_WARNING, self.worker_num)

    def _wait_until_tasks_ready(self) -> None:
        """Ignore ``io_wait_count``: peer mode has no task to notify it."""
        if self.io_wait_count:
            logger.warning("[%s] io_wait_count=%s ignored: peer mode has no Task", self.name, self.io_wait_count)
            self.io_wait_count = 0

    def _init_plugins(self) -> None:
        """Initialize every plugin; on failure stop the children and exit with status 1."""
        try:
            super()._init_plugins()
        except Exception as err:  # pylint: disable=broad-except
            logger.error("[%s] plugin initialization failed: %s", self.name, repr(err), exc_info=True)
            self._exit_now(1)

    def _prepare_to_serve(self) -> None:
        """Run the base template; on an unexpected failure stop the children and exit with status 1.

        Base order: task wiring (empty), ``on_start(0)``,
        :meth:`start_worker_process` (bind, then wait for every worker),
        ``ready_callbacks`` (naming registration), timers (``worker:0``).
        """
        try:
            super()._prepare_to_serve()
        except Exception as err:  # pylint: disable=broad-except
            logger.error("[%s] startup failed: %s", self.name, repr(err), exc_info=True)
            self._exit_now(1)

    def start_worker_process(self) -> None:
        """Bind every address, set this worker's flag, then wait for every other worker's.

        Returns only once every worker serves, so the ``ready_callbacks``
        that follow never advertise a half-started server. A bind failure
        exits through ``_abort_startup``; a failed wait through ``_exit_now``.
        """
        super().start_worker_process()
        self._ready_flags[0] = 1
        elapsed = self._wait_children_ready(self._worker_children)
        logger.info("[%s] peer: %s workers bound in %.0f ms", self.name, self.worker_num, elapsed * 1000)

    def _stop_accepting(self) -> None:
        """Close this worker's listeners, then send the others their first SIGUSR1.

        Each of them then closes its own listeners and drains
        ``close_wait_time`` alongside this process.
        """
        super()._stop_accepting()
        self._signal(self._worker_children)

    def _on_drain_cut(self) -> None:
        """Send the other workers another SIGUSR1, so they stop waiting out ``close_wait_time`` too."""
        self._signal(self._worker_children)

    async def _wait_handlers(self) -> None:
        """Wait for the other workers to exit, SIGKILL them after ``CHILD_EXIT_GRACE_SECONDS``.

        They drained alongside this process, so the grace period only covers
        their ``worker_handle``, ``on_stop`` and listener cleanup.
        ``master_exit_callbacks`` run after this, so ``master_handle`` always
        runs after every other worker's ``worker_handle``; this worker's own
        runs after ``master_handle``, as in merge mode.
        """
        await self._wait_children(self._worker_children, CHILD_EXIT_GRACE_SECONDS)

    def _on_graceful_exit(self) -> None:
        """Release this worker's listeners, then report the exit so a supervisor can tell it from a crash."""
        super()._on_graceful_exit()
        report_attr("trpc_peer_worker_exit", 1)

    def _on_unhandled_exception(self, err: BaseException) -> None:
        """Report the failure and exit with status 1 instead of re-raising.

        The children were already stopped by :meth:`_stop_children`.

        Args:
            err: The exception that terminated the loop.
        """
        self._exit_code = 1
        report_attr("trpc_peer_worker_err_exit", 1)
        logger.error("[%s] exited with error: %s, trace: %s", self.name, repr(err), traceback.format_exc())
