# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""The worker role: the process that runs user handlers.

In ``merge`` mode the worker binds the public addresses and runs handlers
itself. In ``separate`` mode it only does I/O, and
:class:`~trpc_sdk.runtime._process._rpc_task.RpcTaskProcess` children run the
handlers behind an IPC pipe.

Process-tree contract. The worker the application started is the only
supervisor in the tree; an external supervisor (systemd, Kubernetes, ...)
restarts the whole tree. In return:

* a child that dies makes the worker exit with status 1;
* every forked child exits when its parent does, even on SIGKILL, through
  ``PR_SET_PDEATHSIG`` on Linux and the parent sentinel everywhere;
* ``reboot_child`` is not supported in this version and is treated as false.

Shutdown order and the controlled-child signal rules live in
:mod:`~trpc_sdk.runtime._common._shutdown`.
"""

from __future__ import annotations

import asyncio
import multiprocessing
import os
import signal
import sys
import threading
import time
from functools import partial
from multiprocessing.process import BaseProcess
from typing import TYPE_CHECKING
from typing import Any
from typing import Callable
from typing import Dict
from typing import List
from typing import Optional
from typing import Tuple

from trpc_sdk.abc import BaseServerProtocolParser
from trpc_sdk.abc import BaseServerTransport
from trpc_sdk.abc import BaseServerTransportOption
from trpc_sdk.abc import ProcessType
from trpc_sdk.common import bind_lifetime_to_parent
from trpc_sdk.common import execute_after_in_child
from trpc_sdk.common import execute_before_loop_run_in_child
from trpc_sdk.common import set_current_process_var
from trpc_sdk.config import TrpcConfig
from trpc_sdk.log import logger
from trpc_sdk.plugin import init_worker_plugins
from trpc_sdk.task import WorkerTaskConnection
from trpc_sdk.task import register_task_conn
from trpc_sdk.transport import ListenOption
from trpc_sdk.utils import PipeConnection
from trpc_sdk.utils import TimerExecutor
from trpc_sdk.utils import get_async_lib
from trpc_sdk.utils import io_ready_reached
from ._common import ExitTemplate
from ._common import close_heartbeat_pipes
from ._common import init_healthy_checker
from ._common import start_heartbeat_monitor
from ._common._shutdown import _run_callbacks
from ._constants import IO_WAIT_POLL_INTERVAL
from ._types import ChildRecord
from ._types import TaskInfo
from ._types import WorkerInfo

if TYPE_CHECKING:
    from trpc_sdk._inner import MemoryInspector
    from ._process import TaskIpcMesh


_current_worker: Optional[WorkerInfo] = None


def _raise_system_exit(signum: int, frame: Any) -> None:
    """SIGTERM handler used before the event loop exists; raises ``SystemExit``."""
    del signum, frame
    raise SystemExit(0)


def current_worker() -> Optional[WorkerInfo]:
    """Return the :class:`WorkerInfo` of this process, or ``None`` outside a forked worker."""
    return _current_worker


def set_current_worker(worker: Optional[WorkerInfo]) -> None:
    """Mark this process as running for ``worker``; called right after fork."""
    global _current_worker  # pylint: disable=global-statement
    _current_worker = worker


class BaseWorker(ExitTemplate):
    """The process that serves requests: startup sequence plus worker runtime.

    Not a ``Process`` subclass: a worker is normally the process it runs in.
    Roles that fork write ``class X(BaseWorker, Process)``, with ``BaseWorker``
    first so its ``run()`` wins over ``Process.run()``.

    :meth:`run` is fixed: loop, then plugins, then listener, then serve.
    Subclasses override:

    * :meth:`start_worker_process` — attach the request source (IPC pipe or
      listen sockets). The usual extension point.
    * :meth:`_prepare_to_serve` — replace the whole wiring step; for roles
      without a ``WorkerInfo`` (tasks, dispatcher).
    * :meth:`_init_plugins` — infrastructure roles narrow it to non-business
      plugins.
    * :meth:`_before_event_loop`, :meth:`_start_timers`.
    * The :class:`~trpc_sdk.runtime._common.ExitTemplate` hooks, and
      ``_controlled`` for a child its parent stops with SIGUSR1.
    * :meth:`_on_graceful_exit`, :meth:`_on_unhandled_exception`.

    Attributes:
        type: Process role, used for health checks and restarts.
        name: Process name shown in logs and process listings.
        loop: Event loop created in :meth:`run`; ``None`` until then.
        block: If False, :meth:`run` returns after setup. Used by tests.
    """

    # Only assigned when memory inspection is enabled
    memory_inspector: MemoryInspector

    def __init__(self, name: str, process_type: ProcessType) -> None:
        """Record identity only; no loop, socket or pipe exists yet.

        ``super().__init__()`` takes no arguments on purpose: it reaches
        ``Process.__init__`` for a forked role and ``object.__init__``
        otherwise, so ``self.name`` below sets the process name in the first
        case and a plain attribute in the second.

        Args:
            name: Process name shown in logs and process listings.
            process_type: Role this worker plays in the process tree.
        """
        super().__init__()
        self.type: ProcessType = process_type
        self.name = name
        self.loop: Optional[asyncio.AbstractEventLoop] = None
        self.block: bool = True

        self.server_engine = None
        # Master-side record this worker was forked from; ``None`` for a
        # worker that is the current process.
        self.worker: Optional[WorkerInfo] = None
        # Labels timers and inter-worker messages; 0 for a whole-process worker.
        self.worker_id: int = 0

        # This worker's end of each task pipe, index-aligned with ``task_name``
        # and ``task_infos``; ``None`` for a task it has no pipe to.
        self.task_pipe_conn: List[Optional[PipeConnection]] = []
        self.task_name: List[str] = []
        self.task_infos: List[TaskInfo] = []
        # Every socketpair in the tree; used after the last fork to close the
        # ends this process must not hold.
        self.task_ipc_mesh: Optional[TaskIpcMesh] = None
        self.children: List[ChildRecord] = []
        self._children_by_pid: Dict[int, ChildRecord] = {}
        # Always False, whatever ``server.reboot_child`` says.
        self.reboot_child: bool = False
        # Run when shutdown starts (naming deregistration).
        self.exit_callbacks: List[Callable[[], None]] = []
        # Run once the listen sockets are bound (naming registration), so
        # callers are never pointed at an address nobody listens on yet.
        self.ready_callbacks: List[Callable[[], None]] = []
        self.post_drain_callbacks: List[Callable[[], None]] = []
        # The user server exit callback; order relative to the others is
        # described in ``_common._shutdown``.
        self.master_exit_callbacks: List[Callable[[], None]] = []
        # Seconds in-flight requests get after the listen sockets close.
        self.drain_seconds: float = 0.0
        # Task-ready notifications to wait for before binding; 0 = no wait.
        self.io_wait_count: int = 0

        # Per-endpoint tables, index-aligned; see ``_bind_process_info``.
        self.addresses: List[Tuple[str, int]] = []
        self.socket_types: List[str] = []
        # "tcp" / "udp" / "http", possibly composite such as "tcp,http".
        self.networks: List[str] = []
        # ``None`` where no parser plugin is registered for the protocol.
        self.parsers: List[Optional[BaseServerProtocolParser]] = []
        self.services: List[Any] = []

        self.timer: Optional[TimerExecutor] = None

        # This worker's end of the dispatcher req/rsp pipe and the RPCZ pipe.
        self.worker_pipe_conn: Optional[PipeConnection] = None
        self.rpcz_pipe_conn: Optional[PipeConnection] = None

        self.handlers: List[Callable[..., Any]] = []
        # Idle/request timeout (ms).
        self.timeouts: List[int] = []
        self.tran_opts: List[BaseServerTransportOption] = []
        # ``transports[i].opts`` is ``tran_opts[i]``; ``classify_address``
        # decides which process and loop calls ``listen()`` on each.
        self.transports: List[BaseServerTransport] = []
        # Transports this process listened on, each with the loop whose thread
        # must call its ``stop``. Filled after fork only.
        self._listening: List[Tuple[BaseServerTransport, asyncio.AbstractEventLoop]] = []
        self.enable_memory_inspection: bool = False

    def _bind_process_info(self, engine) -> None:
        """Reference the per-endpoint tables the master filled in ``ServiceAdapter.run``.

        Entry ``i`` of every list describes the same listen endpoint. They are
        referenced, not copied, because they are read-only after fork.

        Args:
            engine: Engine holding ``process_info`` and the task list.
        """
        self.server_engine = engine
        process_info = engine.process_info

        self.addresses = process_info.addresses
        self.socket_types = process_info.socket_types
        self.networks = process_info.networks
        self.parsers = process_info.parsers
        self.handlers = process_info.handlers
        self.timeouts = process_info.timeouts
        self.services = process_info.services
        self.tran_opts = process_info.transport_opt
        self.transports = process_info.transports

        self.enable_memory_inspection = TrpcConfig.server_config().memory_inspection.enabled
        if self.enable_memory_inspection:
            # Lazy import: tracemalloc costs memory even when unused.
            from trpc_sdk._inner import MemoryInspector
            # Writes allocation snapshots to a file on SIGUSR2.
            self.memory_inspector = MemoryInspector(
                TrpcConfig.server_config().memory_inspection.filename_prefix,
                TrpcConfig.server_config().memory_inspection.line_limit,
            )

    def _task_producer_names(self) -> List[str]:
        """Producer names to create task pipes for, including children's.

        Each producer needs its own socketpair per task. A merge worker returns
        one per serving loop; a separate worker one per rpc task process.

        Returns:
            Names in response routing order; a superset of
            :meth:`_owned_producer_names`.
        """
        # Local imports from ``._process`` throughout this class: that
        # package's init imports this module, so it can only be loaded once
        # this module has finished loading.
        from ._process import worker_producer_name
        return [worker_producer_name(0)]

    def _owned_producer_names(self) -> List[str]:
        """Producer names whose pipe ends stay open in this process.

        Everything else :meth:`_task_producer_names` allocated belongs to a
        child and is closed here right after the forks.
        """
        return self._task_producer_names()

    def _spawn_children(self) -> None:
        """Create every IPC pipe in the tree, then fork the children.

        Must run before :meth:`run` creates the loop: an epoll/kqueue
        descriptor does not survive fork. All pipes are created before the
        first fork because a task-to-task pair joins two children, so neither
        of them can create it.
        """
        from ._process import TaskIpcMesh
        from ._process import build_task_infos
        from ._process import set_task_ipc_mesh

        self._warn_unsupported_reboot_child()
        self._warn_threads_before_fork()

        self.task_infos = build_task_infos()
        self.task_name = [task_info.name for task_info in self.task_infos]

        # Heartbeat pipes must exist before fork so the children inherit them.
        init_healthy_checker(self.task_infos)

        mesh = TaskIpcMesh(self.task_infos, self._task_producer_names())
        # Published before fork so children inherit it.
        set_task_ipc_mesh(mesh)
        self.task_ipc_mesh = mesh

        owned = self._owned_producer_names()
        self.task_pipe_conn = mesh.producer_pipes(owned[0]) if owned else [None] * len(self.task_infos)

        self._before_fork_tasks()
        for task_info in self.task_infos:
            self._spawn(task_info.name, partial(task_info.handle, task_info, self.server_engine))

    def _before_fork_tasks(self) -> None:
        """Hook: adjust task records before the tasks are forked. No-op here."""

    def _warn_unsupported_reboot_child(self) -> None:
        """Warn that ``server.reboot_child: true`` is treated as false.

        Not in ``__init__``: every child object is constructed here before
        fork, so the warning would repeat once per child.
        """
        if TrpcConfig.server_config().reboot_child:
            logger.warning("[%s] server.reboot_child is not supported in this version and is treated as false: "
                           "a child that dies takes the whole worker down, for the supervisor to restart",
                           self.name)

    def _warn_threads_before_fork(self) -> None:
        """Warn when other threads are running before fork.

        Fork copies only the calling thread, so a lock another thread holds
        (logging's, for instance) stays locked forever in the child.
        """
        if threading.active_count() <= 1:
            return
        names = [thread.name for thread in threading.enumerate() if thread is not threading.current_thread()]
        logger.warning("[%s] forking children while other threads are running: %s", self.name, names)

    def _spawn(self, name: str, factory: Callable[[], Any]) -> None:
        """Fork one child and remember how to rebuild it.

        Args:
            name: Child name, used in log messages.
            factory: Builds a fresh process object; a ``Process`` cannot be
                started twice.
        """
        record = ChildRecord(name=name, factory=factory)
        self.children.append(record)
        self._start_child(record)

    def _start_child(self, record: ChildRecord) -> None:
        """Build and start the process for one child record."""
        record.process = record.factory()
        record.process.start()
        self._children_by_pid[record.process.pid] = record
        logger.info("[%s] forked child %s as pid %s", self.name, record.name, record.process.pid)

    def _bind_task_mesh(self) -> None:
        """Adopt the pre-fork task mesh in a forked child.

        A child inherits the module-level mesh but not the attributes its
        parent set on itself. Without this it would connect to no task and
        close none of its siblings' pipes. No-op when already bound or no task
        was declared.
        """
        if self.task_ipc_mesh is not None:
            return

        from ._process import task_ipc_mesh

        mesh = task_ipc_mesh()
        if mesh is None:
            return

        self.task_ipc_mesh = mesh
        self.task_infos = mesh.task_infos
        self.task_name = [task_info.name for task_info in mesh.task_infos]
        owned = self._owned_producer_names()
        self.task_pipe_conn = (mesh.producer_pipes(owned[0]) if owned else [None] * len(mesh.task_infos))

    def _close_foreign_pipes(self) -> None:
        """Close the inherited socketpair ends that belong to other processes.

        Holding a child-to-child pipe open here keeps its refcount above zero,
        so its reader would hang instead of seeing EOF when the writer dies.
        """
        if self.task_ipc_mesh is not None:
            self.task_ipc_mesh.close_pipes_not_owned_by(self._owned_producer_names())

    def _watch_children(self) -> None:
        """Install the SIGCHLD handler, only when this worker forked children.

        Otherwise a worker forked by someone else would steal ``SIGCHLD`` from
        its own parent.
        """
        if not self.children:
            return
        self.loop.add_signal_handler(signal.SIGCHLD, self._on_child_exit)

    def _on_child_exit(self) -> None:
        """Reap every exited framework child.

        Polls each known pid because signals do not queue. Never waits on
        ``-1``, which would steal the exit status of children started by user
        code (``subprocess``, asyncio subprocesses).
        """
        for pid in list(self._children_by_pid):
            try:
                reaped, _ = os.waitpid(pid, os.WNOHANG)
            except ChildProcessError:
                # Already collected elsewhere; the child is gone either way.
                reaped = pid
            if reaped:
                self._replace_child(pid)

    def _replace_child(self, pid: int) -> None:
        """Take the whole worker down (status 1) when a child dies.

        A child exiting during shutdown is expected and ignored. The restart
        branch is kept for a later version and unreachable today, since
        ``reboot_child`` is always False.

        Args:
            pid: OS pid reported by ``waitpid``.
        """
        record = self._children_by_pid.pop(pid, None)
        if record is None:
            return
        record.exited = True

        if self._exiting:
            logger.info("[%s] child %s (pid %s) exited during shutdown", self.name, record.name, pid)
            return

        logger.error("[%s] child %s (pid %s) exited unexpectedly", self.name, record.name, pid)
        if not self.reboot_child:
            self._exit_code = 1
            self._handle_sigterm()
            return
        self._start_child(record)

    def run(self) -> None:
        """Turn this process into a serving one, then serve.

        For a forked role this is the first code in the child, which is why the
        loop is created here: an event loop cannot cross a fork.

        The role is stamped onto ``multiprocessing.current_process()`` first.
        For a worker that is the main process, that is what makes
        ``is_worker_process()`` true and the ``WORKER`` child callbacks run.
        """
        if isinstance(self, BaseProcess):
            bind_lifetime_to_parent(self._stop_signal())
        set_current_process_var("type", self.type)
        self._wait_until_tasks_ready()

        self._before_event_loop()

        self.loop = get_async_lib(TrpcConfig.global_config().enable_uvloop).new_event_loop()
        asyncio.set_event_loop(self.loop)

        execute_after_in_child()
        self._init_plugins()
        self._prepare_to_serve()
        execute_before_loop_run_in_child()

        self._serve()

    def _wait_until_tasks_ready(self) -> None:
        """Block until tasks sent ``io_wait_count`` notifications, before any bind.

        Runs before the loop exists, so neither the SIGCHLD nor the SIGTERM
        handler is installed. It covers both itself: a child dying would keep
        the count from ever arriving, and a default-action SIGTERM would orphan
        every child. Either way the children are stopped before exiting.

        Raises:
            SystemExit: SIGTERM arrived (status 0) or a child died (status 1).
        """
        if not self.io_wait_count:
            return

        logger.info("[%s] waiting for %d task notification(s) before listening", self.name, self.io_wait_count)
        in_main_thread = threading.current_thread() is threading.main_thread()
        previous = signal.signal(signal.SIGTERM, _raise_system_exit) if in_main_thread else None
        try:
            while not io_ready_reached(self.io_wait_count):
                dead = [record.name for record in self.children
                        if record.process is not None and not record.process.is_alive()]
                if dead:
                    logger.error("[%s] child %s exited before the tasks were ready", self.name, dead)
                    self._exit_code = 1
                    raise SystemExit(1)
                time.sleep(IO_WAIT_POLL_INTERVAL)
        except SystemExit:
            self._exit_now(self._exit_code)
        finally:
            if in_main_thread:
                signal.signal(signal.SIGTERM, previous if previous is not None else signal.SIG_DFL)

    def _serve(self) -> None:
        """Install signal handlers and run the loop until the process exits.

        The exit template leaves the loop with ``SystemExit``. Exits with
        :attr:`_exit_code` afterwards, so a worker brought down by a dead
        child reports failure.
        """
        try:
            if threading.current_thread() is threading.main_thread():
                # Only the main thread may install signal handlers.
                self._register_signal_handlers()
            self._watch_parent()
            if self.block:
                self.loop.run_forever()
        except (SystemExit, KeyboardInterrupt):
            # Do not call ``loop.stop()`` here: ``run_forever`` already
            # returned, and a pending stop would make the first
            # ``run_until_complete`` in the exit hooks return after one iteration.
            self._on_graceful_exit()
            self._stop_children()
        except Exception as err:  # pylint: disable=broad-except
            self._stop_children()
            self._on_unhandled_exception(err)
        if self._exit_code:
            sys.exit(self._exit_code)

    def _watch_parent(self) -> None:
        """Shut down when the parent dies, via the ``multiprocessing`` sentinel.

        The portable half of the parent-death binding (``PR_SET_PDEATHSIG`` is
        the Linux half). Known limit: a sibling forked later inherits the write
        end of this sentinel, so children go down one after another, not at
        once. No-op in a process ``multiprocessing`` did not fork.
        """
        if not isinstance(self, BaseProcess):
            return
        parent = multiprocessing.parent_process()
        if parent is None or parent.sentinel is None:
            return
        self.loop.add_reader(parent.sentinel, self._on_parent_exit, parent.sentinel)

    def _on_parent_exit(self, sentinel: int) -> None:
        """Event callback: the parent died, so shut down without draining.

        A daemon timer kills the process after
        :data:`~trpc_sdk.runtime._constants.PARENT_EXIT_GRACE_SECONDS` in case
        shutdown never completes, e.g. a task stuck inside inference.
        """
        self.loop.remove_reader(sentinel)
        logger.error("[%s] parent process exited; shutting down", self.name)
        self._arm_exit_killer()
        self._cut_now()

    def _supervise(self) -> None:
        """Run the loop of a worker that only watches its children."""
        logger.info("[%s] supervising %d child process(es)", self.name, len(self.children))
        self._serve()

    def _on_graceful_exit(self) -> None:
        """Hook: release resources after the loop stopped on a normal exit.

        May be called twice (``SystemExit`` then ``KeyboardInterrupt``), so an
        override must take what it releases out of ``self`` first.
        """

    def _on_unhandled_exception(self, err: BaseException) -> None:
        """Handle an exception that escaped the event loop; re-raises by default.

        Args:
            err: The exception that terminated the loop.
        """
        logger.error("[%s] exited with error: %s", self.name, repr(err))
        raise err

    async def _create_pipe_conn(self,
                                pipe_conn: PipeConnection,
                                task_info: TaskInfo,
                                loop: Optional[asyncio.AbstractEventLoop] = None) -> None:
        """Attach this worker's end of a task pipe to a loop and register it.

        A coroutine because a merge worker calls it from inside an already
        running serving loop, where ``run_until_complete`` would refuse.

        Args:
            pipe_conn: This worker's ``PipeEnd.RIGHT`` end of the producer <->
                task socketpair. Must not be ``None``.
            task_info: The task this pipe talks to.
            loop: Loop that will own the transport, and the only one allowed
                to write it. Defaults to the main loop.
        """
        loop = loop or self.loop
        pipe_conn.close_extra_conn()
        coros = loop.create_connection(WorkerTaskConnection, sock=pipe_conn.get_socket())
        (transport, _) = await coros
        register_task_conn(task_info.name, transport)

    def _before_event_loop(self) -> None:
        """Take over the signals of a controlled child, then start tracemalloc.

        A controlled child ignores SIGTERM/SIGINT so a process-group signal
        cannot stop it behind its parent's back; its stop signal is only
        recorded until the loop handler exists. Known gap: between fork and
        here the default disposition still applies.

        tracemalloc starts here so it sees loop and plugin setup allocations.
        """
        if self._controlled:
            signal.signal(signal.SIGTERM, signal.SIG_IGN)
            signal.signal(signal.SIGINT, signal.SIG_IGN)
            signal.signal(self._stop_signal(), self._record_early_exit)
        if self.enable_memory_inspection:
            self.memory_inspector.start_tracing()

    def _init_plugins(self) -> None:
        """Initialize every plugin, business ones included, since user handlers run here."""
        init_worker_plugins()

    def _prepare_to_serve(self) -> None:
        """Wire task IPC, the request source, then timers. The order matters.

        1. Publish ``current_worker()`` when forked from a :class:`WorkerInfo`.
        2. Drop foreign pipe ends; attach or drop the heartbeat pipes.
        3. Connect to each task.
        4. :meth:`start_worker_process` — attach the pipe or bind sockets.
        5. ``ready_callbacks`` — addresses are reachable now, so naming
           registration is truthful from here on.
        6. :meth:`_start_timers`.
        """
        self._publish_current_worker()

        self._bind_task_mesh()
        self._close_foreign_pipes()
        self._attach_heartbeats()
        self._connect_tasks()

        self.start_worker_process()
        _run_callbacks(self.name, "ready", self.ready_callbacks)
        self._start_timers()

    def _attach_heartbeats(self) -> None:
        """Monitor the tasks this process forked, or close pipes it only inherited.

        A non-forking child closes both inherited ends so a dead task's pipe
        actually reaches EOF. Task processes start their sender from a ``TASK``
        child callback instead.
        """
        if self.children:
            start_heartbeat_monitor(self.loop)
        else:
            close_heartbeat_pipes()

    def _start_timers(self) -> None:
        """Start this process's timer executor, labelled ``worker:<id>``.

        A process that runs no business code overrides it with a no-op, so an
        untargeted timer runs once per handler process only.
        """
        self.timer = TimerExecutor(loop=self.loop, name=f"worker:{self.worker_id}")
        self.timer.run()

    def _publish_current_worker(self) -> None:
        """Make ``current_worker()`` return this worker's record, if it has one."""
        if self.worker is None:
            return
        self.worker.process_instance = self
        set_current_worker(self.worker)

    @property
    def task_records(self) -> List[TaskInfo]:
        """Task records index-aligned with ``task_pipe_conn``."""
        return self.task_infos

    def _connect_tasks(self) -> None:
        """Connect every task on the main loop; overridden by multi-loop workers."""
        self.loop.run_until_complete(self._connect_tasks_on(self.loop))

    async def _connect_tasks_on(self,
                                loop: asyncio.AbstractEventLoop,
                                pipe_conns: Optional[List[Optional[PipeConnection]]] = None) -> None:
        """Connect every task from one serving loop.

        ``None`` entries are tasks this producer has no pipe to, which a
        ``Task(peers=[...])`` declaration can create.

        Args:
            loop: Loop that will own the transports; handlers may only produce
                to these tasks from it.
            pipe_conns: Producer ends to attach. Defaults to
                ``task_pipe_conn``; a merge worker passes the set for the
                serving loop being set up.
        """
        records = self.task_records
        if pipe_conns is None:
            pipe_conns = self.task_pipe_conn
        for i, pipe_conn in enumerate(pipe_conns):
            task_info: TaskInfo = records[i]
            if pipe_conn is not None:
                await self._create_pipe_conn(task_info=task_info, pipe_conn=pipe_conn, loop=loop)

    def _register_signal_handlers(self) -> None:
        """Install the stop signal, SIGUSR2 (memory inspection) and SIGCHLD.

        A stop signal recorded during startup now shuts down without draining,
        since nothing was in flight yet.
        """
        self.loop.add_signal_handler(self._stop_signal(), self._on_stop_signal)
        if self._early_exit:
            logger.warning("[%s] told to stop during startup", self.name)
            self._cut_now()
        if self.enable_memory_inspection:
            self.loop.add_signal_handler(signal.SIGUSR2, self.memory_inspector.dump_to_file)
        self._watch_children()

    def _listen_on(self, index: int, opt: ListenOption) -> BaseServerTransport:
        """Call ``listen`` on the transport of address ``index`` and remember it.

        Recorded before the call, so a ``listen`` that fails halfway still
        gets its ``stop`` when startup is undone.

        Args:
            index: Position of the address in ``transports``.
            opt: Options for this call; ``opt.loop`` is where ``stop`` runs.

        Returns:
            The transport, so the caller can release its per-loop resources.
        """
        transport = self.transports[index]
        self._listening.append((transport, opt.loop))
        transport.listen(opt)
        return transport

    def _stop_listening(self) -> None:
        """Call ``stop`` on every listened transport, on its own loop; idempotent."""
        listening, self._listening = self._listening, []
        for transport, loop in listening:
            try:
                if loop is self.loop:
                    transport.stop()
                else:
                    loop.call_soon_threadsafe(transport.stop)
            except Exception as err:  # pylint: disable=broad-except
                logger.error("[%s] failed to stop %s: %s", self.name, type(transport).__name__, repr(err))

    async def _destroy_transports(self, transports: List[BaseServerTransport]) -> None:
        """Await ``destroy`` of each transport in turn; failures are only logged."""
        for transport in transports:
            try:
                await transport.destroy()
            except Exception as err:  # pylint: disable=broad-except
                logger.error("[%s] failed to destroy %s: %s", self.name, type(transport).__name__, repr(err))

    def start_worker_process(self) -> None:
        """Connect this worker to whatever feeds it requests; no-op here.

        :class:`~trpc_sdk.runtime._process._rpc_task.RpcTaskProcess` attaches
        to its pipe; :class:`~trpc_sdk.runtime._merge_worker.MergeWorker`
        binds its listen sockets.
        """
        return None
