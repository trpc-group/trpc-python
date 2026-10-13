# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""The worker role: the process that runs user handlers.

A worker is whatever process ends up executing service handlers, whether or
not it also owns a listen socket. In ``merge`` mode it binds the public
addresses itself and runs handlers in the same process. In ``separate`` mode
the worker only does I/O; :class:`~trpc_sdk.server._separate_worker.RpcTaskProcess`
children decode frames, run handlers and write responses back on an IPC pipe.

This module provides:

* :class:`BaseWorker` — the startup sequence every serving process follows,
  plus the runtime it needs on the other side of it: task connections, the
  inter-worker message pipe, timers, and the mapping from listen port to
  service handler. **Not** a ``Process`` subclass, because a worker is
  normally the process it runs in; roles that do fork list
  ``multiprocessing.Process`` beside it, with ``BaseWorker`` first so its
  ``run()`` wins.
* :class:`~trpc_sdk.server._types.WorkerInfo` — the pre-fork record for one
  worker, filled with its id and its end of every IPC pipe. ``merge`` and
  ``separate`` workers own their process outright and do not consume one;
  :func:`current_worker` and :func:`trpc_sdk.server.send_msg_worker` still
  read it when a custom mode does.

Process-tree contract. The worker the application started is the only
supervisor in the tree, and nothing supervises it: an external supervisor
(systemd, Kubernetes, ...) has to restart the whole tree. In return:

* a child that dies makes the worker exit with status 1, not 0, so a
  supervisor that restarts only on failure notices;
* every child the framework forks exits when its parent does, even when the
  parent was killed with SIGKILL: through ``PR_SET_PDEATHSIG`` on Linux,
  and through the parent sentinel watched on the child's loop everywhere;
* restarting a single child (``reboot_child``) is not supported in this
  version and is treated as ``false``.
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
from trpc_sdk.abc import BaseServerTransportOption
from trpc_sdk.abc import ProcessType
from trpc_sdk.common import bind_lifetime_to_parent
from trpc_sdk.common import execute_after_in_child
from trpc_sdk.common import execute_before_loop_run_in_child
from trpc_sdk.common import set_current_process_var
from trpc_sdk.config import TrpcConfig
from trpc_sdk.log import logger
from trpc_sdk.plugin import init_worker_plugins
from trpc_sdk.task import register_task_conn
from trpc_sdk.transport import WorkerMessageConnection
from trpc_sdk.transport import WorkerTaskConnection
from trpc_sdk.utils import TimerExecutor
from trpc_sdk.utils import get_async_lib
from trpc_sdk.utils import io_ready_reached
from trpc_sdk.utils import PipeConnection
from ._constants import IO_WAIT_POLL_INTERVAL
from ._constants import PARENT_EXIT_GRACE_SECONDS
from ._signals import run_worker_signal_handle
from ._types import ChildRecord
from ._types import TaskInfo
from ._types import WorkerInfo

if TYPE_CHECKING:
    from trpc_sdk._inner import MemoryInspector
    from ._task import TaskIpcMesh


_current_worker: Optional[WorkerInfo] = None


def _raise_system_exit(signum: int, frame: Any) -> None:
    """``signal.signal`` handler that turns SIGTERM into ``SystemExit``.

    Only used before the event loop exists, when the loop's own handler
    cannot be installed yet.

    Args:
        signum: Signal number, unused.
        frame: Interrupted frame, unused.
    """
    del signum, frame
    raise SystemExit(0)


def _run_callbacks(owner: str, kind: str, callbacks: List[Callable[[], None]]) -> None:
    """Run lifecycle callbacks in order, logging failures instead of raising.

    One failing callback must not stop the rest, nor keep the process alive
    (or keep it from becoming ready).

    Args:
        owner: Process name, for log messages.
        kind: Which list this is, for log messages.
        callbacks: Zero-argument callables to run.
    """
    for callback in callbacks:
        try:
            callback()
        except Exception as err:  # pylint: disable=broad-except
            logger.error("[%s] %s callback failed: %s", owner, kind, repr(err))


def current_worker() -> Optional[WorkerInfo]:
    """Return the record of the worker running this code.

    Returns:
        The :class:`WorkerInfo` this process was forked for, or ``None`` when
        called outside a worker process (the master, a task or a dispatcher).
    """
    return _current_worker


def set_current_worker(worker: Optional[WorkerInfo]) -> None:
    """Mark this process as running for ``worker``; called right after fork.

    Args:
        worker: Record of the worker this process serves.
    """
    global _current_worker  # pylint: disable=global-statement
    _current_worker = worker


class BaseWorker:
    """The process that serves requests: startup sequence plus worker runtime.

    Not a ``Process`` subclass. A worker *is* the process it runs in, so the
    only thing it needs is a startup sequence that turns a bare process into a
    serving one. Roles that additionally have to be forked — every task, and
    for now every worker the master spawns — list ``Process`` beside this
    class: ``class X(BaseWorker, Process)``. ``BaseWorker`` has to come first
    so its ``run()`` wins over ``Process.run()``, which would otherwise try to
    call a ``target`` that was never set.

    :meth:`run` is that sequence, and it is deliberately fixed: the event loop
    has to exist before plugins are initialized, plugins before the listener
    is attached, and the listener before the loop starts turning. Subclasses
    fill in the parts that differ:

    * :meth:`start_worker_process` — attach the request source. The IPC pipe
      for :class:`~trpc_sdk.server._separate_worker.RpcTaskProcess`, the public
      listen sockets for :class:`~trpc_sdk.server._merge_worker.MergeWorker`.
      This is the usual extension point, because :meth:`_prepare_to_serve`
      already wires everything behind it.
    * :meth:`_prepare_to_serve` — replace the whole wiring step. Roles that
      are not fed by a ``WorkerInfo`` (tasks, the dispatcher) override this
      instead of :meth:`start_worker_process`.
    * :meth:`_init_plugins` — the default initializes *every* plugin, because
      this process runs user handlers. Infrastructure roles narrow it to the
      non-business set.
    * :meth:`_before_event_loop` — work that must happen before a loop exists.
    * :meth:`_start_timers` — the per-process timer executor. A process that
      runs no business code turns it off.
    * :meth:`_stop_accepting` — close the listen sockets when shutdown starts,
      so in-flight requests can drain while no new connection arrives.
    * :meth:`_on_drained` — run the user worker-exit callbacks once the drain
      is over. A process that runs no business code turns it off.
    * :meth:`_on_sigterm_received`, :meth:`_on_graceful_exit`,
      :meth:`_on_unhandled_exception` — the exit lifecycle hooks.

    Attributes:
        type: Process role, used for health checks and restarts.
        name: Process name shown in logs and process listings.
        loop: Event loop created in :meth:`run`; ``None`` until then.
        block: If False, :meth:`run` returns once setup is done instead of
            serving forever. Tests use this to exercise setup on its own.
    """

    # Only assigned when ``enable_memory_inspection`` is set; the attribute is
    # absent otherwise. Declared without a value so the class keeps that
    # behaviour instead of gaining an always-present ``None``.
    memory_inspector: MemoryInspector

    def __init__(self, name: str, process_type: ProcessType) -> None:
        """Record this worker's identity; no loop, socket or pipe exists yet.

        ``super().__init__()`` is called without arguments on purpose: for a
        forked role it reaches ``Process.__init__``, and for a worker that is
        the current process it is a plain ``object.__init__``. Assigning
        ``self.name`` afterwards therefore sets the process name in the first
        case and a plain attribute in the second.

        The request-serving fields below stay empty until
        :meth:`_bind_process_info` copies them from the engine; a role that
        has no ``WorkerInfo`` keeps the defaults and overrides
        :meth:`_prepare_to_serve`.

        Args:
            name: Process name shown in logs and process listings.
            process_type: Role this worker plays in the process tree.
        """
        super().__init__()
        self.type: ProcessType = process_type
        self.name = name
        self.loop: Optional[asyncio.AbstractEventLoop] = None
        self.block: bool = True

        # Master engine that owns the task list and ``on_worker_message``.
        self.server_engine = None
        # Master-side record for this worker (id, pipes, ``process_instance``).
        # ``current_worker()`` returns this object after fork. ``None`` for a
        # worker that is the current process and was never forked from a
        # ``WorkerInfo``.
        self.worker: Optional[WorkerInfo] = None
        # Id used to label this worker's timers and inter-worker messages.
        # Copied from ``WorkerInfo.id`` when there is one; a worker that is
        # the whole process is worker 0.
        self.worker_id: int = 0

        # This worker's end of each task IPC pipe; index-aligned with
        # ``task_name`` and ``task_records``. ``None`` for a task this process
        # has no pipe to.
        self.task_pipe_conn: List[Optional[PipeConnection]] = []
        # Task names paired with ``task_pipe_conn``.
        self.task_name: List[str] = []
        # Task records this worker built itself in ``_spawn_children``.
        self.task_infos: List[TaskInfo] = []
        # Every socketpair in the process tree; built in ``_spawn_children``
        # and used after the last fork to drop the ends we must not hold.
        self.task_ipc_mesh: Optional[TaskIpcMesh] = None
        # Children this worker forked, in fork order.
        self.children: List[ChildRecord] = []
        # OS pid -> the record that produced it, for SIGCHLD lookups.
        self._children_by_pid: Dict[int, ChildRecord] = {}
        # Whether a dead child is replaced or brings the whole worker down.
        # Always False: restarting one child is not supported yet, whatever
        # ``server.reboot_child`` says. ``_spawn_children`` warns about it.
        self.reboot_child: bool = False
        # Callables run on SIGTERM before the user callbacks, in registration
        # order. This is where the server puts service deregistration, so the
        # process leaves the naming service before it stops answering.
        self.exit_callbacks: List[Callable[[], None]] = []
        # Callables run once the listen sockets are bound, in registration
        # order. This is where the server puts naming registration, so callers
        # are never pointed at an address nobody listens on yet.
        self.ready_callbacks: List[Callable[[], None]] = []
        # Callables run after the drain period, right before the process exits.
        self.post_drain_callbacks: List[Callable[[], None]] = []
        # Seconds in-flight requests get to finish after SIGTERM, with the
        # listen sockets already closed. ``0`` exits at once.
        self.drain_seconds: float = 0.0
        # Task-ready notifications to wait for before binding any address;
        # ``0`` means do not wait. Only set on the process that listens.
        self.io_wait_count: int = 0
        # True once SIGTERM handling started; a second SIGTERM exits at once
        # and a dying child is no longer treated as a fault.
        self._exiting: bool = False
        # True once ``_on_drained`` ran, so a second SIGTERM during the drain
        # does not run the user worker-exit callbacks twice.
        self._drained: bool = False
        # Status this process exits with. Set to 1 when it goes down because a
        # child died or never became ready, so a supervisor sees a failure.
        self._exit_code: int = 0

        # Public listen addresses as ``(host, port)``; parallel with handlers.
        self.addresses: List[Tuple[str, int]] = []
        # Socket type (TCP/UDP/UNIX) for each listen address.
        self.socket_types: List[str] = []
        # Network protocol ("tcp" / "udp" / "http", possibly composite such as
        # "tcp,http") for each listen address. Only used by workers that bind
        # the addresses themselves.
        self.networks: List[str] = []
        # Frame checker/parser for each listen address. ``None`` where no
        # checker plugin is registered for the service protocol.
        self.checkers: List[Optional[BaseServerProtocolParser]] = []
        # Service adapter for each listen address; carries the protocol and
        # service name.
        self.services: List[Any] = []
        # Listen port -> service index. Dispatcher-tagged frames carry the
        # listen port; the worker uses this map to pick handler and timeout.
        # The first index wins if two services share a port.
        self.addresses_map: Dict[int, int] = {}

        # Timer executor started after fork; drives worker timers.
        self.timer: Optional[TimerExecutor] = None

        # asyncio protocol for the inter-worker message pipe; set in
        # ``start_message_listener``.
        self.message_pipe_protocol: Optional[asyncio.Protocol] = None
        # asyncio transport for that same message pipe.
        self.message_pipe_transport: Optional[asyncio.BaseTransport] = None
        # This worker's end of the dispatcher req/rsp IPC pipe.
        self.worker_pipe_conn: Optional[PipeConnection] = None
        # This worker's end of the RPCZ span side-channel pipe.
        self.rpcz_pipe_conn: Optional[PipeConnection] = None

        # Request handlers for each listen address.
        self.handlers: List[Callable[..., Any]] = []
        # Idle/request timeout (ms) for each listen address.
        self.timeouts: List[int] = []
        # Transport options for each listen address (TLS, unix path, ...).
        self.tran_opts: List[BaseServerTransportOption] = []
        # Whether tracemalloc-based allocation snapshots are enabled.
        self.enable_memory_inspection: bool = False

    def _bind_process_info(self, engine) -> None:
        """Copy the per-endpoint tables the master filled during ``Service.run``.

        Every list on ``engine.process_info`` is index-aligned: entry ``i``
        of ``addresses``, ``networks``, ``handlers`` and the rest all describe
        the same listen endpoint. They are referenced, not copied, because they
        are read-only after fork.

        Independent of :class:`WorkerInfo`, so a worker that owns its process
        outright can bind endpoints without pretending to be a forked child.

        Args:
            engine: Engine holding ``process_info`` and the task list.
        """
        self.server_engine = engine
        process_info = engine.process_info

        self.addresses = process_info.addresses
        self.socket_types = process_info.socket_types
        self.networks = process_info.networks
        self.checkers = process_info.checkers
        self.handlers = process_info.handlers
        self.timeouts = process_info.timeouts
        self.services = process_info.services
        self.tran_opts = process_info.transport_opt

        if process_info.addresses is not None:
            for i, address in enumerate(process_info.addresses):
                # First index wins when two services share a listen port.
                if address[1] not in self.addresses_map:
                    self.addresses_map[address[1]] = i

        self.enable_memory_inspection = TrpcConfig.server_config().memory_inspection.enabled
        if self.enable_memory_inspection:
            # Import lazily: ``MemoryInspector`` pulls in tracemalloc and
            # its hash tables, which cost memory even when unused.
            from trpc_sdk._inner import MemoryInspector
            # Writes allocation snapshots to a file on SIGUSR2.
            self.memory_inspector = MemoryInspector(
                TrpcConfig.server_config().memory_inspection.filename_prefix,
                TrpcConfig.server_config().memory_inspection.line_limit,
            )

    def _task_producer_names(self) -> List[str]:
        """Producer names to create task pipes for, including children's.

        A producer is anything that may send work to a task, and each one
        needs its own socketpair per task. The default is a single name for
        this process. A merge worker returns one per serving loop; a separate
        worker returns one per rpc task process, because there it is the rpc
        tasks and not the worker that call tasks.

        Returns:
            Names in the order responses will be routed back on. Must be a
            superset of :meth:`_owned_producer_names`.
        """
        from ._task import worker_producer_name  # local: _task imports this module
        return [worker_producer_name(0)]

    def _owned_producer_names(self) -> List[str]:
        """Producer names whose pipe ends stay open in *this* process.

        Everything :meth:`_task_producer_names` allocated but this does not
        claim belongs to a child, and is closed here right after the forks.

        Returns:
            Names this process keeps. Empty for a worker that forks producers
            but never produces itself.
        """
        return self._task_producer_names()

    def _spawn_children(self) -> None:
        """Create every IPC pipe in the tree, then fork the children.

        Called by the server after the endpoint tables are filled and before
        :meth:`run`, so it runs while this process still has no event loop.
        That ordering is required, not incidental: an epoll/kqueue descriptor
        does not survive fork, and a child that inherited one would share the
        parent's readiness state.

        Pipes are all created before the first fork because a task-to-task
        pair joins two children, so neither of them can create it.
        """
        # Local import: ``_task`` needs ``BaseWorker`` from this module, so the
        # dependency can only run one way at import time.
        from ._healthy_checker import init_healthy_checker
        from ._task import TaskIpcMesh
        from ._task import build_task_infos
        from ._task import set_task_ipc_mesh

        self._warn_unsupported_reboot_child()
        self._warn_threads_before_fork()

        self.task_infos = build_task_infos()
        self.task_name = [task_info.name for task_info in self.task_infos]

        # Heartbeat pipes must exist before fork so the children inherit them.
        # This process is the one watching, so it is not in the watched list.
        init_healthy_checker(self.task_infos)

        mesh = TaskIpcMesh(self.task_infos, self._task_producer_names())
        # Published before the fork so children inherit it and can work out
        # which descriptors are theirs.
        set_task_ipc_mesh(mesh)
        self.task_ipc_mesh = mesh

        owned = self._owned_producer_names()
        self.task_pipe_conn = mesh.producer_pipes(owned[0]) if owned else [None] * len(self.task_infos)

        for task_info in self.task_infos:
            self._spawn(task_info.name, partial(task_info.handle, task_info, self.server_engine))

    def _warn_unsupported_reboot_child(self) -> None:
        """Warn once that ``server.reboot_child: true`` is treated as false.

        Called from :meth:`_spawn_children` rather than ``__init__``: every
        child object is constructed in this process before it forks, and
        warning from the constructor would repeat the message once per child.
        """
        if TrpcConfig.server_config().reboot_child:
            logger.warning("[%s] server.reboot_child is not supported in this version and is treated as false: "
                           "a child that dies takes the whole worker down, for the supervisor to restart",
                           self.name)

    def _warn_threads_before_fork(self) -> None:
        """Warn when this process already runs other threads before forking.

        Fork copies only the calling thread. A lock another thread holds at
        that moment (logging's, for instance) stays locked forever in the
        child. Only a warning, since a third-party import may start a thread
        this process cannot do anything about.
        """
        if threading.active_count() <= 1:
            return
        names = [thread.name for thread in threading.enumerate() if thread is not threading.current_thread()]
        logger.warning("[%s] forking children while other threads are running: %s", self.name, names)

    def _spawn(self, name: str, factory: Callable[[], Any]) -> None:
        """Fork one child and remember how to rebuild it.

        Args:
            name: Child name, used in log messages.
            factory: Builds a fresh child process object. Called again on
                restart, since a ``Process`` cannot be started twice.
        """
        record = ChildRecord(name=name, factory=factory)
        self.children.append(record)
        self._start_child(record)

    def _start_child(self, record: ChildRecord) -> None:
        """Build and start the process for one child record.

        Args:
            record: Record to fill in; its previous ``process``, if any, has
                already exited.
        """
        record.process = record.factory()
        record.process.start()
        self._children_by_pid[record.process.pid] = record
        logger.info("[%s] forked child %s as pid %s", self.name, record.name, record.process.pid)

    def _bind_task_mesh(self) -> None:
        """Adopt the pre-fork task mesh when this process did not build one.

        The process that forks builds the mesh in :meth:`_spawn_children` and
        already holds it. A forked child inherits the same object through the
        module-level reference, but not the attributes its parent set on
        *itself*, so it has to look the mesh up and pick out the column
        belonging to its own producer identity.

        Without this a child has no pipe list at all: it would connect to no
        task, and, worse, close none of the pipes belonging to its siblings.

        Does nothing when the mesh is already bound or no task was declared.
        """
        if self.task_ipc_mesh is not None:
            return

        # Local import: ``_task`` needs ``BaseWorker`` from this module.
        from ._task import task_ipc_mesh

        mesh = task_ipc_mesh()
        if mesh is None:
            return

        self.task_ipc_mesh = mesh
        self.task_infos = mesh.task_infos
        self.task_name = [task_info.name for task_info in mesh.task_infos]
        owned = self._owned_producer_names()
        self.task_pipe_conn = (mesh.producer_pipes(owned[0]) if owned else [None] * len(mesh.task_infos))

    def _close_foreign_pipes(self) -> None:
        """Close the socketpair ends that belong to this worker's children.

        Fork copied every descriptor. Holding a child-to-child pipe open here
        would keep the kernel's reference count above zero, so its reader
        would never see EOF when the writer died: a hang instead of an error.
        """
        if self.task_ipc_mesh is not None:
            self.task_ipc_mesh.close_pipes_not_owned_by(self._owned_producer_names())

    def _watch_children(self) -> None:
        """Ask the loop to tell us when a child dies.

        Only installed when there is a child, so a worker forked by someone
        else does not steal ``SIGCHLD`` from its own parent.
        """
        if not self.children:
            return
        self.loop.add_signal_handler(signal.SIGCHLD, self._on_child_exit)

    def _on_child_exit(self) -> None:
        """Reap every exited child and decide what to do about it.

        Loops rather than reaping once: signals do not queue, so several
        children dying close together can produce a single ``SIGCHLD``.
        """
        while True:
            try:
                pid, _ = os.waitpid(-1, os.WNOHANG)
            except ChildProcessError:
                # Nothing left to reap; something else already collected it.
                return
            if pid == 0:
                return
            self._replace_child(pid)

    def _replace_child(self, pid: int) -> None:
        """Restart a dead child, or take the whole worker down with it.

        ``reboot_child`` is the choice between availability and noticing: left
        off, a child that dies is treated as a fault of the whole service and
        the worker exits with status 1 so its supervisor sees it. Turned on,
        the child would be re-forked; that path is kept for a later version
        and unreachable today, since ``reboot_child`` is always False.

        Once shutdown has started, children exiting is expected (a supervisor
        often signals the whole process group), so it is neither restarted
        nor counted as a failure.

        Args:
            pid: OS pid reported by ``waitpid``.
        """
        record = self._children_by_pid.pop(pid, None)
        if record is None:
            # A child of a library, or one already handled. Not ours to judge.
            return

        if self._exiting:
            logger.info("[%s] child %s (pid %s) exited during shutdown", self.name, record.name, pid)
            return

        logger.error("[%s] child %s (pid %s) exited unexpectedly", self.name, record.name, pid)
        if not self.reboot_child:
            self._exit_code = 1
            self._handle_sigterm()
            return
        self._start_child(record)

    def _stop_children(self) -> None:
        """SIGTERM every child, then wait for it, before this process exits.

        Runs after ``reboot_child`` has stopped mattering, so a child dying
        here is expected rather than a fault.
        """
        self.reboot_child = False
        for record in self.children:
            if record.process is None or not record.process.is_alive():
                continue
            try:
                record.process.terminate()
            except OSError as err:
                logger.warning("[%s] could not terminate child %s: %s", self.name, record.name, repr(err))
        for record in self.children:
            if record.process is not None:
                record.process.join()

    def run(self) -> None:
        """Turn this process into a serving one, then serve.

        For a forked role this is the first code that executes in the child,
        which is why the loop is created here and not in ``__init__``: an
        event loop cannot cross a fork. The two process-hook calls let plugins
        repair inherited state; they dispatch on the process role.

        The role is stamped onto ``multiprocessing.current_process()`` first.
        For a forked role that object is ``self`` and the stamp repeats what
        ``__init__`` set; for a worker that *is* the main process it is what
        makes ``is_worker_process()`` true, and therefore what makes the
        ``WORKER`` child callbacks run here at all.
        """
        if isinstance(self, BaseProcess):
            bind_lifetime_to_parent()
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

        Template step of :meth:`run`, run before the event loop exists. Only
        the process that owns the listen sockets is given a count; an rpc task
        needs no wait of its own, because no request reaches it until the I/O
        process listens.

        Neither the SIGCHLD nor the SIGTERM handler is installed yet, so this
        covers both itself: a child that dies (say, a task crashing while it
        loads its model) would keep the count from ever arriving, and a
        SIGTERM with the default action would orphan every child. Either way
        the children are stopped before this process exits.

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
            self._stop_children()
            sys.exit(self._exit_code)
        finally:
            if in_main_thread:
                signal.signal(signal.SIGTERM, previous if previous is not None else signal.SIG_DFL)

    def _serve(self) -> None:
        """Take over SIGTERM and run the loop until the process exits.

        Exits with :attr:`_exit_code` once the loop has unwound, so a worker
        brought down by a dead child reports failure.
        """
        try:
            if threading.current_thread() is threading.main_thread():
                # Only the main thread may install signal handlers; a worker
                # started from another thread just runs its loop.
                self._register_signal_handlers()
            self._watch_parent()
            if self.block:
                self.loop.run_forever()
        except (SystemExit, KeyboardInterrupt):
            self.loop.stop()
            self._on_graceful_exit()
            self._stop_children()
        except Exception as err:  # pylint: disable=broad-except
            self.loop.stop()
            self._stop_children()
            self._on_unhandled_exception(err)
        if self._exit_code:
            sys.exit(self._exit_code)

    def _watch_parent(self) -> None:
        """Shut down when the process that forked this one dies.

        The portable half of the parent-death binding (``PR_SET_PDEATHSIG``
        is the Linux half): ``multiprocessing`` gives every child a sentinel
        fd that becomes readable once the parent is gone.

        Known limit: a sibling forked later inherits the write end of this
        child's sentinel, so this child only sees EOF once that sibling has
        exited too. Children therefore go down one after another, not at once.
        A no-op in a process ``multiprocessing`` did not fork.
        """
        if not isinstance(self, BaseProcess):
            return
        parent = multiprocessing.parent_process()
        if parent is None or parent.sentinel is None:
            return
        self.loop.add_reader(parent.sentinel, self._on_parent_exit, parent.sentinel)

    def _on_parent_exit(self, sentinel: int) -> None:
        """Event callback: the parent died, so take the normal SIGTERM path.

        A daemon timer kills the process after
        :data:`PARENT_EXIT_GRACE_SECONDS` in case the shutdown never gets
        there, for instance because a task is stuck inside inference.

        Args:
            sentinel: The parent sentinel fd that became readable.
        """
        self.loop.remove_reader(sentinel)
        logger.error("[%s] parent process exited; shutting down", self.name)
        killer = threading.Timer(PARENT_EXIT_GRACE_SECONDS, os._exit, args=(1,))
        killer.daemon = True
        killer.start()
        self._handle_sigterm()

    def _supervise(self) -> None:
        """Run the loop of a worker that only watches its children.

        Same machinery as :meth:`_serve` — SIGTERM, SIGCHLD, ``run_forever``
        — with nothing but the child watchdog attached to the loop, because
        this process forked specialists to do the serving. Use it in place of
        :meth:`_serve` rather than duplicating the signal handling.
        """
        logger.info("[%s] supervising %d child process(es)", self.name, len(self.children))
        self._serve()

    def _handle_sigterm(self) -> None:
        """Start shutdown: deregister, stop accepting, drain, then exit.

        The sequence is:

        1. ``exit_callbacks`` (service deregistration).
        2. :meth:`_on_sigterm_received`.
        3. :meth:`_stop_accepting`, so no new connection arrives.
        4. After :attr:`drain_seconds`, scheduled with ``call_later`` so the
           loop keeps writing responses meanwhile, :meth:`_exit_after_drain`:
           ``post_drain_callbacks`` (the user server-exit callbacks), then
           :meth:`_on_drained` (the user worker-exit callbacks), then exit.

        ``exit_callbacks`` are kept apart from :meth:`_on_sigterm_received` on
        purpose: subclasses override that hook to log their own shutdown, and
        shutdown work registered by the server must not depend on each of
        them remembering to call ``super()``.

        A second call while draining skips the rest of the drain and exits.
        """
        if self._exiting:
            logger.warning("[%s] shutdown requested again while draining; exiting now", self.name)
            self._exit_after_drain()
            return
        self._exiting = True

        _run_callbacks(self.name, "exit", self.exit_callbacks)
        self._on_sigterm_received()
        try:
            self._stop_accepting()
        except Exception as err:  # pylint: disable=broad-except
            # The loop would swallow this and the exit below would never be
            # scheduled, leaving the process up after SIGTERM.
            logger.error("[%s] failed to stop accepting: %s", self.name, repr(err))

        if self.drain_seconds > 0 and self.loop is not None and self.loop.is_running():
            logger.warning("[%s] draining in-flight requests for %.3f s", self.name, self.drain_seconds)
            self.loop.call_later(self.drain_seconds, self._exit_after_drain)
            return
        self._exit_after_drain()

    def _exit_after_drain(self) -> None:
        """Run ``post_drain_callbacks``, then :meth:`_on_drained`, then leave the loop through ``SystemExit``.

        Each runs at most once, even when a second SIGTERM cuts the drain
        short and calls this again.
        """
        _run_callbacks(self.name, "post-drain", self.post_drain_callbacks)
        self.post_drain_callbacks = []
        if not self._drained:
            self._drained = True
            try:
                self._on_drained()
            except Exception as err:  # pylint: disable=broad-except
                logger.error("[%s] drained hook failed: %s", self.name, repr(err))
        sys.exit()

    def _on_drained(self) -> None:
        """Hook: run the user worker-exit callbacks, after the server-exit ones.

        Runs once the drain period is over, so the callbacks may release what
        in-flight requests were using. Processes that run no handlers override
        it with a no-op.
        """
        run_worker_signal_handle()

    def _stop_accepting(self) -> None:
        """Hook: close the listen sockets; connections already open stay up.

        No-op here. Workers that listen override it.
        """

    def _on_graceful_exit(self) -> None:
        """Hook: release resources after the loop stopped on a normal exit."""

    def _on_unhandled_exception(self, err: BaseException) -> None:
        """Handle an exception that escaped the event loop.

        The default re-raises, so a forked worker dies with a traceback and a
        non-zero exit status and its parent can restart it. Override to
        swallow the error instead.

        Args:
            err: The exception that terminated the loop.
        """
        logger.error("[%s] exited with error: %s", self.name, repr(err))
        raise err

    async def _create_pipe_conn(self,
                                pipe_conn: PipeConnection,
                                task_info: TaskInfo,
                                loop: Optional[asyncio.AbstractEventLoop] = None) -> None:
        """Attach this worker's end of a task IPC pipe to a loop.

        Drops the unused socketpair end inherited across fork, wraps the
        remaining fd in
        :class:`~trpc_sdk.transport.WorkerTaskConnection`, and registers the
        transport under ``task_info.name`` so handlers can produce to that
        task.

        A coroutine rather than a blocking call because a merge worker sets its
        task pipes up from inside each serving loop, where that loop is already
        running and ``run_until_complete`` would refuse. The caller decides how
        to drive it: the main loop drives it with ``run_until_complete``, a
        serving thread simply awaits it.

        Args:
            pipe_conn: This worker's ``PipeEnd.RIGHT`` end of the producer <->
                task socketpair. Must not be ``None``.
            task_info: The task this pipe talks to; supplies the name the
                transport is registered under.
            loop: Loop that will own the transport, and therefore the only one
                allowed to write it. Defaults to this worker's main loop; a
                merge worker passes each serving thread's loop instead.
        """
        loop = loop or self.loop
        pipe_conn.close_extra_conn()
        coros = loop.create_connection(WorkerTaskConnection, sock=pipe_conn.get_socket())
        (transport, _) = await coros
        register_task_conn(task_info.name, transport)

    def _before_event_loop(self) -> None:
        """Start tracemalloc before the event loop (or anything else) allocates.

        First hook inside :meth:`run`. A no-op when memory inspection is
        off. Starting later would miss allocations from loop and plugin
        setup.
        """
        if self.enable_memory_inspection:
            self.memory_inspector.start_tracing()

    def _init_plugins(self) -> None:
        """Initialize every plugin, including business ones.

        This process runs user handlers, so filters, naming, metrics and the
        rest must be fully set up. Infrastructure roles that never execute
        user code narrow this to ``init_task_plugins()``.
        """
        init_worker_plugins()

    def _prepare_to_serve(self) -> None:
        """Wire task IPC, the message pipe, the request source, then timers.

        Runs after plugins are initialized and before ``loop.run_forever()``.
        Order matters:

        1. Publish this process as ``current_worker()`` so
           ``send_msg_worker`` can tag messages with this worker's id.
        2. Drop inherited pipe ends that belong to other processes, and
           attach (or drop) the task heartbeat pipes.
        3. Connect to each task over its IPC pipe.
        4. If ``on_worker_message`` is set, listen on ``read_message_pipe``.
        5. :meth:`start_worker_process` — subclass attaches to the
           dispatcher pipe or binds listen sockets.
        6. ``ready_callbacks`` — the addresses are reachable now, so this is
           the earliest moment naming registration is truthful.
        7. :meth:`_start_timers`.

        Uses ``loop.run_until_complete`` for connection setup, so the loop
        must already exist.
        """
        self._publish_current_worker()

        self._bind_task_mesh()
        self._close_foreign_pipes()
        self._attach_heartbeats()
        self._connect_tasks()
        self._listen_for_worker_messages()

        self.start_worker_process()
        _run_callbacks(self.name, "ready", self.ready_callbacks)
        self._start_timers()

    def _attach_heartbeats(self) -> None:
        """Monitor the tasks this process forked, or drop pipes it only inherited.

        The process that forked the tasks owns the monitor end of every
        heartbeat pipe and watches them on its own loop, now that the last
        fork is behind it. Any other child of that process (an rpc task, say)
        inherited both ends of every pipe and closes them, so a dead task's
        pipe actually reaches EOF. Task processes do not come through here:
        their sender is started from a ``TASK`` child callback.
        """
        # Local import: the heartbeat module registers a plugin at import time.
        from ._healthy_checker import close_heartbeat_pipes
        from ._healthy_checker import start_heartbeat_monitor

        if self.children:
            start_heartbeat_monitor(self.loop)
        else:
            close_heartbeat_pipes()

    def _start_timers(self) -> None:
        """Start this process's timer executor, labelled ``worker:<id>``.

        Template step, last in :meth:`_prepare_to_serve`. A process that runs
        no business code overrides it with a no-op, so that a timer without a
        target name runs once per handler process and not once more there.
        """
        self.timer = TimerExecutor(loop=self.loop, name=f"worker:{self.worker_id}")
        self.timer.run()

    def _publish_current_worker(self) -> None:
        """Make ``current_worker()`` return this worker's record.

        A no-op for a worker that owns its process outright: there is no
        ``WorkerInfo`` to publish, because nobody forked this process from one.
        """
        if self.worker is None:
            return
        self.worker.process_instance = self
        set_current_worker(self.worker)

    @property
    def task_records(self) -> List[TaskInfo]:
        """Task records index-aligned with ``task_pipe_conn``."""
        return self.task_infos

    def _connect_tasks(self) -> None:
        """Open this worker's end of every task connection, on its main loop.

        Overridden by a worker that serves on more than one loop, since a
        transport belongs to the loop that created it.
        """
        self.loop.run_until_complete(self._connect_tasks_on(self.loop))

    async def _connect_tasks_on(self,
                                loop: asyncio.AbstractEventLoop,
                                pipe_conns: Optional[List[Optional[PipeConnection]]] = None) -> None:
        """Connect every task from one serving loop.

        Iterates the pipe list, which is index-aligned with
        :attr:`task_records`; an empty list means no task was registered and
        nothing is connected. ``None`` entries are tasks this producer has no
        pipe to, which a ``Task(peers=[...])`` declaration can create.

        Args:
            loop: Loop that will own the transports, and the only one from
                which handlers may then produce to these tasks.
            pipe_conns: Producer ends to attach. Defaults to this worker's
                own ``task_pipe_conn``; a merge worker passes the set
                belonging to the serving loop being set up.
        """
        records = self.task_records
        if pipe_conns is None:
            pipe_conns = self.task_pipe_conn
        for i, pipe_conn in enumerate(pipe_conns):
            task_info: TaskInfo = records[i]
            if pipe_conn is not None:
                await self._create_pipe_conn(task_info=task_info, pipe_conn=pipe_conn, loop=loop)

    def _listen_for_worker_messages(self) -> None:
        """Attach the inter-worker message pipe, when there is one to attach.

        Skipped unless the user registered ``on_worker_message`` *and* this
        worker was forked with a message pipe: without a callback the RIGHT end
        is never written and the pipe stays idle, and without a ``WorkerInfo``
        there is no pipe at all.
        """
        if self.worker is None or self.server_engine is None:
            return
        if not self.server_engine.on_worker_message:
            return
        self.start_message_listener(self.server_engine)

    def _register_signal_handlers(self) -> None:
        """Install SIGTERM, and SIGUSR2 when memory inspection is on.

        SIGTERM goes through :meth:`_on_sigterm_received`. SIGUSR2 dumps a
        tracemalloc snapshot to the configured file prefix. SIGCHLD is only
        taken over when this worker forked children of its own.
        """
        self.loop.add_signal_handler(signal.SIGTERM, self._handle_sigterm)
        # User can get memory allocation information through SIGUSR2 signal
        if self.enable_memory_inspection:
            self.loop.add_signal_handler(signal.SIGUSR2, self.memory_inspector.dump_to_file)
        self._watch_children()

    def _on_sigterm_received(self) -> None:
        """Log the exit.

        Invoked on the event-loop signal path. The base :meth:`_handle_sigterm`
        stops accepting after this returns; the user worker-exit callbacks run
        later, from :meth:`_on_drained`, once the drain period is over.
        """
        logger.error("[worker] worker exit")

    def start_worker_process(self) -> None:
        """Connect this worker to whatever feeds it requests.

        No-op here. A process that is fed by a pipe, such as
        :class:`~trpc_sdk.server._separate_worker.RpcTaskProcess`, attaches to
        it; a process that accepts for itself, such as
        :class:`~trpc_sdk.server._merge_worker.MergeWorker`, binds its listen
        sockets instead.
        """
        pass

    def start_message_listener(self, engine) -> None:
        """Listen on this worker's inter-worker message pipe.

        Wraps ``read_message_pipe`` (LEFT) in :class:`WorkerMsgConnection`.
        Frames written to the RIGHT end — typically by another worker via
        ``send_msg_worker``, using an fd inherited at fork — are decoded
        and handed to ``engine.on_worker_message(from_id, data)``. The
        master process is not on the data path; it only created the
        socketpair before fork.

        Args:
            engine: Server engine whose ``on_worker_message`` callback is
                invoked for each decoded frame.
        """
        handle = partial(WorkerMessageConnection, engine=engine)
        coro = self.loop.create_connection(handle, sock=self.worker.read_message_pipe.get_socket())
        (transport, protocol) = self.loop.run_until_complete(coro)
        self.message_pipe_transport = transport
        self.message_pipe_protocol = protocol
