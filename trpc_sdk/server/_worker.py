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
"""

from __future__ import annotations

import asyncio
import os
import signal
import sys
import threading
from functools import partial
from typing import TYPE_CHECKING
from typing import Any
from typing import Callable
from typing import Dict
from typing import List
from typing import Optional
from typing import Tuple

from trpc_sdk.abc import BaseServerProtocolChecker
from trpc_sdk.abc import BaseServerTransportOption
from trpc_sdk.common import ProcessType
from trpc_sdk.common import execute_after_in_child
from trpc_sdk.common import execute_before_loop_run_in_child
from trpc_sdk.common import get_or_create_event_loop
from trpc_sdk.config import TrpcConfig
from trpc_sdk.ipc import PipeConnection
from trpc_sdk.log import logger
from trpc_sdk.plugin import init_worker_plugins
from trpc_sdk.task import TaskStreamClientMap
from trpc_sdk.task import register_task_conn
from trpc_sdk.transport import WorkerMessageConnection
from trpc_sdk.transport import WorkerTaskConnection
from trpc_sdk.utils import TimerExecutor
from trpc_sdk.utils import asynclib
from trpc_sdk.utils import init_thread_pool
from ._signals import run_worker_signal_handle
from ._types import ChildRecord
from ._types import TaskInfo
from ._types import WorkerInfo

if TYPE_CHECKING:
    from trpc_sdk._inner import MemoryInspector
    from ._task import TaskIpcMesh


_current_worker: Optional[WorkerInfo] = None


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
    has to exist before plugins are initialized, plugins before the front end
    is attached, and the front end before the loop starts turning. Subclasses
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
        # ``task_name`` and ``task_records``. ``None`` for stream tasks, which
        # connect over a Unix socket instead.
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
        self.reboot_child: bool = TrpcConfig.server_config().reboot_child
        # Callables run on SIGTERM before the user callbacks, in registration
        # order. This is where the server puts service deregistration, so the
        # process leaves the naming service before it stops answering.
        self.exit_callbacks: List[Callable[[], None]] = []

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
        self.checkers: List[Optional[BaseServerProtocolChecker]] = []
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
        pair joins two children, so neither of them can create it. Timer
        children are forked after the tasks; they do not share the mesh.
        """
        # Local import: ``_task`` needs ``BaseWorker`` from this module, so the
        # dependency can only run one way at import time.
        from trpc_sdk.utils import get_register_timer_process
        from ._healthy_checker import init_healthy_checker
        from ._task import TaskIpcMesh
        from ._task import build_task_infos
        from ._task import set_task_ipc_mesh

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

        # Timers used to be forked by the master engine. That path is gone, so
        # the worker that owns this process forks them here, and they join
        # ``children`` for restart and shutdown.
        for timer_info in get_register_timer_process().values():
            self._spawn(timer_info.name, partial(timer_info.timer_handle, timer_info))

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
        the worker exits so its supervisor sees it. Turned on, the child is
        re-forked and the outage is limited to the work it was holding.

        Args:
            pid: OS pid reported by ``waitpid``.
        """
        record = self._children_by_pid.pop(pid, None)
        if record is None:
            # A child of a library, or one already handled. Not ours to judge.
            return

        logger.error("[%s] child %s (pid %s) exited unexpectedly", self.name, record.name, pid)
        if not self.reboot_child:
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
        repair inherited state; they dispatch on the process role and are
        no-ops in a process that was not forked.
        """
        self._before_event_loop()

        self.loop = asynclib.new_event_loop()
        asyncio.set_event_loop(self.loop)

        execute_after_in_child()
        self._init_plugins()
        self._prepare_to_serve()
        execute_before_loop_run_in_child()

        self._serve()

    def _serve(self) -> None:
        """Take over SIGTERM and run the loop until the process exits."""
        try:
            if threading.current_thread() is threading.main_thread():
                # Only the main thread may install signal handlers; a worker
                # started from another thread just runs its loop.
                self._register_signal_handlers()
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
        """Run the exit callbacks, then leave the loop through ``SystemExit``.

        ``exit_callbacks`` run first and are kept apart from
        :meth:`_on_sigterm_received` on purpose: subclasses override that hook
        to log their own shutdown, and shutdown work registered by the server
        must not depend on each of them remembering to call ``super()``.
        """
        for callback in self.exit_callbacks:
            try:
                callback()
            except Exception as err:  # pylint: disable=broad-except
                # One failing callback must not stop the rest, nor keep the
                # process alive.
                logger.error("[%s] exit callback failed: %s", self.name, repr(err))
        self._on_sigterm_received()
        sys.exit()

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
        """Attach this worker's end of a non-stream task IPC pipe to a loop.

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

    def _create_unix_conn(self, task_info: TaskInfo) -> None:
        """Point the stream-task client at the Unix socket the task will bind.

        Stream tasks do not share a socketpair. After fork the task listens
        on ``.__{name}`` and this worker's client connects there. The client
        is stored in :class:`TaskStreamClientMap` so handlers can look it
        up by name.

        Args:
            task_info: Stream task whose ``task_stream_handler.client`` is
                configured here. The matching ``task_pipe_conn`` entry is
                ``None``.
        """
        task_stream_client = task_info.task_stream_handler.client
        task_stream_client.set_unix_path(f".__{task_info.name}")
        TaskStreamClientMap.register_task_stream_client(task_info.name, task_stream_client)

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

        The thread pool is not a plugin, but it is built here for the same
        reason: it must be created after fork, in the process that will
        ``submit`` blocking work from a handler.
        """
        init_worker_plugins()
        init_thread_pool()

    def _prepare_to_serve(self) -> None:
        """Wire task IPC, the message pipe, the request source, then timers.

        Runs after plugins are initialized and before ``loop.run_forever()``.
        Order matters:

        1. Publish this process as ``current_worker()`` so
           ``send_msg_worker`` can tag messages with this worker's id.
        2. Connect to each task (pipe, or Unix socket for stream tasks).
        3. If ``on_worker_message`` is set, listen on ``read_message_pipe``.
        4. :meth:`start_worker_process` — subclass attaches to the
           dispatcher pipe or binds listen sockets.
        5. Start the timer executor.

        Uses ``loop.run_until_complete`` for connection setup, so the loop
        must already exist.
        """
        self._publish_current_worker()
        self.timer = TimerExecutor(loop=self.loop, name=f"worker:{self.worker_id}")

        self._bind_task_mesh()
        self._close_foreign_pipes()
        self._connect_tasks()
        self._listen_for_worker_messages()

        self.start_worker_process()
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
            if task_info.task_stream_handler:
                self._create_unix_conn(task_info)
            elif pipe_conn is not None:
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
        """Run user worker-exit callbacks, then log the exit.

        Invoked on the event-loop signal path, so callbacks must not block.
        ``sys.exit()`` is issued by the base :meth:`_handle_sigterm` after
        this returns.
        """
        run_worker_signal_handle()
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
