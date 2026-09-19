# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Task child processes: background work moved off the worker processes.

A task is registered with ``@trpc.task`` and runs in its own process, so slow
or blocking work does not sit on a worker's event loop. Workers hand work over
one IPC pipe per ``(task, worker)`` pair, created by the master before fork; a
task that serves a stream handler uses a Unix-domain socket instead, because a
stream needs a full-duplex connection per call rather than a datagram-shaped
hand-off.

This module provides:

* :class:`BaseTaskProcess` — what makes a task a worker that happens to be
  forked: :class:`~trpc_sdk.server._worker.BaseWorker` for the startup sequence
  and IPC bookkeeping, ``multiprocessing.Process`` for the process identity
  (MRO is ``(BaseWorker, Process)`` so :meth:`BaseWorker.run` wins).
* :class:`TaskProcess` — an event loop feeding a global queue that
  :class:`~trpc_sdk.task.TaskConsumer` threads drain.

The pre-fork record is :class:`~trpc_sdk.server._types.TaskInfo`.
"""

from __future__ import annotations

import asyncio
import os
from functools import partial
from multiprocessing import Process
from typing import Any
from typing import Callable
from typing import Collection
from typing import Dict
from typing import List
from typing import Optional

from trpc_sdk.abc import BaseTaskStreamServiceHandler
from trpc_sdk.common import ProcessType
from trpc_sdk.ipc import PipeConnection
from trpc_sdk.ipc import PipeEnd
from trpc_sdk.ipc import PipeKind
from trpc_sdk.ipc import create_pipe
from trpc_sdk.log import logger
from trpc_sdk.naming import pause_heartbeat as pause_registry_heartbeat
from trpc_sdk.naming import resume_heartbeat
from trpc_sdk.plugin import get_registered_tasks
from trpc_sdk.plugin import init_task_plugins
from trpc_sdk.stream import TaskStreamServiceAdapter
from trpc_sdk.task import TaskConsumer
from trpc_sdk.task import consume_response
from trpc_sdk.task import register_task_num
from trpc_sdk.task import task_init
from trpc_sdk.transport import TaskStreamConnection
from trpc_sdk.transport import TaskWorkerConnection
from trpc_sdk.utils import unlink_unix_socket_file
from ._constants import WORKER_PRODUCER_PREFIX
from ._types import TaskInfo
from ._worker import BaseWorker


def worker_producer_name(index: int) -> str:
    """Return the producer name of one serving loop in the worker process.

    Args:
        index: Loop index; ``0`` when the worker serves on its main loop only.
    """
    return f"{WORKER_PRODUCER_PREFIX}:{index}"


def base_task_name(name: str) -> str:
    """Strip the child index off an indexed task name.

    A task registered with ``task_num > 1`` is forked as ``name:0``,
    ``name:1``, ... while ``Task(peers=[...])`` names its peers without an
    index, so the two have to be compared on the base name.

    Args:
        name: Task name, indexed (``"infer:2"``) or not (``"infer"``).

    Returns:
        The part before the first colon.
    """
    return name.split(":", 1)[0]


def build_task_infos() -> List[TaskInfo]:
    """Turn every ``@trpc.task`` registration into pre-fork records.

    One :class:`TaskInfo` per child: a task declared with ``task_num: 3``
    produces ``name:0``, ``name:1`` and ``name:2``. No pipe is created and
    nothing is forked here; :func:`create_task_ipc_mesh` and the worker's
    ``_spawn_children`` do that next.

    ``pause_heartbeat()`` is called here, while still in the process that will
    fork, so the registry client stops before any child exists.

    Returns:
        Pre-fork records in the order they will be forked.
    """
    task_infos: List[TaskInfo] = []
    for task_handler in get_registered_tasks():
        register_task_num(task_handler.name, task_handler.task_num)

        handle = TaskProcess

        for i in range(task_handler.task_num):
            task = TaskInfo()
            task.task_stream_handler = task_handler.task_stream_handler
            task.name = f"{task_handler.name}:{i}"
            task.handle = handle
            task.task_handle = (task_handler.initializer, task_handler.handler)
            task.batch_size = task_handler.batch_size
            task.thread_num = task_handler.thread_num
            task.model_info = task_handler.model_info
            task.use_c_task = task_handler.use_c_task
            task.c_task_driver_params = task_handler.c_task_driver_params
            task.on_fork = task_handler.on_fork
            task.pause_heartbeat = task_handler.pause_heartbeat
            task.peers = list(getattr(task_handler, "peers", []) or [])
            if task.pause_heartbeat:
                pause_registry_heartbeat()
            task_infos.append(task)
    return task_infos


class TaskIpcMesh:
    """Every task socketpair in the process tree, created before any fork.

    Two kinds of edge live here and they are built the same way, which is the
    point: a task talks to another task over exactly the pipe a worker would
    have used, so :func:`~trpc_sdk.task.send_to_task` behaves identically whether
    it is called from a worker or from inside a task handler.

    * producer -> task, where a producer is one serving loop of the worker or
      one rpc task process. Always created.
    * task -> task, only for the pairs a ``Task(peers=[...])`` declared. A
      full mesh would cost ``O(T^2)`` socketpairs for a capability most tasks
      never use.

    Everything must exist before the **first** fork, not merely before the
    fork of the two processes an edge connects: a task-to-task pair joins two
    different children, so neither can create it.

    Attributes:
        task_infos: The tasks, in fork order. Index into this list is the
            column of the mesh.
        producer_names: Everyone that may send work, in the order
            ``TaskInfo.task_pipe_conn`` uses. Index into this list is the
            ``task_conn_index`` carried by a response.
    """

    def __init__(self, task_infos: List[TaskInfo], worker_producers: List[str]) -> None:
        """Create every socketpair the declared topology needs.

        Args:
            task_infos: Pre-fork task records from :func:`build_task_infos`.
            worker_producers: Producer names that reach *every* task, i.e. the
                worker's serving loops and, in separate mode, the rpc task
                processes.
        """
        self.task_infos: List[TaskInfo] = task_infos
        self._worker_producers: List[str] = list(worker_producers)
        # A task that declared peers is a producer too, once per child.
        peer_producers = [task.name for task in task_infos if task.peers]
        self.producer_names: List[str] = self._worker_producers + peer_producers

        # Producer name -> its end toward each task, index-aligned with
        # ``task_infos``. ``None`` where that producer has no edge to a task.
        self._producer_pipes: Dict[str, List[Optional[PipeConnection]]] = {
            name: [None] * len(task_infos) for name in self.producer_names
        }
        self._build()

    def _build(self) -> None:
        """Fill both sides of every declared edge with a fresh socketpair.

        The task keeps ``PipeEnd.LEFT`` and the producer ``PipeEnd.RIGHT``,
        matching what the master used to hand out, so the protocols on either
        side are unchanged.
        """
        for column, task in enumerate(self.task_infos):
            task.producer_names = list(self.producer_names)
            task.task_pipe_conn = [None] * len(self.producer_names)
            if task.task_stream_handler is not None:
                # Stream tasks bind a Unix-domain socket instead: a stream
                # needs a connection per call, not a shared datagram pipe.
                continue
            for row, producer in enumerate(self.producer_names):
                if not self._has_edge(producer, task):
                    continue
                socket_pair = create_pipe(PipeKind.TCP)
                socket_pair[0].setblocking(False)
                socket_pair[1].setblocking(False)
                task.task_pipe_conn[row] = PipeConnection(socket_pair, PipeEnd.LEFT)
                self._producer_pipes[producer][column] = PipeConnection(socket_pair, PipeEnd.RIGHT)

    def _has_edge(self, producer: str, task: TaskInfo) -> bool:
        """Report whether ``producer`` is allowed to send work to ``task``.

        Args:
            producer: Producer name from ``producer_names``.
            task: The task record being considered.

        Returns:
            True for a worker or rpc task producer, which reach every task.
            For a peer producer, whether that task declared this one, compared
            on base names so ``peers=["infer"]`` covers every child of it.
        """
        if producer in self._worker_producers:
            return True
        if producer == task.name:
            # A task does not open a pipe to itself.
            return False
        declaring = self._task_by_name(producer)
        return declaring is not None and base_task_name(task.name) in declaring.peers

    def _task_by_name(self, name: str) -> Optional[TaskInfo]:
        """Return the task record with this exact indexed name, if any.

        Args:
            name: Indexed task name such as ``"infer:1"``.
        """
        for task in self.task_infos:
            if task.name == name:
                return task
        return None

    def producer_pipes(self, producer: str) -> List[Optional[PipeConnection]]:
        """Return one producer's end toward each task.

        Args:
            producer: Producer name from ``producer_names``.

        Returns:
            A list index-aligned with ``task_infos``; ``None`` where this
            producer has no edge to that task.
        """
        return self._producer_pipes.get(producer, [None] * len(self.task_infos))

    def close_pipes_not_owned_by(self, owners: Collection[str]) -> None:
        """Drop every socketpair end this process has no business holding.

        Fork copies **all** fds, so without this a process keeps the write end
        of pipes between two other processes open. The kernel then still counts
        a live reference and the reader never sees EOF when its real peer dies:
        the symptom is a read that hangs forever rather than an error, which is
        why this step cannot be skipped.

        Call it after the last fork, from inside each process.

        Args:
            owners: Names this process answers to: its task name, and/or the
                producer names of its serving loops. Everything else is closed.
        """
        owned = set(owners)
        for column, task in enumerate(self.task_infos):
            for row, producer in enumerate(self.producer_names):
                task_end = task.task_pipe_conn[row]
                if task_end is None:
                    continue
                if task.name in owned:
                    # Keep the task end, drop the producer's.
                    task_end.close_extra_conn()
                elif producer in owned:
                    self._producer_pipes[producer][column].close_extra_conn()
                else:
                    # Neither side is ours: this pair belongs to two other
                    # processes entirely.
                    task_end.socket_pair[0].close()
                    task_end.socket_pair[1].close()


_task_ipc_mesh: Optional[TaskIpcMesh] = None


def set_task_ipc_mesh(mesh: Optional[TaskIpcMesh]) -> None:
    """Publish the mesh so forked children inherit it.

    Assigned in the worker **before** the first fork, which is the only way a
    child can learn about pipes it did not create.

    Args:
        mesh: The mesh this process tree was built with, or ``None`` to clear
            it between tests.
    """
    global _task_ipc_mesh  # pylint: disable=global-statement
    _task_ipc_mesh = mesh


def task_ipc_mesh() -> Optional[TaskIpcMesh]:
    """Return the mesh this process tree was built with.

    Returns:
        The mesh, or ``None`` in a process that was not started through a
        worker's ``_spawn_children`` (the legacy master path, and most tests).
    """
    return _task_ipc_mesh


class BaseTaskProcess(BaseWorker, Process):
    """A task is a worker that always runs in a process of its own.

    Both halves are load-bearing. :class:`~trpc_sdk.server._worker.BaseWorker`
    gives a task the same startup sequence and the same IPC bookkeeping every
    worker has, which is what lets a task talk to other tasks exactly the way
    a worker talks to a task — same pipes, same helpers, no second mechanism.
    ``Process`` supplies the process identity, because unlike a worker a task
    is never the current process: it is always forked by whoever hands it
    work. ``BaseWorker`` is listed first so its ``run()`` wins over
    ``Process.run()``, which would otherwise try to call a ``target`` that
    was never set.

    Subclasses implement :meth:`BaseWorker._prepare_to_serve` to attach the
    pipes (or Unix socket) they are fed from.
    """

    def _init_plugins(self) -> None:
        """Initialize only the non-business plugins.

        Narrows :meth:`BaseWorker._init_plugins`: a task receives already
        decoded work over IPC, so the server-side filter, naming and codec
        plugins have no role here. A task that needs a business-defined
        plugin should set it up in its user initializer.
        """
        init_task_plugins()


class TaskProcess(BaseTaskProcess):
    """Python task child: drain worker IPC (or a Unix stream socket) off the workers.

    Constructed in the master **before fork**. After fork,
    :class:`BaseTaskProcess` builds the event loop and plugins; this class
    only fills in task-specific wiring in :meth:`_prepare_to_serve`.

    Two serving modes:

    * Stream task — bind ``.__{name}`` and accept full-duplex Unix connections
      from workers. There is no socketpair; the matching
      ``task_pipe_conn`` entries are ``None``.
    * Non-stream task — wrap each worker socketpair in :class:`TaskConnection`,
      start :class:`TaskConsumer` threads that pull batches from the global
      queue, and schedule :func:`consume_response` to write results back.

    ``run()`` and SIGTERM handling stay on :class:`BaseTaskProcess`.
    """

    def __init__(self, task_info: TaskInfo, engine) -> None:
        """Record name, pipes, handler and queue opts; no sockets are bound yet.

        Runs in the master **before fork**. The event loop, plugin setup and
        IPC wiring happen later in :meth:`run` / :meth:`_prepare_to_serve`.

        Args:
            task_info: Master-side task record. ``task_handle`` is
                ``(initializer, handler)``; ``task_pipe_conn`` is index-aligned
                with the worker list (``None`` for stream tasks).
            engine: Master engine. The child inherits this object through fork
                and reads ``opts.task_*`` defaults from it.
        """
        super().__init__(task_info.name, ProcessType.TASK)
        # Master engine that forked this child; used for task_* option defaults.
        self.server_engine = engine
        # Master-side task record this process was spawned for.
        self.task_info: TaskInfo = task_info
        # Inbound: this task's end of each producer <-> task IPC pipe,
        # index-aligned with ``task_info.producer_names``. ``None`` for a
        # stream task (which is reached over a Unix-domain socket instead) and
        # for a peer producer that did not declare this task.
        #
        # Distinct from the inherited ``BaseWorker.task_pipe_conn``, which is
        # the *outbound* direction: this task's producer-side ends toward the
        # peers it declared. A task is both a consumer and a producer, so it
        # needs both lists.
        self.producer_pipe_conn: List[Optional[PipeConnection]] = task_info.task_pipe_conn

        # Outbound: the peers this task declared. ``BaseWorker._connect_tasks``
        # then wires them exactly as it does for a worker, which is what makes
        # ``send_to_task`` usable from inside a task handler.
        mesh = task_ipc_mesh()
        if mesh is not None:
            self.task_ipc_mesh = mesh
            self.task_infos = mesh.task_infos
            self.task_name = [peer.name for peer in mesh.task_infos]
            self.task_pipe_conn = mesh.producer_pipes(task_info.name)

        # Optional user callback run once after fork, before work is accepted.
        # ``task_handle[0]``; registration and tests may leave this ``None``.
        self.task_initializer: Optional[Callable[..., Any]] = task_info.task_handle[0]
        # User handler invoked by each :class:`TaskConsumer` (sync or async).
        self.task_handle: Callable[..., Any] = task_info.task_handle[1]
        # Milliseconds a consumer sleeps when the inbound queue is empty.
        self.wait_interval: int = engine.opts.task_wait_interval
        if self.task_info.batch_size <= 0:
            self.task_info.batch_size = engine.opts.task_batch_size

        # Capacity of the global C task queue created in ``_init_plugins``.
        self.queue_size: int = engine.opts.task_queue_size
        # Number of :class:`TaskConsumer` threads started for a non-stream task.
        self.consumer_num: int = engine.opts.task_consumer_num
        # If True, registry heartbeats were paused at registration and must be
        # resumed in ``_prepare_to_serve`` after the initializer returns.
        self.pause_heartbeat: bool = task_info.pause_heartbeat
        # Unix-domain path a stream task binds; unused for non-stream tasks.
        self._unix_path: str = f".__{task_info.name}"
        # asyncio server returned by ``create_unix_server``; ``None`` until a
        # stream task binds, and ``None`` again after graceful exit.
        self._unix_srv: Optional[asyncio.AbstractServer] = None
        # Stream-task service implementation; ``None`` for non-stream tasks.
        self._task_stream_service: Optional[BaseTaskStreamServiceHandler] = (
            task_info.task_stream_handler.service if task_info.task_stream_handler else None
        )

    def _create_unix_server(self) -> None:
        """Bind ``.__{name}`` so workers can open a full-duplex stream.

        Unlinks a leftover socket file first: a previous incarnation may have
        crashed without removing it, and ``create_unix_server`` would then fail
        to bind. Called only when ``_task_stream_service`` is set.

        Uses ``loop.run_until_complete`` because this runs in
        :meth:`_prepare_to_serve`, before the loop is serving.
        """
        service_adapter = TaskStreamServiceAdapter(task_service=self._task_stream_service)
        handler = partial(TaskStreamConnection, service_adapter=service_adapter, loop=self.loop)
        if os.path.exists(self._unix_path):
            logger.info(f"'{self._unix_path}' file is already exist, must delete to avoid error")
            unlink_unix_socket_file(self._unix_path)
        coros = self.loop.create_unix_server(handler, path=self._unix_path)
        self._unix_srv = self.loop.run_until_complete(coros)

    def _create_producer_conns(self) -> None:
        """Attach each producer IPC pipe, then start consumers and the rsp pump.

        Named apart from :meth:`~trpc_sdk.server._worker.BaseWorker._create_pipe_conn`
        on purpose: that one opens a single pipe *towards* a task, this one
        accepts every pipe arriving *at* this task. They are opposite ends of
        the mesh and a task process does both, so sharing a name would make one
        of them unreachable.

        For every non-``None`` pipe: drop the unused socketpair end inherited
        across fork, wrap the remaining fd in :class:`TaskConnection`, and
        register it on the loop. Then start ``consumer_num``
        :class:`TaskConsumer` threads that pull batches through
        ``task_handle``, and schedule :func:`consume_response` so completed
        work is written back to the originating worker.

        Called only for non-stream tasks. ``None`` entries are producers that
        cannot reach this task, which happens once another task declares
        ``peers``; they are skipped so ``index`` stays the producer index the
        response has to be routed back on.
        """
        for index, pipe_conn in enumerate(self.producer_pipe_conn):
            if pipe_conn is None:
                continue
            pipe_conn.close_extra_conn()
            handle = partial(TaskWorkerConnection, app=self, index=index)
            coros = self.loop.create_connection(handle, sock=pipe_conn.get_socket())
            self.loop.run_until_complete(coros)

        for _ in range(self.consumer_num):
            task_consumer = TaskConsumer(func=self.task_handle,
                                         wait_interval=self.wait_interval,
                                         batch_size=self.task_info.batch_size)
            task_consumer.start()

        self.loop.create_task(consume_response(self.wait_interval))

    def _init_plugins(self) -> None:
        """Create the global task queue, then initialize non-business plugins.

        The queue must exist first: plugins (and later consumers) already
        assume it. Business-defined plugins are skipped here; if a task needs
        one, the user initializer should set it up.
        """
        task_init(self.queue_size)
        super()._init_plugins()

    def _prepare_to_serve(self) -> None:
        """Drop foreign fds, run the initializer, then accept work.

        Closing comes first, before any user code can block: fork gave this
        child every socketpair in the tree, and a pipe between two *other*
        processes that we keep open stops its reader from ever seeing EOF.

        If ``pause_heartbeat`` was set at registration, registry heartbeats
        resume after the initializer so the service is not advertised until
        the task is ready. A stream task then binds its Unix socket; a
        non-stream task attaches the producer pipes and starts consumers.
        Peer connections are opened last, so a task can call
        :func:`~trpc_sdk.task.send_to_task` from its handler.
        """
        self._close_foreign_pipes()

        if self.task_initializer is not None:
            self.task_initializer()
        if self.pause_heartbeat:
            resume_heartbeat()

        if self._task_stream_service:
            self._create_unix_server()
        else:
            self._create_producer_conns()

        self._connect_tasks()

    def _owned_producer_names(self) -> List[str]:
        """Return this task's own name, which covers both roles it plays.

        The mesh is checked cell by cell, so one name is enough: it keeps the
        task end of every pipe pointed at this task, and the producer end of
        every pipe this task declared as a peer.
        """
        return [self.task_info.name]

    def _on_sigterm_received(self) -> None:
        """Log the SIGTERM; the base class then exits through ``SystemExit``."""
        logger.error("[task] worker exit")

    def _on_graceful_exit(self) -> None:
        """Close the Unix server and unlink its socket file, if one was bound.

        The next incarnation of this task reuses ``.__{name}`` and cannot bind
        while the old file is still on disk.
        """
        if self._unix_srv is not None:
            self._unix_srv.close()
            unlink_unix_socket_file(self._unix_path)
            self._unix_srv = None
