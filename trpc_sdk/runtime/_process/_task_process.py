# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Task child processes: background work moved off the worker processes.

A task is registered with ``@trpc.task`` and runs in its own process, so slow
or blocking work does not sit on a worker's event loop. Workers hand work over
one IPC pipe per ``(task, worker)`` pair, created by the master before fork.

This module provides:

* :class:`BaseTaskProcess` — what makes a task a worker that happens to be
  forked: :class:`~trpc_sdk.runtime._worker.BaseWorker` for the startup sequence
  and IPC bookkeeping, ``multiprocessing.Process`` for the process identity
  (MRO is ``(BaseWorker, Process)`` so :meth:`BaseWorker.run` wins).
* :class:`TaskProcess` — an event loop feeding a global queue that
  :class:`~trpc_sdk.task.TaskConsumer` threads drain.

The pre-fork record is :class:`~trpc_sdk.runtime._types.TaskInfo`.
"""

from __future__ import annotations

import time
from functools import partial
from multiprocessing import Process
from typing import Any
from typing import Callable
from typing import Collection
from typing import Dict
from typing import List
from typing import Optional

from trpc_sdk.abc import ProcessType
from trpc_sdk.log import logger
from trpc_sdk.plugin import get_registered_tasks
from trpc_sdk.plugin import init_task_plugins
from trpc_sdk.task import TaskConsumer
from trpc_sdk.task import TaskWorkerConnection
from trpc_sdk.task import consume_response
from trpc_sdk.task import register_task_num
from trpc_sdk.task import task_init
from trpc_sdk.utils import PipeConnection
from trpc_sdk.utils import PipeEnd
from trpc_sdk.utils import PipeKind
from trpc_sdk.utils import create_pipe
from trpc_sdk.utils import pause_heartbeat as pause_registry_heartbeat
from trpc_sdk.utils import resume_heartbeat
from .._constants import CHILD_EXIT_GRACE_SECONDS
from .._constants import WORKER_SENDER_PREFIX
from .._types import TaskInfo
from .._worker import BaseWorker


def worker_sender_name(index: int) -> str:
    """Return the sender name of one serving loop in the worker process.

    Args:
        index: Loop index; ``0`` when the worker serves on its main loop only.
    """
    return f"{WORKER_SENDER_PREFIX}:{index}"


def build_task_infos(default_task_num: int = 1) -> List[TaskInfo]:
    """Turn every ``@trpc.task`` registration into pre-fork records.

    One :class:`TaskInfo` per child: a task declared with ``task_num: 3``
    produces ``name:0``, ``name:1`` and ``name:2``. No pipe is created and
    nothing is forked here; :func:`create_task_ipc_mesh` and the worker's
    ``_spawn_children`` do that next.

    ``pause_heartbeat()`` is called here, while still in the process that will
    fork, so the registry client stops before any child exists.

    Args:
        default_task_num: Process count of a task whose ``task_num`` is 0
            (not set); YAML ``global.processmodel.task_num``.

    Returns:
        Pre-fork records in the order they will be forked.
    """
    task_infos: List[TaskInfo] = []
    for task_handler in get_registered_tasks():
        task_num = task_handler.task_num or default_task_num
        register_task_num(task_handler.name, task_num)

        handle = TaskProcess

        for i in range(task_num):
            task = TaskInfo()
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
            if task.pause_heartbeat:
                pause_registry_heartbeat()
            task_infos.append(task)
    return task_infos


class TaskIpcMesh:
    """Every sender -> task socketpair in the process tree, created before any fork.

    A sender is one serving loop of the worker or one rpc task process, and
    every sender reaches every task. Tasks do not talk to each other.

    Everything must exist before the **first** fork: in separate mode both
    ends belong to children (an rpc task and a task), so neither can create it.

    Attributes:
        task_infos: The tasks, in fork order. Index into this list is the
            column of the mesh.
        sender_names: Everyone that may send work, in the order
            ``TaskInfo.task_pipe_conn`` uses. Index into this list is the
            ``task_conn_index`` carried by a response.
    """

    def __init__(self, task_infos: List[TaskInfo], senders: List[str]) -> None:
        """Create one socketpair per ``(sender, task)``.

        Args:
            task_infos: Pre-fork task records from :func:`build_task_infos`.
            senders: Sender names, i.e. the worker's serving loops or, in
                separate mode, the rpc task processes.
        """
        self.task_infos: List[TaskInfo] = task_infos
        self.sender_names: List[str] = list(senders)

        # Sender name -> its end toward each task, index-aligned with
        # ``task_infos``.
        self._sender_pipes: Dict[str, List[Optional[PipeConnection]]] = {
            name: [None] * len(task_infos) for name in self.sender_names
        }
        self._build()

    def _build(self) -> None:
        """Fill both sides of every ``(sender, task)`` pair with a fresh socketpair.

        The task keeps ``PipeEnd.LEFT`` and the sender ``PipeEnd.RIGHT``,
        matching what the master used to hand out, so the protocols on either
        side are unchanged.
        """
        for column, task in enumerate(self.task_infos):
            task.sender_names = list(self.sender_names)
            task.task_pipe_conn = [None] * len(self.sender_names)
            for row, sender in enumerate(self.sender_names):
                socket_pair = create_pipe(PipeKind.TCP)
                socket_pair[0].setblocking(False)
                socket_pair[1].setblocking(False)
                task.task_pipe_conn[row] = PipeConnection(socket_pair, PipeEnd.LEFT)
                self._sender_pipes[sender][column] = PipeConnection(socket_pair, PipeEnd.RIGHT)

    def sender_pipes(self, sender: str) -> List[Optional[PipeConnection]]:
        """Return one sender's end toward each task.

        Args:
            sender: Sender name from ``sender_names``.

        Returns:
            A list index-aligned with ``task_infos``; all ``None`` for a name
            that is not a sender.
        """
        return self._sender_pipes.get(sender, [None] * len(self.task_infos))

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
                sender names of its serving loops. Everything else is closed.
        """
        owned = set(owners)
        for column, task in enumerate(self.task_infos):
            for row, sender in enumerate(self.sender_names):
                task_end = task.task_pipe_conn[row]
                if task_end is None:
                    continue
                if task.name in owned:
                    # Keep the task end, drop the sender's.
                    task_end.close_extra_conn()
                elif sender in owned:
                    self._sender_pipes[sender][column].close_extra_conn()
                else:
                    # Neither side is ours: this pair belongs to two other
                    # processes entirely.
                    task_end.socket_pair[0].close()
                    task_end.socket_pair[1].close()


_task_ipc_mesh: Optional[TaskIpcMesh] = None

# TODO：删除
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

    Both halves are load-bearing. :class:`~trpc_sdk.runtime._worker.BaseWorker`
    gives a task the same startup sequence, exit template and pipe
    bookkeeping every worker has. ``Process`` supplies the process identity, because unlike a worker a task
    is never the current process: it is always forked by whoever hands it
    work. ``BaseWorker`` is listed first so its ``run()`` wins over
    ``Process.run()``, which would otherwise try to call a ``target`` that
    was never set.

    Subclasses implement :meth:`BaseWorker._prepare_to_serve` to attach the
    pipes they are fed from.
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
    """Python task child: drain worker IPC off the workers.

    Constructed in the master **before fork**. After fork,
    :class:`BaseTaskProcess` builds the event loop and plugins; this class
    only fills in task-specific wiring in :meth:`_prepare_to_serve`: wrap
    each worker socketpair in :class:`TaskConnection`, start
    :class:`TaskConsumer` threads that pull batches from the global queue,
    and schedule :func:`consume_response` to write results back.

    Shutdown is the shared exit template, with nothing to drain. Under
    ``server.mode: separate`` the record has ``separate_mode`` set and the
    task is a controlled child, like the rpc tasks: SIGTERM and SIGINT are
    ignored, and it stops on SIGUSR1 (sent by the I/O process, or the
    parent-death signal) or when the parent sentinel fires. Either way
    :meth:`_on_graceful_exit` lets the consumers finish their batch.
    """

    def __init__(self, task_info: TaskInfo, engine) -> None:
        """Record name, pipes, handler and queue opts; no sockets are bound yet.

        Runs in the master **before fork**. The event loop, plugin setup and
        IPC wiring happen later in :meth:`run` / :meth:`_prepare_to_serve`.

        Args:
            task_info: Master-side task record. ``task_handle`` is
                ``(initializer, handler)``; ``task_pipe_conn`` is index-aligned
                with the sender list.
            engine: Master engine. The child inherits this object through fork
                and reads ``opts.task_*`` defaults from it.
        """
        super().__init__(task_info.name, ProcessType.TASK)
        # Master engine that forked this child; used for task_* option defaults.
        self.server_adapter = engine
        # Master-side task record this process was spawned for.
        self.task_info: TaskInfo = task_info
        # This task's end of each sender <-> task IPC pipe, index-aligned
        # with ``task_info.sender_names``.
        self.sender_pipe_conn: List[Optional[PipeConnection]] = task_info.task_pipe_conn
        # Only read by ``_close_unowned_task_pipes``; a task sends to no other task,
        # so the inherited ``task_pipe_conn`` stays empty.
        self.task_ipc_mesh = task_ipc_mesh()

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
        # Number of :class:`TaskConsumer` threads started by this task.
        self.consumer_num: int = engine.opts.task_consumer_num
        # If True, registry heartbeats were paused at registration and must be
        # resumed in ``_prepare_to_serve`` after the initializer returns.
        self.pause_heartbeat: bool = task_info.pause_heartbeat
        # Consumer threads started in ``_create_sender_conns``; stopped on exit.
        self._consumers: List[TaskConsumer] = []
        # Forked by a separate-mode I/O process, which stops it with SIGUSR1.
        self._controlled = task_info.separate_mode

    def _create_sender_conns(self) -> None:
        """Attach each sender IPC pipe, then start consumers and the rsp pump.

        Named apart from :meth:`~trpc_sdk.runtime._worker.BaseWorker._create_pipe_conn`
        on purpose: that one opens a pipe *towards* a task, this one accepts
        every pipe arriving *at* this task.

        For every pipe: drop the unused socketpair end inherited across fork,
        wrap the remaining fd in :class:`TaskConnection`, and register it on
        the loop with its sender index, which is what a response is routed
        back on. Then start ``consumer_num`` :class:`TaskConsumer` threads
        that pull batches through ``task_handle``, and schedule
        :func:`consume_response` so completed work is written back to the
        originating sender.
        """
        for index, pipe_conn in enumerate(self.sender_pipe_conn):
            pipe_conn.close_extra_conn()
            handle = partial(TaskWorkerConnection, app=self, index=index)
            coros = self.loop.create_connection(handle, sock=pipe_conn.get_socket())
            self.loop.run_until_complete(coros)

        for _ in range(self.consumer_num):
            task_consumer = TaskConsumer(func=self.task_handle,
                                         wait_interval=self.wait_interval,
                                         batch_size=self.task_info.batch_size)
            task_consumer.start()
            self._consumers.append(task_consumer)

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
        the task is ready. The task then attaches the sender pipes and
        starts consumers.

        Under separate mode a SIGUSR1 sent while the initializer runs is only
        recorded and acted on once the loop handler is installed; a long initializer is
        cut short by the I/O process's SIGKILL after
        :data:`~trpc_sdk.runtime._constants.CHILD_EXIT_GRACE_SECONDS`.
        """
        self._close_unowned_task_pipes()

        if self.task_initializer is not None:
            self.task_initializer()
        if self.pause_heartbeat:
            resume_heartbeat()

        self._create_sender_conns()

    def _on_graceful_exit(self) -> None:
        """Let each consumer finish its batch, then return so the process exits.

        Blocks: the loop has stopped, and nobody needs it to turn any more.
        In separate mode every rpc task has exited before the I/O process
        signals this one, so no sender waits for a result. Gives up a second
        before the parent's SIGKILL, so the exit is ours rather than the
        kernel's. The consumer list is taken out of ``self`` first, so a
        second call does nothing.
        """
        consumers, self._consumers = self._consumers, []
        if not consumers:
            return
        logger.info("[task] %s stopping %d consumer(s)", self.task_info.name, len(consumers))
        for consumer in consumers:
            consumer.terminate()
        deadline = time.monotonic() + max(0.0, CHILD_EXIT_GRACE_SECONDS - 1.0)
        for consumer in consumers:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                logger.warning("[task] %s consumers still busy at exit", self.task_info.name)
                return
            consumer.join(remaining)

    def _task_pipe_owners(self) -> List[str]:
        """Return this task's own name, so ``_close_unowned_task_pipes`` keeps its task ends."""
        return [self.task_info.name]

    def _on_drained(self) -> None:
        """No user worker-exit callbacks in a task process."""
