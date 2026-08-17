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

* :class:`TaskInfo` — the master-side record for one task child, filled before
  fork with pipes, handler and process class (same role as
  :class:`~trpc.server._worker.WorkerInfo` / :class:`~trpc.server._dispatcher.DispatcherInfo`).
* :class:`TaskProcess` — the Python implementation: an event loop feeding a
  global queue that :class:`~trpc.task.consumer.TaskConsumer` threads drain.
* :class:`CTaskProcess` — the native implementation, which hands the task
  loop to the C driver and therefore has no Python event loop at all.
"""

from __future__ import annotations

import asyncio
import importlib
import os
from dataclasses import dataclass
from dataclasses import field
from functools import partial
from multiprocessing import Process
from typing import Any
from typing import Callable
from typing import Dict
from typing import List
from typing import Optional
from typing import Tuple
from typing import Type

from trpc_sdk.common import ProcessType
from trpc_sdk.log import logger
from trpc_sdk.naming import resume_heartbeat
from trpc_sdk.stream import BaseTaskStreamServiceHandler
from trpc_sdk.stream import TaskStreamServiceAdapter
from trpc_sdk.task import TaskConsumer
from trpc_sdk.task import TaskStreamHandler
from trpc_sdk.task import consume_response
from trpc_sdk.task import get_default_codec
from trpc_sdk.task import task_util_c
from trpc_sdk.transport import TaskConnection
from trpc_sdk.transport import TaskServerStreamConnectionHandler
from trpc_sdk.utils import unlink_unix_socket_file

from ._child_process import BaseChildProcess
from ._pipe import PipeConnection


@dataclass
class TaskInfo:
    """Master-side record for one task child process.

    The engine fills this **before fork**: process class, handler pair, IPC
    pipes, and C-task flags. After ``start()``, ``process_instance`` holds the
    live child. Copied from the user-facing
    :class:`~trpc.trpc_abc.task.Task` registration in
    ``TrpcServerEngine.init_task_process``.
    """

    # Stable name ``{registered_name}:{index}`` used for IPC lookup and
    # Unix-domain path ``.__{name}`` for stream tasks.
    name: str = ""
    # ``multiprocessing.Process`` subclass to spawn: ``TaskProcess`` or
    # ``CTaskProcess``. Set in ``init_task_process`` before fork.
    handle: Optional[Type[Process]] = None
    # ``(initializer, handler)`` from the user registration; the child
    # reads these after fork.
    task_handle: Optional[Tuple[Optional[Callable[..., Any]], Callable[..., Any]]] = None
    # Live child after ``start()``. The master stores the ``Process``
    # here to join / inspect pid.
    process_instance: Optional[Process] = None
    # This task's end of each worker <-> task IPC pipe, index-aligned
    # with ``engine.worker_process_list``. ``None`` for stream tasks,
    # which connect over a Unix-domain socket instead.
    task_pipe_conn: List[Optional[PipeConnection]] = field(default_factory=list)
    # Batch size for :class:`~trpc.task.consumer.TaskConsumer`; ``0``
    # means fall back to ``engine.opts.task_batch_size``.
    batch_size: int = 0
    # Native C-task thread count; ignored by the Python ``TaskProcess``.
    thread_num: int = 0
    # Optional opaque model metadata passed through to the C driver.
    model_info: Any = None
    # Native C-task reactor count; used by ``ctask.CTask.set_task_info``.
    reactor_num: int = 1
    # If True, spawn ``CTaskProcess``; otherwise ``TaskProcess``.
    use_c_task: bool = True
    # Non-empty selects ``trpc_driver.run_c_task_process`` over the
    # default ``python_handler`` + ``CTask`` path.
    c_task_driver_params: Dict[str, Any] = field(default_factory=dict)
    # Optional callback that replaces ``RegisterTask`` on the default
    # C-task path when set.
    on_fork: Optional[Callable[..., Any]] = None
    # If True, registry heartbeats were paused at registration and must
    # be resumed after the task initializer returns.
    pause_heartbeat: bool = False
    # Stream client/service pair; ``None`` for non-stream tasks.
    task_stream_handler: Optional[TaskStreamHandler] = None

    @property
    def pid(self):
        """OS pid of the live child.

        Returns:
            ``process_instance.pid``. Raises ``AttributeError`` if called
            before the child has been started.
        """
        return self.process_instance.pid


class TaskProcess(BaseChildProcess):
    """Python task child: drain worker IPC (or a Unix stream socket) off the workers.

    Constructed in the master **before fork**. After fork,
    :class:`BaseChildProcess` builds the event loop and plugins; this class
    only fills in task-specific wiring in :meth:`_prepare_to_serve`.

    Two serving modes:

    * Stream task — bind ``.__{name}`` and accept full-duplex Unix connections
      from workers. There is no socketpair; the matching
      ``task_pipe_conn`` entries are ``None``.
    * Non-stream task — wrap each worker socketpair in :class:`TaskConnection`,
      start :class:`TaskConsumer` threads that pull batches from the global
      queue, and schedule :func:`consume_response` to write results back.

    ``run()`` and SIGTERM handling stay on :class:`BaseChildProcess`.
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
        # This task's end of each worker <-> task IPC pipe, index-aligned with
        # ``engine.worker_process_list``. ``None`` for stream tasks, which
        # connect over a Unix-domain socket instead.
        self.task_pipe_conn: List[Optional[PipeConnection]] = task_info.task_pipe_conn
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
        handler = partial(TaskServerStreamConnectionHandler, service_adapter=service_adapter, loop=self.loop)
        if os.path.exists(self._unix_path):
            logger.info(f"'{self._unix_path}' file is already exist, must delete to avoid error")
            unlink_unix_socket_file(self._unix_path)
        coros = self.loop.create_unix_server(handler, path=self._unix_path)
        self._unix_srv = self.loop.run_until_complete(coros)

    def _create_pipe_conn(self) -> None:
        """Attach each worker IPC pipe, then start consumers and the rsp pump.

        For every non-``None`` pipe: drop the unused socketpair end inherited
        across fork, wrap the remaining fd in :class:`TaskConnection`, and
        register it on the loop. Then start ``consumer_num``
        :class:`TaskConsumer` threads that pull batches through
        ``task_handle``, and schedule :func:`consume_response` so completed
        work is written back to the originating worker.

        Called only for non-stream tasks, so each ``task_pipe_conn`` entry is
        a live :class:`PipeConnection`.
        """
        for index, pipe_conn in enumerate(self.task_pipe_conn):
            pipe_conn.close_extra_conn()
            handle = partial(TaskConnection, app=self, index=index)
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
        task_util_c.task_init(self.queue_size)
        super()._init_plugins()

    def _prepare_to_serve(self) -> None:
        """Run the user initializer, then accept work from the workers.

        If ``pause_heartbeat`` was set at registration, registry heartbeats
        resume after the initializer so the service is not advertised until
        the task is ready. A stream task then binds its Unix socket; a
        non-stream task attaches the worker pipes and starts consumers.
        """
        if self.task_initializer is not None:
            self.task_initializer()
        if self.pause_heartbeat:
            resume_heartbeat()

        if self._task_stream_service:
            self._create_unix_server()
        else:
            self._create_pipe_conn()

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


class CTaskProcess(Process):
    """Native task child: the C driver owns the loop, so there is no asyncio.

    Does **not** extend :class:`BaseChildProcess`. The native driver runs the
    whole task loop, so this process has no Python event loop and does not
    share the base class's plugin / SIGTERM / ``run_forever`` sequence.

    :meth:`run` picks a driver from ``task_info.c_task_driver_params``:

    * non-empty — :meth:`_run_trpc_driver` hands control to
      ``trpc_driver.run_c_task_process``.
    * empty — :meth:`_run_default_driver` registers the Python handler with
      ``python_handler.RegisterTask`` (unless ``on_fork`` replaces that) and
      starts :class:`~trpc.task.ctask.CTask`.
    """

    def __init__(self, task_info: TaskInfo, engine) -> None:
        """Record name, pipes, handler and driver flags; nothing is started yet.

        Runs in the master **before fork**. :meth:`run` is the first code that
        executes inside the new process.

        Args:
            task_info: Master-side task record. ``c_task_driver_params``
                non-empty selects the trpc_driver path.
            engine: Master engine, inherited through fork. Used for
                ``opts.task_wait_interval`` / ``opts.task_batch_size``.
        """
        super().__init__()
        # Process role used by the master for health checks and restarts.
        self.type = ProcessType.TASK
        # Process name shown in logs and process listings.
        self.name = f"c_task_{task_info.name}"
        # Master engine that forked this child.
        self.server_engine = engine
        # This task's end of each worker <-> task IPC pipe; ``None`` for
        # stream tasks. The native driver reads the list from ``task_info``.
        self.task_pipe_conn: List[Optional[PipeConnection]] = task_info.task_pipe_conn
        # Master-side task record this process was spawned for.
        self.task_info: TaskInfo = task_info
        # Optional user callback run once after fork; ``task_handle[0]``.
        self.task_initializer: Optional[Callable[..., Any]] = task_info.task_handle[0]
        # User handler registered with the native driver; ``task_handle[1]``.
        self.task_handle: Callable[..., Any] = task_info.task_handle[1]
        # Milliseconds a consumer sleeps when the inbound queue is empty.
        self.wait_interval: int = engine.opts.task_wait_interval
        # If True, registry heartbeats were paused at registration and must be
        # resumed after the initializer (wrapped into ``RegisterTask``).
        self.pause_heartbeat: bool = task_info.pause_heartbeat
        if self.task_info.batch_size <= 0:
            self.task_info.batch_size = engine.opts.task_batch_size

        # Always ``None``: the native driver owns the loop, unlike TaskProcess.
        self.loop: Optional[asyncio.AbstractEventLoop] = None
        # Kept for API parity with :class:`BaseChildProcess`; unused here.
        self.block: bool = True
        # True when ``c_task_driver_params`` is non-empty: use trpc_driver.
        self.c_task_driver: bool = len(self.task_info.c_task_driver_params) > 0

    def run(self) -> None:
        """Child entry point: pick the native driver and hand it the task."""
        if self.c_task_driver:
            self._run_trpc_driver()
        else:
            self._run_default_driver()

    def _run_default_driver(self) -> None:
        """Register the Python handler (unless ``on_fork``), then start CTask.

        ``task_info.on_fork``, when set, replaces ``RegisterTask`` entirely.
        Otherwise the handler is registered with ``python_handler``; if
        ``pause_heartbeat`` is set, the initializer is wrapped so
        :func:`resume_heartbeat` runs after it. :class:`~trpc.task.ctask.CTask`
        then takes ``task_info`` and blocks in ``start()``.
        """
        if self.task_info.on_fork:
            self.task_info.on_fork()
        else:
            python_handler = importlib.import_module("trpc_sdk.task.python_handler")
            if self.pause_heartbeat:

                def with_resume_heartbeat_initializer(initializer):
                    """Run ``initializer`` if set, then resume registry heartbeats."""
                    if initializer is not None:
                        initializer()
                    resume_heartbeat()

                python_handler.RegisterTask(with_resume_heartbeat_initializer(self.task_initializer), self.task_handle,
                                            self.task_info.batch_size)
            else:
                python_handler.RegisterTask(self.task_initializer, self.task_handle, self.task_info.batch_size)

        ctask = importlib.import_module("trpc_sdk.task.ctask")
        task = ctask.CTask()
        task.set_task_info(self.task_info)
        task.start()

    def _run_trpc_driver(self) -> None:
        """Hand the process to ``trpc_driver.run_c_task_process``.

        Log level and log file come from ``c_task_driver_params``, defaulting
        to level ``4`` and ``./trpc_task_driver.log``.
        """
        trpc_driver = importlib.import_module("trpc_driver")
        log_level = self.task_info.c_task_driver_params.get("log_level", 4)
        log_file = self.task_info.c_task_driver_params.get("log_file", "./trpc_task_driver.log")
        trpc_driver.run_c_task_process(self.task_info,
                                       get_default_codec(),
                                       self.task_initializer,
                                       self.task_handle,
                                       log_level=log_level,
                                       log_file=log_file)
