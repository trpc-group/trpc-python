# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Shared types for runtime components.

Holds the pre-fork records the worker / engine fill before any child exists.
Implementation stays in ``_worker`` and ``_task``.
"""

from __future__ import annotations

import socket
from dataclasses import dataclass
from dataclasses import field
from multiprocessing import Process
from typing import Any
from typing import Callable
from typing import Dict
from typing import List
from typing import Optional
from typing import Tuple
from typing import Type

from trpc_sdk.transport import encode_worker_msg_req
from trpc_sdk.utils import PipeConnection

@dataclass
class ChildProcessInfo:
    """One child this worker forked, plus how to build a replacement.

    A ``multiprocessing.Process`` can only be started once, so restarting a
    dead child would mean constructing a new one; the factory is kept for
    that. Restarting is not supported yet (``reboot_child`` is treated as
    false). Handing the replacement the same pipes would not work: the parent
    closed the child's ends after the first fork, and a half-written frame
    left in a socket buffer would desynchronize the parser on the far side.
    A restart has to rebuild every IPC edge of the child, which needs a
    framework-level fd registry or task endpoints reached by name.
    """

    # Child name, used in log messages.
    name: str
    # Builds a fresh, not-yet-started child process object.
    factory: Callable[[], Any]
    # The live child after ``start()``; ``None`` before the first fork.
    process: Optional[Any] = None
    # Set once SIGCHLD reaped the child. ``Process.is_alive()`` cannot be
    # trusted after that: ``waitpid`` already collected the status, so
    # ``multiprocessing``'s own poll fails with ECHILD and reports it alive.
    exited: bool = False


@dataclass
class WorkerInfo:
    """Pre-fork record for one worker process.

    A custom mode that still forks workers fills this before fork: worker
    id, IPC pipes, and which ``Process`` subclass to spawn. After
    ``start()``, ``process_instance`` holds the live child. ``merge`` and
    ``separate`` do not consume this record.
    """

    # Worker index in ``worker_process_list``; used as the from/to id
    # on the inter-worker message pipe.
    id: int = 0
    # ``multiprocessing.Process`` subclass to spawn. Set by a custom mode
    # that still forks workers, right before fork.
    handle: Optional[Type[Process]] = None
    # Live child after ``start()``. The parent stores the ``Process``
    # here to join / inspect pid; after fork the child overwrites it
    # with ``self`` so ``pid`` is this process.
    process_instance: Optional[Process] = None
    # This worker's end (``PipeEnd.LEFT``) of the req/rsp IPC pipe
    # toward the process that accepts client connections. The acceptor
    # owns ``PipeEnd.RIGHT`` of the same pair. Always a stream socket.
    pipe_conn: Optional[PipeConnection] = None
    # Raw socketpair backing ``pipe_conn``. Kept so the acceptor
    # can wrap the other end as ``PipeConnection(..., PipeEnd.RIGHT)``.
    socket_pair: Optional[Tuple[socket.socket, socket.socket]] = None
    # This worker's end (``PipeEnd.LEFT``) of the RPCZ span side-channel.
    # The worker writes finished spans here; the acceptor reads them
    # and forwards to the RPCZ collector. Always a stream pair.
    rpcz_pipe_conn: Optional[PipeConnection] = None
    # Raw socketpair backing ``rpcz_pipe_conn``.
    rpcz_socket_pair: Optional[Tuple[socket.socket, socket.socket]] = None
    # This worker's read end (``PipeEnd.LEFT``) of the inter-worker
    # message pipe. Nothing listens on it since ``on_worker_message`` was
    # removed; kept for custom modes that still fill this record.
    read_message_pipe: Optional[PipeConnection] = None
    # Write end (``PipeEnd.RIGHT``) of that same message pipe. The parent
    # (and ``send_msg``) push encoded frames here; the worker never
    # writes this end.
    write_message_pipe: Optional[PipeConnection] = None
    # This worker's end of each worker <-> task IPC pipe, index-aligned
    # with ``task_name``. ``None`` for a task this process has no pipe to.
    task_pipe_conn: List[Optional[PipeConnection]] = field(default_factory=list)
    # Task names paired with ``task_pipe_conn``.
    task_name: List[str] = field(default_factory=list)

    @property
    def pid(self) -> int:
        """OS pid of the live child, or of this process after fork.

        Returns:
            ``process_instance.pid``. Raises ``AttributeError`` if called
            before the child has been started.
        """
        return self.process_instance.pid

    def send_msg(self, from_worker: int, data: str) -> None:
        """Push an inter-worker message onto this worker's write pipe.

        Called from the master via ``send_msg_to_worker``. Encodes
        ``from_worker`` plus ``data`` with ``encode_worker_msg_req`` and
        writes the frame to ``write_message_pipe``. The framework no longer
        reads that pipe; a custom mode that fills this record must.

        Args:
            from_worker: Sender worker id, written into the frame.
            data: UTF-8 text payload.
        """
        buf = encode_worker_msg_req(from_worker, data.encode("utf-8"))
        self.write_message_pipe.send(buf)


@dataclass
class TaskInfo:
    """Master-side record for one task child process.

    The engine fills this **before fork**: process class, handler pair, IPC
    pipes, and C-task flags. After ``start()``, ``process_instance`` holds the
    live child. Copied from the user-facing
    :class:`~trpc_sdk.abc.Task` registration in
    ``TrpcServerAdapter.init_task_process``.
    """

    # Stable name ``{registered_name}:{index}`` used for IPC lookup.
    name: str = ""
    handle: Optional[Type[Process]] = None
    # ``(initializer, handler)`` from the user registration; the child
    # reads these after fork.
    task_handle: Optional[Tuple[Optional[Callable[..., Any]], Callable[..., Any]]] = None
    # Live child after ``start()``. The master stores the ``Process``
    # here to join / inspect pid.
    process_instance: Optional[Process] = None
    # This task's end of each sender <-> task IPC pipe, index-aligned with
    # ``sender_names``.
    task_pipe_conn: List[Optional[PipeConnection]] = field(default_factory=list)
    # Who may send work to this task, in the order ``task_pipe_conn`` uses.
    # The index into this list is the ``task_conn_index`` a response carries,
    # so it is what routes a result back to the process that asked for it.
    sender_names: List[str] = field(default_factory=list)
    # Batch size for :class:`~trpc_sdk.task.TaskConsumer`; ``0``
    # means fall back to ``engine.opts.task_batch_size``.
    batch_size: int = 0
    # The four fields below configured the native C-task driver, which this
    # build no longer ships. They are still carried from the registration so
    # existing configs keep starting, but nothing reads them any more.
    thread_num: int = 0
    model_info: Any = None
    reactor_num: int = 1
    # Optional callback that replaced ``RegisterTask`` on the native path;
    # likewise unread now.
    on_fork: Optional[Callable[..., Any]] = None
    # If True, registry heartbeats were paused at registration and must
    # be resumed after the task initializer returns.
    pause_heartbeat: bool = False
    # True when the task is forked by a separate-mode I/O process: it then
    # ignores SIGTERM and SIGINT and stops on SIGUSR1 from that process.
    separate_mode: bool = False

    @property
    def pid(self) -> int:
        """OS pid of the live child.

        Returns:
            ``process_instance.pid``. Raises ``AttributeError`` if called
            before the child has been started.
        """
        return self.process_instance.pid
