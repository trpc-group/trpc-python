# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Shared types for server components.

Holds the queue-wait window used by ``Service``, the merge-mode accept
target, and the pre-fork records the worker / engine fill before any child
exists. Implementation stays in ``_acceptor``, ``_worker`` and ``_task``.
"""

from __future__ import annotations

import asyncio
import socket
from dataclasses import dataclass
from dataclasses import field
from enum import IntEnum
from enum import unique
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
from ._loop_thread import LoopThread


@unique
class QueueWaitPolicy(IntEnum):
    """Queue-wait circuit-breaker policy (YAML ``server.queue_wait_policy``).

    Compared with ``ServiceOption.queue_wait_policy`` in
    :meth:`trpc_sdk.server._cython._service.Service._check_queue_wait`.
    """

    # Drop the request as soon as it exceeds the queue-wait limit.
    DEFAULT = 0
    # Drop only when the sliding window (QueueWindow) is also overloaded.
    SLIDING_WINDOW = 1


@dataclass
class Bucket:
    """One time-slice of queue-wait samples in a two-bucket sliding window."""

    # Number of requests recorded in this time slice.
    total: int = 0
    # Number of requests that exceeded the queue-wait timeout.
    failed: int = 0
    # Time-slice index (``current_ms // interval``) currently stored in this bucket.
    index: int = 0


class QueueWindow:
    """Two-bucket sliding window used for queue-wait circuit breaking.

    Time is divided into slices of ``interval`` milliseconds. The current and
    previous slices occupy ``windows_list[index % 2]``. When a slice exceeds
    ``threshold`` samples and its failure ratio is above ``ratio``, the window
    is treated as overloaded and ``report_queue_time`` returns ``False``.
    """

    def __init__(self, interval: int = 60 * 1000, ratio: float = 0.1, threshold: int = 10):
        # Two rotating buckets for the current and previous time slices.
        self.windows_list: List[Bucket] = [Bucket() for _ in range(2)]
        # Time-slice index of the bucket most recently written.
        self.cur: int = 0
        # Reserved previous-slice index; unused by the current algorithm.
        self.last: int = 0
        # Length of one time slice, in milliseconds.
        self.interval: int = interval
        # Failure-ratio limit; exceeding it marks the window unhealthy.
        self.ratio: float = ratio
        # Minimum samples in a slice before the failure ratio is considered.
        self.threshold: int = threshold

    def report_queue_time(self, current: float, status: bool) -> bool:
        """Record one queue-wait sample and return whether the window is healthy.

        Args:
            current: Current timestamp in milliseconds.
            status: ``True`` if the request did not time out in the queue;
                ``False`` if it exceeded the queue-wait limit.

        Returns:
            ``False`` if the current or previous slice is overloaded;
            ``True`` otherwise.
        """
        # Map the timestamp onto a slice, then onto one of the two rotating buckets.
        time_index = int(current / self.interval)
        bucket_index = time_index % 2

        bucket = self.windows_list[bucket_index]
        if time_index != bucket.index:
            # This bucket still holds an older slice; reset it for the new one.
            self.cur = time_index
            self.windows_list[bucket_index].total = 0
            self.windows_list[bucket_index].failed = 0
            self.windows_list[bucket_index].index = time_index

        bucket.total = bucket.total + 1
        if status is False:
            bucket.failed = bucket.failed + 1

        if bucket.total > self.threshold and bucket.failed / bucket.total > self.ratio:
            return False

        return self.get_last_windows_status(current)

    def get_last_windows_status(self, current: float) -> bool:
        """Return whether the previous time slice is healthy.

        Args:
            current: Current timestamp in milliseconds.

        Returns:
            ``False`` if the previous slice exists and is overloaded;
            ``True`` if it is missing or still within limits.
        """
        current_index = int(current / self.interval)
        last_time_index = current_index - 1
        last_bucket = self.windows_list[last_time_index % 2]
        if last_bucket.index != last_time_index:
            # No samples were recorded in the previous slice.
            return True

        if last_bucket.total > self.threshold and last_bucket.failed / last_bucket.total > self.ratio:
            return False

        return True


@dataclass
class AcceptTarget:
    """One loop thread plus the protocol factory to use for sockets sent to it.

    The factory is per target rather than per acceptor because some listeners
    cannot share one across loops. An aiohttp ``AppRunner`` builds a ``Server``
    bound to whichever loop was running during ``setup()``, so each thread
    needs its own runner and therefore its own factory. Plain tRPC protocols
    have no such constraint and every target repeats the same factory.
    """

    # Thread whose loop will own the transport for connections sent here.
    thread: LoopThread
    # Builds the protocol serving one connection on ``thread``'s loop.
    protocol_factory: Callable[[], asyncio.BaseProtocol]


@dataclass
class ChildRecord:
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
    # with ``self`` so ``current_worker().process_instance`` is this
    # process.
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
    # message pipe. ``start_message_listener`` attaches
    # ``WorkerMsgConnection`` here and delivers payloads to
    # ``on_worker_message``.
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
        writes the frame to ``write_message_pipe``. The worker's
        ``WorkerMsgConnection`` reads it and invokes ``on_worker_message``.

        Args:
            from_worker: Sender worker id, forwarded to the callback.
            data: UTF-8 text payload delivered to ``on_worker_message``.
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
    ``TrpcServerEngine.init_task_process``.
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
    # This task's end of each producer <-> task IPC pipe, index-aligned with
    # ``producer_names``. ``None`` where that producer has no pipe to this
    # task: a peer task which did not declare this one.
    task_pipe_conn: List[Optional[PipeConnection]] = field(default_factory=list)
    # Who may send work to this task, in the order ``task_pipe_conn`` uses.
    # The index into this list is the ``task_conn_index`` a response carries,
    # so it is what routes a result back to the process that asked for it.
    producer_names: List[str] = field(default_factory=list)
    # Task names this task declared with ``Task(peers=[...])``. Base names,
    # without a child index: declaring ``"infer"`` reaches every child of it.
    peers: List[str] = field(default_factory=list)
    # Batch size for :class:`~trpc_sdk.task.TaskConsumer`; ``0``
    # means fall back to ``engine.opts.task_batch_size``.
    batch_size: int = 0
    # The four fields below configured the native C-task driver, which this
    # build no longer ships. They are still carried from the registration so
    # existing configs keep starting, but nothing reads them any more.
    thread_num: int = 0
    model_info: Any = None
    reactor_num: int = 1
    use_c_task: bool = True
    c_task_driver_params: Dict[str, Any] = field(default_factory=dict)
    # Optional callback that replaced ``RegisterTask`` on the native path;
    # likewise unread now.
    on_fork: Optional[Callable[..., Any]] = None
    # If True, registry heartbeats were paused at registration and must
    # be resumed after the task initializer returns.
    pause_heartbeat: bool = False

    @property
    def pid(self) -> int:
        """OS pid of the live child.

        Returns:
            ``process_instance.pid``. Raises ``AttributeError`` if called
            before the child has been started.
        """
        return self.process_instance.pid
