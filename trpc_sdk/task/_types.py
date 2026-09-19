# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Shared types and process-wide registries for the task feature.

A task is a child process forked by the server before its event loop starts.
Workers reach it over an IPC pipe instead of calling the handler inline, so
the two sides need a small common vocabulary: the wire-level status codes,
the request/response records the task process queues internally, and the
name-to-connection tables the worker uses to route a call.

The registries live here rather than in :mod:`trpc_sdk.task._utils` because
:class:`TaskStreamClientMap` resolves multi-process task names through
:class:`TaskRegistry`; keeping them apart would make the two modules import
each other.
"""

from __future__ import annotations

import random
import threading
from asyncio import Future
from asyncio.transports import Transport
from collections.abc import MutableMapping
from dataclasses import dataclass
from enum import IntEnum
from enum import unique
from typing import Dict
from typing import Iterator
from typing import Optional
from typing import Tuple

from trpc_sdk.abc import BaseTaskStreamClientHandler
from trpc_sdk.abc import BaseTaskStreamServiceHandler
from trpc_sdk.utils import now_ms


@dataclass
class TaskStreamHandler:
    """Both halves of a streaming task, registered together on a task.

    The two handlers end up in different processes: ``service`` runs inside
    the task child and serves the stream, while ``client`` is kept by every
    worker so business code can open a stream towards that child. They are
    declared as one pair at registration time because a streaming task is
    unusable with only one side.
    """

    client: BaseTaskStreamClientHandler
    service: BaseTaskStreamServiceHandler


@unique
class TaskStatusCode(IntEnum):
    """Status codes carried in the head of a task response frame.

    ``IntEnum`` keeps the comparisons against the raw ``ret_code`` the C
    queue writes back valid without a cast. Mirrors the ``TaskStatusCode``
    enum in ``_cython/_task_queue.pxd``; the two definitions must stay
    numerically aligned.
    """

    SUCCESS = 0
    TIMEOUT = 1
    QUEUE_FULL = 2
    RSP_WRONG_TYPE = 3
    RSP_WRONG_LENGTH = 4


class TaskRequest:
    """One decoded request waiting in the task process request queue.

    Requests are batched, so a request can sit in the queue long enough to
    outlive the caller's deadline. ``recv_time`` is the worker-side receive
    timestamp shipped in the frame head, not the time this object was built,
    which lets the task drop already-dead work before invoking the handler.
    """

    def __init__(self, task_conn_index: int, pkg_id: int, recv_time: int, timeout: int, obj: object):
        """Initialize a queued task request.

        Args:
            task_conn_index: Index of the worker pipe the response goes back
                on. The task child is connected to every worker.
            pkg_id: Per-connection request id used to match the response to
                the caller's future.
            recv_time: Worker-side receive time in milliseconds.
            timeout: Caller deadline in milliseconds; ``0`` disables it.
            obj: Decoded request body handed to the user handler.
        """
        self.task_conn_index = task_conn_index
        self.pkg_id = pkg_id
        self.recv_time = recv_time
        self.timeout = timeout
        self.obj = obj

    def is_timeout(self) -> bool:
        """Report whether the caller deadline has already passed.

        Returns:
            ``False`` when no timeout was set, otherwise whether the wait
            since ``recv_time`` exceeds it.
        """
        return 0 < self.timeout < (now_ms() - self.recv_time)


class TaskResponse:
    """Result of one task request, addressed back to the calling worker.

    The routing fields are copied from the request rather than recomputed:
    by the time the handler returns, the batch consumer no longer holds the
    request that produced this response.
    """

    def __init__(self, req: TaskRequest, obj: object):
        """Initialize a response for an already dequeued request.

        Args:
            req: Request this response answers; supplies the return route.
            obj: Handler return value, encoded into the response body.
        """
        self.task_conn_index = req.task_conn_index
        self.pkg_id = req.pkg_id
        self.ret_code = TaskStatusCode.SUCCESS
        self.obj = obj

    def set_status_code(self, status_code: int):
        """Mark the response as failed.

        Args:
            status_code: One of the :class:`TaskStatusCode` values. The
                worker turns anything other than ``SUCCESS`` into a
                :class:`~trpc_sdk.exceptions.TaskHandleError`.
        """
        self.ret_code = status_code


class _ThreadLocalConns(MutableMapping):
    """``task name -> transport`` table private to each thread.

    An asyncio transport may only be written from the loop that created it,
    and a merge-mode worker runs one loop per thread. A single process-wide
    table would therefore hand a handler running on thread 2 a pipe owned by
    thread 0's loop, and writing it would corrupt that loop's buffers. Each
    serving loop instead opens its own socketpair to every task and registers
    it here, so ``register_task_conn`` and ``get_task_conn`` always resolve to
    the connection belonging to the caller's own loop.

    A process with a single serving loop has a single thread and sees exactly
    the behaviour of the plain dict this replaces.

    Behaves as an ordinary mutable mapping so existing ``TaskRegistry.conns``
    call sites need no change.
    """

    def __init__(self) -> None:
        self._local = threading.local()

    @property
    def _conns(self) -> Dict[str, Transport]:
        """Return this thread's table, creating it on first use."""
        conns = getattr(self._local, "conns", None)
        if conns is None:
            conns = {}
            self._local.conns = conns
        return conns

    def __getitem__(self, task_name: str) -> Transport:
        return self._conns[task_name]

    def __setitem__(self, task_name: str, conn: Transport) -> None:
        self._conns[task_name] = conn

    def __delitem__(self, task_name: str) -> None:
        del self._conns[task_name]

    def __iter__(self) -> Iterator[str]:
        return iter(self._conns)

    def __len__(self) -> int:
        return len(self._conns)


class TaskRegistry:
    """Worker-side tables mapping task names to their child processes.

    State is class-level on purpose. The tables are filled during server
    startup, before the worker event loop runs, and are then read from every
    coroutine in the process. Keys are the task names declared in the server
    config; a task declared with ``num > 1`` registers one entry per child
    under ``"<name>:<index>"`` and is reached through round-robin.
    """

    # Outgoing request counter per task pipe, wrapping at 32 bits.
    pkg_ids: Dict[Transport, int] = {}
    # (task pipe, pkg_id) -> future the caller awaits.
    rsp_futures: Dict[Tuple[Transport, int], Future] = {}
    # Task name -> pipe transport writing to that child. Per thread, because
    # each serving loop owns its own pipes; see :class:`_ThreadLocalConns`.
    conns: MutableMapping = _ThreadLocalConns()
    # Task name -> number of children forked for it.
    nums: Dict[str, int] = {}
    # Task name -> child index returned by the last round-robin pick.
    cur_select: Dict[str, int] = {}

    @classmethod
    def next_child_name(cls, task_name: str) -> Optional[str]:
        """Advance the round-robin cursor of a multi-process task.

        Args:
            task_name: Task name as declared in the server config, without
                a child index.

        Returns:
            ``"<task_name>:<index>"`` for the chosen child, or ``None`` when
            no child count was ever registered for this name.
        """
        if task_name not in cls.nums:
            return None

        task_num = cls.nums[task_name]
        if task_name in cls.cur_select:
            index = (cls.cur_select[task_name] + 1) % task_num
        else:
            # Every worker inherits this table from the same fork, so a fixed
            # starting index would point all workers at child 0.
            index = random.randrange(task_num)
        cls.cur_select[task_name] = index
        return f"{task_name}:{index}"


class TaskStreamClientMap:
    """Stream client handlers of the running tasks, keyed by task name.

    Populated by the worker once per task child at startup. Business code
    looks a handler up by the plain task name; multi-process tasks are
    registered under their indexed names and resolved through
    :meth:`TaskRegistry.next_child_name`.
    """

    _stream_client_map: Dict[str, BaseTaskStreamClientHandler] = {}

    @classmethod
    def register_task_stream_client(cls, name: str, task_stream_client: BaseTaskStreamClientHandler):
        """Publish the stream client of one task child.

        Args:
            name: Task name, or ``"<name>:<index>"`` for a task with more
                than one child.
            task_stream_client: Client half of that task's stream handler.
        """
        cls._stream_client_map[name] = task_stream_client

    @classmethod
    def get_task_stream_client(cls, name: str) -> Optional[BaseTaskStreamClientHandler]:
        """Look up the stream client to use for a task.

        Args:
            name: Task name as declared in the server config.

        Returns:
            The registered handler, or ``None`` when the name is unknown or
            the selected child has not registered yet.
        """
        if name in cls._stream_client_map:
            return cls._stream_client_map[name]

        child_name = TaskRegistry.next_child_name(name)
        if child_name is None:
            return None
        return cls._stream_client_map.get(child_name, None)
