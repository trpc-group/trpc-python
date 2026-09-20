# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Shared constants for :mod:`trpc_sdk.utils`.

This module is the bottom of the utils import graph: it must not import
any other ``trpc_sdk.utils`` submodule. Enums and string constants live here
so :mod:`trpc_sdk.utils._types` and the other private modules can share them
without cycles.

:data:`UNIX_TYPE` is the socket-family label stored on service config,
not a Python ``socket.AF_UNIX`` value. :class:`ProcessType` is stamped
onto ``multiprocessing.current_process()`` after fork.
:class:`TcpState` mirrors Linux ``tcp_info.tcpi_state`` for
:func:`trpc_sdk.utils._network.is_close_wait`.
:class:`TimerType` selects how :class:`~trpc_sdk.utils._timer.TimerExecutor`
schedules a task.
"""

from __future__ import annotations

from enum import Enum
from enum import IntEnum
from enum import unique

# Label compared by :func:`trpc_sdk.utils._network.is_unix_socket`. Config
# stores this string; it is not ``socket.AF_UNIX``.
UNIX_TYPE = "unix"


class ProcessType(Enum):
    """Role of a tRPC child process.

    Stamped onto ``multiprocessing.current_process().type`` after fork
    so :func:`trpc_sdk.utils._multi_process.get_process_type` can branch
    without a process-wide registry.

    Attributes:
        MANAGER: Parent that owns listen sockets and spawns children.
        WORKER: Request-handling process.
        DISPATCHER: Accepts connections and forwards frames to workers.
        TASK: Dedicated task-process (share-memory / stream task).
        TIMER: Process that only runs :class:`~trpc_sdk.utils._timer.TimerExecutor`.
        UNKNOWN: Default before a type is assigned, or an unclassified process.
    """

    MANAGER = 1
    WORKER = 2
    DISPATCHER = 3
    TASK = 4
    TIMER = 5
    UNKNOWN = 6


class TcpState(IntEnum):
    """Linux ``tcp_info.tcpi_state`` values used with ``TCP_INFO``.

    Integer values match the kernel enum starting at
    ``TCP_ESTABLISHED = 1``. :func:`trpc_sdk.utils._network.is_close_wait`
    reads only the first byte of the ``TCP_INFO`` blob and compares it
    to :attr:`TCP_CLOSE_WAIT`. macOS is skipped at the call site because
    ``TCP_INFO`` is not available there on the Python versions we support.
    """

    TCP_ESTABLISHED = 1
    TCP_SYN_SENT = 2
    TCP_SYN_RECV = 3
    TCP_FIN_WAIT1 = 4
    TCP_FIN_WAIT2 = 5
    TCP_TIME_WAIT = 6
    TCP_CLOSE = 7
    TCP_CLOSE_WAIT = 8
    TCP_LAST_ACK = 9
    TCP_LISTEN = 10
    TCP_CLOSING = 11


@unique
class TimerType(Enum):
    """How :class:`~trpc_sdk.utils._timer.TimerExecutor` schedules one task.

    Attributes:
        ONCE: Run ``handle`` once after ``interval`` seconds.
        REPEAT_WITH_CRON: Repeat on an aiocron spec (``TimerTask.spec``).
        REPEAT_WITH_SECOND: Repeat every ``interval`` seconds via
            :meth:`trpc_sdk.utils._timer.TimerTask.start`.
    """

    ONCE = 0
    REPEAT_WITH_CRON = 1
    REPEAT_WITH_SECOND = 2
