# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Dispatcher-side worker selection.

After the dispatcher splits a complete frame, it must choose which worker IPC
pipe to write it to. Streaming frames are sticky: the same
``(session_id, stream_id)`` always lands on the same worker so stream state
stays in one process. Unary frames use a registered scheduler when one exists,
otherwise a process-local round-robin.

``register_dispatcher_worker_func`` is the hook existing business code uses to
replace that unary policy.
"""

from __future__ import annotations

from typing import Callable
from typing import Optional

from trpc_sdk._inner import trpc_pb2

from ._types import SchedulerParams

# Round-robin cursor for the default unary scheduler; lives in this process.
_send_worker_index: int = 0

# Legacy unary hook: ``func(worker_conn_num) -> worker_index``. Kept for
# existing business that registered before SchedulerParams existed.
_dispatcher_worker_func: Optional[Callable[[int], int]] = None
# Current unary hook: ``func(SchedulerParams) -> worker_index``.
_worker_scheduler_func: Optional[Callable[[SchedulerParams], int]] = None


def _default_dispatcher_worker(schedule: SchedulerParams) -> int:
    """Advance the round-robin cursor and return the next worker index.

    Args:
        schedule: Frame routing inputs; only ``worker_conn_num`` is used.
    """
    global _send_worker_index  # pylint: disable=global-statement
    _send_worker_index = (_send_worker_index + 1) % schedule.worker_conn_num
    return _send_worker_index


def get_next_worker(schedule: SchedulerParams) -> int:
    """Return the worker index that should handle this frame.

    Streaming frames (``frame_type == TRPC_STREAM_FRAME``) bypass registered
    hooks and hash ``session_id + stream_id`` so one stream stays on one
    worker. Unary frames try ``_worker_scheduler_func``, then the legacy
    ``_dispatcher_worker_func``, then round-robin.

    Args:
        schedule: Frame routing inputs built by the dispatcher.
    """
    if schedule.frame_type == trpc_pb2.TRPC_STREAM_FRAME:
        # Sticky route: the same stream must not hop between workers.
        return (schedule.session_id + schedule.stream_id) % schedule.worker_conn_num
    if _worker_scheduler_func:
        return _worker_scheduler_func(schedule)
    if _dispatcher_worker_func:
        return _dispatcher_worker_func(schedule.worker_conn_num)
    return _default_dispatcher_worker(schedule)


def register_dispatcher_worker_func(
        dispatcher_worker: Optional[Callable[[SchedulerParams], int]],
        is_scheduler: bool = False) -> None:
    """Install or clear a custom unary worker-selection hook.

    Passing ``None`` clears that hook. Streaming frames ignore both hooks.

    Args:
        dispatcher_worker: Selector to install, or ``None`` to clear.
            When ``is_scheduler`` is false the callable is the legacy form
            ``func(worker_conn_num) -> index``; when true it is
            ``func(SchedulerParams) -> index``.
        is_scheduler: ``True`` to set the SchedulerParams hook; ``False``
            to set the legacy ``worker_conn_num`` hook.
    """
    global _dispatcher_worker_func, _worker_scheduler_func  # pylint: disable=global-statement
    if is_scheduler:
        _worker_scheduler_func = dispatcher_worker
    else:
        _dispatcher_worker_func = dispatcher_worker
