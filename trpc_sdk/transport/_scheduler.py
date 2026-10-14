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

The separate-mode I/O process goes through :func:`select_rpc_task`, which
applies the same rules but never returns an rpc task that has died: a unary
frame falls back to the next live one, a stream frame whose rpc task died has
nowhere to go.
"""

from __future__ import annotations

from typing import Callable
from typing import Optional
from typing import Sequence

from trpc_sdk._inner import trpc_pb2
from trpc_sdk.log import logger

from ._types import SchedulerParams

# Round-robin cursor for the default unary scheduler; lives in this process.
_send_worker_index: int = 0

# Legacy unary hook: ``func(worker_conn_num) -> worker_index``. Kept for
# existing business that registered before SchedulerParams existed.
_dispatcher_worker_func: Optional[Callable[[int], int]] = None
# Current unary hook: ``func(SchedulerParams) -> worker_index``.
_worker_scheduler_func: Optional[Callable[[SchedulerParams], int]] = None
# Set once a hook returned an unusable rpc task index, so the fallback is
# reported once per process rather than once per frame.
_warned_bad_hook_index: bool = False


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


def _call_unary_hook(schedule: SchedulerParams) -> Optional[int]:
    """Return what the registered unary hook picks, or ``None`` without one."""
    if _worker_scheduler_func:
        return _worker_scheduler_func(schedule)
    if _dispatcher_worker_func:
        return _dispatcher_worker_func(schedule.worker_conn_num)
    return None


def select_rpc_task(schedule: SchedulerParams, alive: Sequence[bool]) -> Optional[int]:
    """Pick the rpc task for one frame, skipping dead ones.

    Stream frames keep the sticky hash and are never moved: the stream's
    state lives in the process the hash points at. Unary frames try the user
    hook, then round-robin; a hook result that is out of range or dead is
    warned about once and replaced by round-robin.

    Args:
        schedule: Frame routing inputs; ``worker_conn_num`` must equal
            ``len(alive)``.
        alive: Per rpc task, whether it can still take work.

    Returns:
        The rpc task index, or ``None`` when the frame has nowhere to go: no
        rpc task is alive, or the stream's rpc task died.
    """
    global _warned_bad_hook_index  # pylint: disable=global-statement
    count = schedule.worker_conn_num
    if count <= 0:
        return None
    if schedule.frame_type == trpc_pb2.TRPC_STREAM_FRAME:
        index = (schedule.session_id + schedule.stream_id) % count
        return index if alive[index] else None

    picked = _call_unary_hook(schedule)
    if picked is not None:
        if isinstance(picked, int) and 0 <= picked < count and alive[picked]:
            return picked
        if not _warned_bad_hook_index:
            _warned_bad_hook_index = True
            logger.warning("[scheduler] custom scheduler returned %r, which is not a live rpc task in [0, %s); "
                           "falling back to round-robin (reported once)", picked, count)

    for _ in range(count):
        index = _default_dispatcher_worker(schedule)
        if alive[index]:
            return index
    return None


def register_dispatcher_worker_func(
        dispatcher_worker: Optional[Callable[[SchedulerParams], int]],
        is_scheduler: bool = False) -> None:
    """Install or clear a custom unary worker-selection hook.

    Passing ``None`` clears that hook. Streaming frames ignore both hooks.

    The hook is called synchronously on the I/O loop for every unary frame,
    so it must return quickly. In separate mode, an index that is out of
    range or points at a dead rpc task is replaced by round-robin.

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


def has_custom_scheduler() -> bool:
    """Whether a unary scheduler was registered with ``register_dispatcher_worker_func``."""
    return _dispatcher_worker_func is not None or _worker_scheduler_func is not None
