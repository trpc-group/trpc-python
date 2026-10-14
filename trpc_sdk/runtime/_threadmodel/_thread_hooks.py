# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Per-serving-loop start/stop hooks, and which serving loop this thread runs.

A *serving loop* is an event loop that runs user code (handlers, filters,
``@route``, timers). In the single-threaded merge model it is the worker's
main loop; multi-threaded merge has one per serving thread, and
``io_handle_type: separate`` one per handle thread.
Resources bound to a loop or a thread (an async DB pool, an inference
session) belong in an ``on_start`` hook so each serving loop gets its own.

The registry is a module-level list filled at import time, before
:meth:`~trpc_sdk.server.Server.serve`. ``serve()`` freezes it: a hook
registered later could never run on loops that already started.
"""

from __future__ import annotations

import asyncio
import inspect
import threading
from dataclasses import dataclass
from dataclasses import field
from typing import Any
from typing import Callable
from typing import Dict
from typing import List
from typing import Optional
from typing import Tuple

from trpc_sdk.log import logger

# ``hook(info)``; a plain function or an ``async def``.
HandlerThreadHook = Callable[["HandlerThreadInfo"], Any]


@dataclass
class HandlerThreadInfo:
    """Identity of one serving loop, handed to every hook.

    Attributes:
        index: Position among serving loops, ``0 .. count - 1``.
        count: Number of serving loops in this process.
        loop: The serving loop; hooks run on it.
        state: Per-loop storage owned by user hooks.
    """

    index: int
    count: int
    loop: asyncio.AbstractEventLoop
    state: Dict[str, Any] = field(default_factory=dict)


_hooks: List[Tuple[Optional[HandlerThreadHook], Optional[HandlerThreadHook]]] = []
_frozen: bool = False
_local = threading.local()


def register_handler_thread_hook(on_start: Optional[HandlerThreadHook] = None,
                                 on_stop: Optional[HandlerThreadHook] = None) -> None:
    """Run ``on_start`` when each serving loop starts and ``on_stop`` when it stops.

    Both run on the serving loop's own thread. Start hooks run in
    registration order and an exception aborts startup (the worker exits
    with status 1); stop hooks run in reverse order and an exception is only
    logged.

    Args:
        on_start: Called as ``on_start(info)`` before the loop serves
            requests; may be ``async def``.
        on_stop: Called as ``on_stop(info)`` during graceful shutdown; may be
            ``async def``.

    Raises:
        RuntimeError: If called after :meth:`~trpc_sdk.server.Server.serve`.
    """
    if _frozen:
        raise RuntimeError("register_handler_thread_hook must be called before Server.serve()")
    _hooks.append((on_start, on_stop))


def current_handler_thread() -> Optional[HandlerThreadInfo]:
    """Return the serving loop running on this thread.

    Returns:
        Its :class:`HandlerThreadInfo`, or ``None`` outside a serving loop.
    """
    return getattr(_local, "info", None)


def set_current_handler_thread(info: Optional[HandlerThreadInfo]) -> None:
    """Record which serving loop this thread runs; read by :func:`current_handler_thread`.

    Args:
        info: The loop's info, or ``None`` to clear.
    """
    _local.info = info


def freeze_handler_thread_hooks() -> None:
    """Reject further registrations; called by ``Server.serve``."""
    global _frozen  # pylint: disable=global-statement
    _frozen = True


async def run_start_hooks(info: HandlerThreadInfo) -> None:
    """Run every ``on_start`` in registration order on the serving loop.

    Args:
        info: The serving loop being started.

    Raises:
        Exception: Whatever a hook raised; later hooks do not run.
    """
    for on_start, _ in list(_hooks):
        if on_start is not None:
            await _call(on_start, info)


async def run_stop_hooks(info: HandlerThreadInfo) -> None:
    """Run every ``on_stop`` in reverse registration order; errors are logged.

    Args:
        info: The serving loop being stopped.
    """
    for _, on_stop in reversed(list(_hooks)):
        if on_stop is None:
            continue
        try:
            await _call(on_stop, info)
        except Exception as err:  # pylint: disable=broad-except
            logger.error("[handler thread %s] on_stop hook failed: %s", info.index, repr(err), exc_info=True)


async def _call(hook: HandlerThreadHook, info: HandlerThreadInfo) -> None:
    result = hook(info)
    if inspect.isawaitable(result):
        await result
