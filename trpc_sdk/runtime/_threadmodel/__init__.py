# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Thread model of one serving process, after trpc-cpp ``runtime/threadmodel``.

The threads of a ``merge`` worker are built from the pieces below. Which of
them a worker uses is decided by ``server.handle_thread_num`` (resolved into
``io_handle_type``):
:class:`~trpc_sdk.runtime._merge_worker.MergeWorker` runs every request on the
loop that read it, while :class:`SeparateThread` hands requests from I/O
loops to handle loops.

The package is split by responsibility:

- :mod:`~trpc_sdk.runtime._threadmodel._separate_thread` --
  :class:`SeparateThread`, merge mode with the separate thread model
  (trpc-cpp ``separate_runtime``)
- :mod:`~trpc_sdk.runtime._threadmodel._loop_thread` -- :class:`LoopThread`,
  one event loop on its own thread (trpc-cpp ``worker_thread``), used as a
  merge serving loop, an I/O loop, or a handle loop
- :mod:`~trpc_sdk.runtime._threadmodel._handler_pool` -- the pool that picks
  a handle loop for each request under ``io_handle_type: separate``
  (trpc-cpp ``threadmodel/separate/separate_scheduling``)
- :mod:`~trpc_sdk.runtime._threadmodel._thread_hooks` -- per-serving-loop
  start/stop hooks and the current serving-loop lookup
"""

from ._handler_pool import HandleSlot
from ._handler_pool import SharedHandlerPool
from ._loop_thread import LoopThread
from ._loop_thread import connect_accepted
from ._loop_thread import stop_loop_threads
from ._thread_hooks import HandlerThreadInfo
from ._thread_hooks import current_handler_thread
from ._thread_hooks import freeze_handler_thread_hooks
from ._thread_hooks import register_handler_thread_hook
from ._thread_hooks import run_start_hooks
from ._thread_hooks import run_stop_hooks
from ._thread_hooks import set_current_handler_thread


def __getattr__(name: str):
    """Load :class:`SeparateThread` the first time it is asked for.

    A body-level import would cycle. :class:`~trpc_sdk.runtime._merge_worker.MergeWorker`
    imports this package before its own class statement has run, and
    :class:`SeparateThread` subclasses that class. By the time a caller
    reads the name, ``MergeWorker`` already exists: :mod:`trpc_sdk.runtime._modes`
    imports it first.

    Args:
        name: Attribute being looked up on this package.

    Returns:
        The :class:`SeparateThread` class.

    Raises:
        AttributeError: ``name`` is not a lazily exported attribute.
    """
    if name != "SeparateThread":
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    from ._separate_thread import SeparateThread
    globals()["SeparateThread"] = SeparateThread
    return SeparateThread


__all__ = [
    "HandleSlot",
    "HandlerThreadInfo",
    "LoopThread",
    "SeparateThread",
    "SharedHandlerPool",
    "connect_accepted",
    "current_handler_thread",
    "freeze_handler_thread_hooks",
    "register_handler_thread_hook",
    "run_start_hooks",
    "run_stop_hooks",
    "set_current_handler_thread",
    "stop_loop_threads",
]
