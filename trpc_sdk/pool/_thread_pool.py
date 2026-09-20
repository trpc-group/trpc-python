# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Process-wide thread pool for offloading sync work from the event loop.

This is not a TCP connection pool. It wraps a
:class:`concurrent.futures.ThreadPoolExecutor` so an async handler can
``await`` a blocking or CPU-heavy callable without stalling the loop.

Registered as the ``"thread"`` plugin under :attr:`PluginType.POOL`.
The default ``init_pos`` is ``WORKER``, so the executor is built in the
worker process after fork; parent-process threads do not survive
``fork``. Pool size comes from ``server.thread_num`` (default about
CPU cores × 5), which is unrelated to merge-mode
``server.worker_thread_num``.
"""

from __future__ import annotations

import concurrent.futures
from typing import Any
from typing import Callable

from trpc_sdk.abc import PluginType
from trpc_sdk.common import get_or_create_event_loop
from trpc_sdk.config import TrpcConfig
from trpc_sdk.plugin import register_plugin

# Process-wide executor. ``None`` means it has not been created yet.
# After fork the child must set this back to ``None`` before
# :func:`submit`; otherwise it still points at the parent's dead pool.
GlobalThreadPool = None


@register_plugin(PluginType.POOL, "thread")
def setup() -> None:
    """Build the executor when the plugin manager initializes this plugin.

    ``PluginManager`` calls ``cls()`` at the WORKER stage. Here ``cls``
    is this function, so the side effect is :func:`init_thread_pool`.

    Args:
        None.
    """
    init_thread_pool()


def init_thread_pool() -> None:
    """Replace :data:`GlobalThreadPool` with a new executor.

    Size is ``TrpcConfig.server_config().thread_num`` in the current
    process. Called from :func:`setup` and from the lazy path in
    :func:`submit`.

    Args:
        None.
    """
    global GlobalThreadPool
    GlobalThreadPool = concurrent.futures.ThreadPoolExecutor(
        TrpcConfig.server_config().thread_num)


async def submit(func: Callable[..., Any], *args: Any) -> Any:
    """Run a synchronous ``func(*args)`` on the thread pool.

    ``func`` must be a regular callable, not a coroutine. If the pool
    is still ``None`` (tests, or a forked child that reset the global),
    it is created first. The current event loop only waits on
    ``run_in_executor``; the callable itself runs on a worker thread.

    Args:
        func: Synchronous callable to execute.
        *args: Positional arguments forwarded to ``func``.

    Returns:
        Whatever ``func`` returns.

    Raises:
        Exception: Propagated from ``func`` when it fails on the worker
            thread.
    """
    global GlobalThreadPool
    if GlobalThreadPool is None:
        init_thread_pool()

    loop = get_or_create_event_loop()
    return await loop.run_in_executor(GlobalThreadPool, func, *args)
