# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Startup barrier for IO workers waiting on task-process notifications.

After fork, an IO worker may block in :func:`wait_notify_in_io` until
task processes have called :func:`inc_io_wait_count` a configured
number of times. The hook is registered from
:meth:`trpc.server._server.Server.serve` via
``fork_hook.register_child_callback``, so it runs in the child before
that process builds an event loop.

The counter and its lock are ``multiprocessing`` objects created at
import time, which must happen before the first ``fork``. Children
inherit the same mapping.
"""

from __future__ import annotations

import multiprocessing
import time

# Notification counter inherited across fork. Created at import so it
# exists before the first child is spawned.
_io_ready_cnt = multiprocessing.Value('i', 0)
# Guards only the read/write of ``_io_ready_cnt``. The 1-second sleep
# in :func:`wait_notify_in_io` is taken outside this lock so waiters
# do not serialize each other for the full poll interval.
_io_lock = multiprocessing.Lock()


def wait_notify_in_io(cnt: int) -> None:
    """Block until the notification counter reaches ``cnt``.

    Synchronous 1-second poll. Runs in the forked child, before that
    process creates an event loop, so it must not ``await``.

    Args:
        cnt: Number of :func:`inc_io_wait_count` notifications this
            IO worker waits for before returning.

    Returns:
        None. Returns once ``_io_ready_cnt.value >= cnt``.
    """
    global _io_ready_cnt, _io_lock
    while True:
        with _io_lock:
            if _io_ready_cnt.value >= cnt:
                break
        time.sleep(1)


def inc_io_wait_count() -> None:
    """Increment the IO-ready counter by one.

    Task processes call this after they finish work that IO workers
    must wait for (for example loading a model). IO workers in
    :func:`wait_notify_in_io` return once the counter reaches their
    configured threshold.
    """
    global _io_ready_cnt, _io_lock
    with _io_lock:
        _io_ready_cnt.value += 1
