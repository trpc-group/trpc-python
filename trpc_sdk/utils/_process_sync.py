# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Cross-process synchronization primitives shared across fork.

Three primitives live here, all backed by ``multiprocessing`` objects
created at import time. Importing :mod:`trpc_sdk.utils` must therefore
happen before the first ``fork``; children inherit the same mapping and
never allocate a second copy.

IO-worker startup barrier. After fork, the process that owns the listen
sockets waits until task processes have called :func:`inc_io_wait_count`
a configured number of times. YAML ``server.io_wait_count: N`` reaches
the worker through ``ServerOption.io_wait_count``, whose ``_wait_until_tasks_ready`` polls
:func:`io_ready_reached` before that process builds an event loop.
:func:`wait_notify_in_io` is the plain blocking form of the same wait.

Register flag. :class:`~trpc_sdk.server._registry.RegistryThread` waits
on :func:`register_spinlock` until some process flips
:data:`REGISTER_STATUE` from :attr:`~.RegisterStatus.unregister` to
:attr:`~.RegisterStatus.register`. The cwd-relative lock file is only a
``portalocker`` carrier; the flag lives in the ``Value``, not in the file
contents.

Registry-heartbeat pause count. :func:`pause_heartbeat` /
:func:`resume_heartbeat` are a nested counter, not a boolean.
:func:`trpc_sdk.runtime._process._task_process.build_task_infos` increments once per task
that set ``pause_heartbeat``, still in the parent, so the registry client
stops advertising before any child is forked. :func:`resume_heartbeat`
runs in each such child after its initializer; :func:`should_heartbeat`
is true only when the counter is back at zero.
"""

from __future__ import annotations

import asyncio
import logging
import multiprocessing
import os
import time
from multiprocessing import Value

from portalocker import LOCK_EX
from portalocker import lock

from ._constants import TRPC_SHARE_MEM_LOCK
from ._constants import RegisterStatus

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
    while not io_ready_reached(cnt):
        time.sleep(1)


def io_ready_reached(cnt: int) -> bool:
    """Return whether the notification counter has reached ``cnt``.

    The non-blocking half of :func:`wait_notify_in_io`, for a caller that has
    to check other conditions between polls, such as whether the task that
    should notify is still alive.

    Args:
        cnt: Number of :func:`inc_io_wait_count` notifications to wait for.

    Returns:
        True once ``_io_ready_cnt.value >= cnt``.
    """
    global _io_ready_cnt, _io_lock
    with _io_lock:
        return _io_ready_cnt.value >= cnt


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


# Shared-memory flag. Built at import so the object exists before the
# first fork; children inherit the mapping rather than creating a
# second counter.
REGISTER_STATUE = Value('i', 0)


def warm_up() -> None:
    """Create the lock file if it is missing.

    :func:`get_register_status` opens the path with ``'r+'``, which
    raises ``FileNotFoundError`` when the file does not exist. Call
    this first so later exclusive locks have an inode to attach to.

    Args:
        None.

    Returns:
        None.
    """
    if not os.path.exists(TRPC_SHARE_MEM_LOCK):
        with open(TRPC_SHARE_MEM_LOCK, 'wb'):
            pass


def remove_lock_file() -> None:
    """Unlink the lock file after deregistration.

    Missing files are ignored. The shared-memory flag is not reset.

    Args:
        None.

    Returns:
        None.
    """
    if os.path.exists(TRPC_SHARE_MEM_LOCK):
        os.remove(TRPC_SHARE_MEM_LOCK)


def get_register_status() -> str:
    """Read the shared-memory register flag under an exclusive lock.

    Opens the lock file with ``'r+'``. The path must already exist;
    call :func:`warm_up` first or this raises ``FileNotFoundError``.

    The annotated return type is ``str`` for historical reasons. The
    value actually returned is ``REGISTER_STATUE.value``, an ``int``.

    Args:
        None.

    Returns:
        The current ``REGISTER_STATUE.value`` (an ``int``, despite the
        ``str`` annotation).
    """
    file = None
    try:
        file = open(TRPC_SHARE_MEM_LOCK, 'r+')
        lock(file, LOCK_EX)

        return REGISTER_STATUE.value
    finally:
        if file:
            file.close()


def set_register_status(register_status: int) -> bool:
    """Write ``register_status`` into the shared-memory flag.

    Opens the lock file with ``'wb'``, which truncates it. The file
    is only a ``portalocker`` carrier; the flag itself lives in
    :data:`REGISTER_STATUE` and is not serialized into the file.

    Args:
        register_status: Integer to store, typically a
            :class:`RegisterStatus` value.

    Returns:
        ``True`` on success, ``False`` if the lock or write failed.
    """
    file = None
    try:
        file = open(TRPC_SHARE_MEM_LOCK, 'wb', buffering=0)
        lock(file, LOCK_EX)

        REGISTER_STATUE.value = register_status
        return True
    except Exception as exc:  # pylint: disable=broad-except
        logging.exception("fail to set register status:%s, exc:%s", register_status, str(exc))
        return False
    finally:
        if file:
            file.close()


def register_enable() -> bool:
    """Flip the shared-memory flag to :attr:`RegisterStatus.register`.

    Same locking path as :func:`set_register_status`: ``'wb'``
    truncates the lock file, which remains a lock carrier only.

    Args:
        None.

    Returns:
        ``True`` on success, ``False`` if the lock or write failed.
    """
    file = None
    try:
        file = open(TRPC_SHARE_MEM_LOCK, 'wb', buffering=0)
        lock(file, LOCK_EX)

        REGISTER_STATUE.value = RegisterStatus.register.value
        return True
    except Exception as exc:  # pylint: disable=broad-except
        logging.exception("fail to register enable, exc:%s", str(exc))
        return False
    finally:
        if file:
            file.close()


async def register_spinlock() -> None:
    """Wait until the shared-memory flag leaves ``unregister``.

    A coroutine that runs on :class:`~trpc_sdk.server._registry.RegistryThread`'s
    own event loop. Polls once per second; it is not a CPU spin.

    Args:
        None.

    Returns:
        None. Returns when ``REGISTER_STATUE`` is no longer
        :attr:`RegisterStatus.unregister`.
    """
    while REGISTER_STATUE.value == RegisterStatus.unregister.value:
        await asyncio.sleep(1)


# Nested registry-heartbeat pause count. Built at import so pauses taken
# in the parent before fork and resumes from children hit the same int.
_heartbeat_pause_count = Value('i', 0)


def pause_heartbeat() -> None:
    """Increment the process-wide registry-heartbeat pause count.

    Heartbeats stay off until every matching :func:`resume_heartbeat`
    has run. Safe to call more than once; each call needs its own resume.
    """
    with _heartbeat_pause_count.get_lock():
        _heartbeat_pause_count.value += 1


def resume_heartbeat() -> None:
    """Decrement the pause count, stopping at zero.

    A resume without a matching pause is a no-op so a child that was
    not asked to pause cannot drive the counter negative.
    """
    with _heartbeat_pause_count.get_lock():
        if _heartbeat_pause_count.value > 0:
            _heartbeat_pause_count.value -= 1


def should_heartbeat() -> bool:
    """Return whether registry heartbeats are currently allowed.

    Returns:
        ``True`` when no pause is outstanding, otherwise ``False``.
    """
    with _heartbeat_pause_count.get_lock():
        return _heartbeat_pause_count.value == 0
