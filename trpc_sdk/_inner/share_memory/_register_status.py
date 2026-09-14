# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Cross-process register flag backed by a shared-memory integer.

:data:`REGISTER_STATUE` is a ``multiprocessing.Value`` created at
import time, which must happen before the first ``fork``. Children
inherit the same mapping; they do not allocate a new one.

:class:`~trpc.server._registry.RegistryThread` waits on
:func:`register_spinlock` until some process flips the flag from
:attr:`~.RegisterStatus.unregister` to
:attr:`~.RegisterStatus.register`. The cwd-relative lock file is only
a ``portalocker`` carrier; the flag lives in the ``Value``, not in
the file contents.
"""

from __future__ import annotations

import asyncio
import logging
import os
from multiprocessing import Value

from portalocker import LOCK_EX
from portalocker import lock

from ._constants import TRPC_SHARE_MEM_LOCK
from ._constants import RegisterStatus

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

    A coroutine that runs on :class:`~trpc.server._registry.RegistryThread`'s
    own event loop. Polls once per second; it is not a CPU spin.

    Args:
        None.

    Returns:
        None. Returns when ``REGISTER_STATUE`` is no longer
        :attr:`RegisterStatus.unregister`.
    """
    while REGISTER_STATUE.value == RegisterStatus.unregister.value:
        await asyncio.sleep(1)
