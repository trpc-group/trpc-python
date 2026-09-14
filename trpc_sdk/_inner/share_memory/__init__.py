# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""tRPC-Python share_mem package: two cross-process primitives.

Public symbols are re-exported here. Callers should ``from trpc.share_mem
import ...``.

The package provides:

- a shared-memory register flag (:data:`REGISTER_STATUE`) that
  :class:`~trpc.server._registry.RegistryThread` waits on
- an IO-worker startup barrier (:func:`wait_notify_in_io` /
  :func:`inc_io_wait_count`) used after fork

Importing this package constructs the ``multiprocessing.Value`` /
``Lock`` objects, so it must happen before the first ``fork``. The
explicit imports below load the stateful submodules for that reason.

The package is split by responsibility:

- :mod:`~trpc.share_mem._constants` — lock-file path and
  :class:`RegisterStatus`
- :mod:`~trpc.share_mem._register_status` — flag, lock-file
  lifecycle, and register wait
- :mod:`~trpc.share_mem._io_barrier` — IO-worker notification counter
"""

from __future__ import annotations

from ._constants import TRPC_SHARE_MEM_LOCK
from ._constants import RegisterStatus
from ._io_barrier import inc_io_wait_count
from ._io_barrier import wait_notify_in_io
from ._register_status import REGISTER_STATUE
from ._register_status import get_register_status
from ._register_status import register_enable
from ._register_status import register_spinlock
from ._register_status import remove_lock_file
from ._register_status import set_register_status
from ._register_status import warm_up

__all__ = [
    "TRPC_SHARE_MEM_LOCK",
    "RegisterStatus",
    "REGISTER_STATUE",
    "warm_up",
    "remove_lock_file",
    "get_register_status",
    "set_register_status",
    "register_enable",
    "register_spinlock",
    "wait_notify_in_io",
    "inc_io_wait_count",
]
