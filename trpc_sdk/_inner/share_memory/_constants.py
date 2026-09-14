# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Constants for the shared-memory register flag.

:data:`TRPC_SHARE_MEM_LOCK` is a path relative to the process current
working directory, not an absolute path. Every tRPC process on the
machine that shares that cwd uses the same file as a ``portalocker``
carrier; the file content itself does not store the register flag.
"""

from __future__ import annotations

from enum import Enum
from enum import unique


# Relative path of the lock file used by :mod:`._register_status`.
# Created under the process cwd; all tRPC processes that share that cwd
# contend on the same inode.
TRPC_SHARE_MEM_LOCK = ".__trpc.python.sharemem.lock"


@unique
class RegisterStatus(Enum):
    """State stored in the shared-memory register flag.

    The numeric values are part of the cross-process contract: they are
    written straight into ``REGISTER_STATUE``. Kept as :class:`Enum`
    (not ``IntEnum``) so historical identity comparisons stay as they
    were.
    """

    # Matches the initial value of ``Value('i', 0)``.
    unregister = 0
    register = 1
