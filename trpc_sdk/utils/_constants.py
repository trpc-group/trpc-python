# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Shared constants for :mod:`trpc_sdk.utils`.

This module is the bottom of the utils import graph: it must not import
any other ``trpc_sdk.utils`` submodule. Enums and string constants live here
so the other private modules can share them without cycles. Timer enums
live with the timer code in :mod:`trpc_sdk.utils._timer`.

:data:`UNIX_TYPE` is the socket-family label stored on service config,
not a Python ``socket.AF_UNIX`` value. :class:`TcpState` mirrors Linux
``tcp_info.tcpi_state`` for :func:`trpc_sdk.utils._network.is_close_wait`.

:data:`TRPC_SHARE_MEM_LOCK` and :class:`RegisterStatus` back the register
flag in :mod:`trpc_sdk.utils._process_sync`. The lock path is relative to
the process current working directory, not absolute: every tRPC process on
the machine that shares that cwd uses the same file as a ``portalocker``
carrier, and the file content itself does not store the register flag.
"""

from __future__ import annotations

from enum import Enum
from enum import IntEnum
from enum import unique

# Label compared by :func:`trpc_sdk.utils._network.is_unix_socket`. Config
# stores this string; it is not ``socket.AF_UNIX``.
UNIX_TYPE = "unix"

# Relative path of the lock file used by :mod:`trpc_sdk.utils._process_sync`.
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


class TcpState(IntEnum):
    """Linux ``tcp_info.tcpi_state`` values used with ``TCP_INFO``.

    Integer values match the kernel enum starting at
    ``TCP_ESTABLISHED = 1``. :func:`trpc_sdk.utils._network.is_close_wait`
    reads only the first byte of the ``TCP_INFO`` blob and compares it
    to :attr:`TCP_CLOSE_WAIT`. macOS is skipped at the call site because
    ``TCP_INFO`` is not available there on the Python versions we support.
    """

    TCP_ESTABLISHED = 1
    TCP_SYN_SENT = 2
    TCP_SYN_RECV = 3
    TCP_FIN_WAIT1 = 4
    TCP_FIN_WAIT2 = 5
    TCP_TIME_WAIT = 6
    TCP_CLOSE = 7
    TCP_CLOSE_WAIT = 8
    TCP_LAST_ACK = 9
    TCP_LISTEN = 10
    TCP_CLOSING = 11
