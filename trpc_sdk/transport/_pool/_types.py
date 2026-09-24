# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Shared type definitions for the client connection pool.

:class:`ConnPoolOptions` is stamped onto client transport options and
read by :class:`~trpc_sdk.transport._pool.ConnectionPool` /
:class:`~trpc_sdk.transport._pool.ConnectionPoolManager`.
:data:`ConnAddress` is the key type used when a manager indexes a pool
by socket or ``(host, port)``.
"""

from __future__ import annotations

import socket
from dataclasses import dataclass
from typing import Union

from trpc_sdk.abc import AddrType

ConnAddress = Union[AddrType, socket.socket]


@dataclass
class ConnPoolOptions:
    """Tunables for one address-scoped connection pool.

    Attributes:
        dial_timeout: Connect timeout in milliseconds; divided by 1000
            before use in
            :meth:`trpc_sdk.transport._pool.ConnectionPoolManager.get`.
        idle_timeout: Seconds an idle conn may sit in the queue. ``0``
            disables the idle check.
        max_conn_lifetime: Hard lifetime in seconds from create time;
            ``0`` disables it.
        max_idle: Idle queue capacity; ``0`` means no idle caching.
        max_active: Active conn cap; ``0`` means unlimited.
        wait: When the active cap is hit, block on the idle queue
            instead of raising ``ClientPoolLimitError``.
    """

    dial_timeout: float = 200
    idle_timeout: float = 60
    max_conn_lifetime: float = 0
    max_idle: int = 5
    max_active: int = 0
    wait: bool = False
