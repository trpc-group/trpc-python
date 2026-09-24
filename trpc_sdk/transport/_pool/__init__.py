# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Client TCP / Unix connection pools used by the client transport half.

Pooling is a tRPC-Python extension, not part of the cross-language tRPC
architecture, so it lives inside :mod:`trpc_sdk.transport` rather than
at the top level. Only the client round trips in
``_client_transport*`` dial through it; the server half never does.

The package is split by responsibility:

- :mod:`~trpc_sdk.transport._pool._base` -- the ``BasePool`` get/put
  contract
- :mod:`~trpc_sdk.transport._pool._types` -- :class:`ConnPoolOptions`
  and address keys
- :mod:`~trpc_sdk.transport._pool._constants` -- default stream-reader
  buffer limit
- :mod:`~trpc_sdk.transport._pool._tcp_connection` -- dial TCP/Unix and
  wrap as :class:`Connection`
- :mod:`~trpc_sdk.transport._pool._dial` -- shared TCP/Unix connect
  helper
- :mod:`~trpc_sdk.transport._pool._pooled_connection` --
  :class:`PooledConnection`
- :mod:`~trpc_sdk.transport._pool._short_conn_factory` -- one-shot conn
  that is never queued idle
- :mod:`~trpc_sdk.transport._pool._connection_pool` -- idle-queue pool,
  manager, and :func:`get_pool_connection`
"""

from __future__ import annotations

from asyncio import IncompleteReadError
from asyncio import LimitOverrunError

from ._base import BasePool
from ._connection_pool import ConnectionPool
from ._connection_pool import ConnectionPoolManager
from ._connection_pool import Pool
from ._connection_pool import PoolCache
from ._connection_pool import get_pool_connection
from ._pooled_connection import PooledConnection
from ._short_conn_factory import ShortConnFactory
from ._tcp_connection import Connection
from ._tcp_connection import open_connection
from ._types import ConnAddress
from ._types import ConnPoolOptions

__all__ = [
    "BasePool",
    "ConnAddress",
    "ConnPoolOptions",
    "PooledConnection",
    "ConnectionPool",
    "ConnectionPoolManager",
    "Pool",
    "PoolCache",
    "ShortConnFactory",
    "get_pool_connection",
    "Connection",
    "open_connection",
    "IncompleteReadError",
    "LimitOverrunError",
]
