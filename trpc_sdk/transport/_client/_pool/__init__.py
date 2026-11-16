# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.
"""Client TCP / Unix connection pools used by the client transport half.

Pooling is a tRPC-Python extension, not part of the cross-language tRPC
architecture, so it lives inside :mod:`trpc_sdk.transport._client` rather
than at the top level. Only the client round trips in
:mod:`trpc_sdk.transport._client` dial through it; the server half never
does.

The package is split by responsibility:

- :mod:`~trpc_sdk.transport._client._pool._base` -- the
  ``BaseConnection`` / ``BaseConnectionPool`` contracts
- :mod:`~trpc_sdk.transport._client._pool._types` --
  :class:`KeepLiveConnPoolOptions` and address keys
- :mod:`~trpc_sdk.transport._client._pool._constants` -- default
  stream-reader buffer limit
- :mod:`~trpc_sdk.transport._client._pool._dial` -- shared TCP/Unix
  connect helper over the stdlib ``asyncio.open_connection``
- :mod:`~trpc_sdk.transport._client._pool._connection` --
  :class:`Connection`
- :mod:`~trpc_sdk.transport._client._pool._short_conn_pool` -- one-shot
  conn that is never queued idle
- :mod:`~trpc_sdk.transport._client._pool._keep_live_conn_pool` --
  idle-queue pool, manager, and per-loop manager cache
- :mod:`~trpc_sdk.transport._client._pool._pool_conn` --
  :func:`get_pool_connection`
"""

from __future__ import annotations

from asyncio import IncompleteReadError
from asyncio import LimitOverrunError

from ._base import BaseConnectionPool
from ._connection import Connection
from ._keep_live_conn_pool import KeepLiveConnPool
from ._keep_live_conn_pool import KeepLiveConnPoolManager
from ._keep_live_conn_pool import KeepLiveConnPoolManagerCache
from ._pool_conn import get_pool_connection
from ._short_conn_pool import ShortConnPool
from ._types import ConnAddress
from ._types import KeepLiveConnPoolOptions

__all__ = [
    "BaseConnectionPool",
    "KeepLiveConnPool",
    "KeepLiveConnPoolManager",
    "KeepLiveConnPoolManagerCache",
    "Connection",
    "ConnAddress",
    "KeepLiveConnPoolOptions",
    "ShortConnPool",
    "IncompleteReadError",
    "LimitOverrunError",
    "get_pool_connection",
]
