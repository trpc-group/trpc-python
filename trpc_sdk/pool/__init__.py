# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""tRPC-Python pool package: client TCP pools and the worker thread pool.

Public symbols are re-exported here. Callers should ``from trpc_sdk.pool
import ...``. The ``BasePool`` get/put contract lives in
:mod:`trpc_sdk.abc`.

The package is split by responsibility:

- :mod:`~trpc_sdk.pool._types` — :class:`ConnPoolOptions` and address keys
- :mod:`~trpc_sdk.pool._pooled_connection` — :class:`PooledConnection`
- :mod:`~trpc_sdk.pool._constants` — multiplexed conn state and the
  default stream-reader buffer limit
- :mod:`~trpc_sdk.pool._stream_io` — asyncio stream reader / writer /
  protocol used by TCP connections
- :mod:`~trpc_sdk.pool._dial` — shared TCP/Unix connect helper
- :mod:`~trpc_sdk.pool._short_conn_factory` — one-shot conn that is never
  queued idle
- :mod:`~trpc_sdk.pool._connection_pool` — idle-queue pool, manager, and
  :func:`get_pool_connection`
- :mod:`~trpc_sdk.pool._tcp_connection` — open TCP/Unix and wrap as
  :class:`Connection`
- :mod:`~trpc_sdk.pool._thread_pool` — process-wide ``ThreadPoolExecutor``
  plugin, unrelated to TCP pooling
- :mod:`~trpc_sdk.pool._multiplexed` — multiplexed virtual connections
"""

from __future__ import annotations

from asyncio import IncompleteReadError
from asyncio import LimitOverrunError

from ._connection_pool import ConnectionPool
from ._connection_pool import ConnectionPoolManager
from ._connection_pool import Pool
from ._connection_pool import PoolCache
from ._connection_pool import get_pool_connection
from ._multiplexed import ConnectionGroup
from ._multiplexed import Multiplexed
from ._multiplexed import MultiplexedConnection
from ._multiplexed import MultiplexedOptions
from ._multiplexed import VirtualConnection
from ._multiplexed import create_multiplexed
from ._multiplexed import default_multiplexed_pool
from ._pooled_connection import PooledConnection
from ._short_conn_factory import ShortConnFactory
from ._stream_io import StreamReader
from ._stream_io import StreamReaderProtocol
from ._stream_io import StreamWriter
from ._tcp_connection import Connection
from ._tcp_connection import open_connection
from ._thread_pool import GlobalThreadPool
from ._thread_pool import init_thread_pool
from ._thread_pool import setup
from ._thread_pool import submit
from ._types import ConnAddress
from ._types import ConnPoolOptions

__all__ = [
    "ConnAddress",
    "ConnPoolOptions",
    "PooledConnection",
    "ConnectionPool",
    "ConnectionPoolManager",
    "Pool",
    "PoolCache",
    "ShortConnFactory",
    "get_pool_connection",
    "StreamReader",
    "StreamWriter",
    "StreamReaderProtocol",
    "Connection",
    "open_connection",
    "Multiplexed",
    "MultiplexedOptions",
    "MultiplexedConnection",
    "VirtualConnection",
    "ConnectionGroup",
    "create_multiplexed",
    "default_multiplexed_pool",
    "IncompleteReadError",
    "LimitOverrunError",
    "GlobalThreadPool",
    "setup",
    "init_thread_pool",
    "submit",
]
