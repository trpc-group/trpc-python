# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.
"""Entry point the client round trips use to borrow a connection.

:class:`~trpc_sdk.transport._client._pool.ShortConnPool` dials a
one-shot connection that is force-closed on return and never queued.
:class:`~trpc_sdk.transport._client._pool.KeepLiveConnPoolManager` maps a
local+peer key onto keep-alive connection pools.
:func:`get_pool_connection` is the entry used by client transport:
short-conn options skip the manager; otherwise a per-loop manager is
cached in :class:`KeepLiveConnPoolManagerCache`.
"""

from __future__ import annotations

from asyncio import BaseEventLoop
from typing import Optional

from trpc_sdk.abc import BaseClientTransportOption
from trpc_sdk.common import get_or_create_event_loop
from trpc_sdk.log import logger

from ._connection import Connection
from ._short_conn_pool import ShortConnPool
from ._keep_live_conn_pool import KeepLiveConnPoolManagerCache
from ._keep_live_conn_pool import KeepLiveConnPoolManager

_short_conn_pool = ShortConnPool()

_keep_live_conn_pool_manager_cache = KeepLiveConnPoolManagerCache()


async def get_pool_connection(options: BaseClientTransportOption,
                              timeout: float,
                              loop: Optional[BaseEventLoop] = None) -> Connection:
    """Return a pooled (or short) connection for ``options``.

    ``options.short_conn`` uses :class:`ShortConnPool` and never
    touches the manager cache. Otherwise a :class:`KeepLiveConnPoolManager`
    is created once per event loop and reused. If the same address was
    previously bound to a different loop, that stale loop entry is
    deleted after a warning.

    Args:
        options: Client transport options, including ``pool_opts`` when
            a new manager must be built.
        timeout: Dial timeout in seconds; ``<= 0`` uses pool options.
        loop: Event loop that owns the manager; ``None`` picks current.

    Returns:
        A :class:`Connection` ready for ``read`` / ``write``.
    """
    loop = loop or get_or_create_event_loop()

    if options.short_conn:
        return await _short_conn_pool.get(options=options, timeout=timeout, loop=loop)

    conn_pool_mgr = _keep_live_conn_pool_manager_cache.get(loop, None)
    if not conn_pool_mgr:
        old_loop: BaseEventLoop = _keep_live_conn_pool_manager_cache.get(options.address, None)
        if old_loop:
            if id(old_loop) != id(loop):
                logger.warning(f"address: {options.address} exist different loop, \
                               old {id(old_loop)} != current loop: {loop}")
                if old_loop in _keep_live_conn_pool_manager_cache.__dict__:
                    del _keep_live_conn_pool_manager_cache.__dict__[old_loop]
        _keep_live_conn_pool_manager_cache[options.address] = loop

        pool_opts = getattr(options, "pool_opts", None)
        conn_pool_mgr = KeepLiveConnPoolManager(options=pool_opts, loop=loop)
        _keep_live_conn_pool_manager_cache[loop] = conn_pool_mgr

    return await conn_pool_mgr.get(options, timeout)
