# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""One-shot TCP/Unix connections that are never returned to an idle queue.

:class:`ShortConnFactory` duck-types the ``get`` / ``put`` surface of
:class:`~trpc_sdk.abc.BasePool` so a
:class:`~trpc_sdk.pool.PooledConnection` can still
``close`` through ``pool.put``. ``put`` always force-closes.
"""

from __future__ import annotations

from asyncio import BaseEventLoop

from trpc_sdk.abc import BaseClientTransportOption

from ._dial import dial_connection
from ._pooled_connection import PooledConnection


class ShortConnFactory:
    """Dial a one-shot conn that is never returned to an idle queue.

    ``put`` always :meth:`~PooledConnection.force_close` so callers can
    still use the pooled ``close`` path. ``is_short`` is True on the
    wrapper so logs can tell these apart from pooled conns.

    Args:
        None.
    """

    async def get(
            self,
            options: BaseClientTransportOption,
            timeout: float,
            loop: BaseEventLoop) -> PooledConnection:
        """Dial through :func:`~trpc_sdk.pool._dial.dial_connection` and wrap it.

        Args:
            options: Transport options with the remote address / sock.
            timeout: Dial timeout in seconds.
            loop: Event loop passed to ``open_connection``.

        Returns:
            A short-lived :class:`PooledConnection`.
        """
        conn = await dial_connection(options=options, timeout=timeout, loop=loop)
        return PooledConnection(conn=conn, pool=self, address=options.address, is_short=True)

    @staticmethod
    def put(pool_conn: PooledConnection, _: bool = True) -> None:
        """Force-close ``pool_conn``; short conns are never cached.

        Args:
            pool_conn: Wrapper returned by :meth:`get`.
            _: Ignored; the pooled ``put`` signature is ``force_close``.
        """
        pool_conn.force_close()
