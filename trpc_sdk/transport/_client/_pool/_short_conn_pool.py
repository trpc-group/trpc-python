# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.
"""One-shot TCP/Unix connections that are never returned to an idle queue.

:class:`ShortConnPool` implements the ``get`` / ``put`` surface of
:class:`~trpc_sdk.transport._client._pool.BaseConnectionPool` so a
:class:`~trpc_sdk.transport._client._pool.Connection` can still
``close`` through ``pool.put``. ``put`` always force-closes.
"""

from __future__ import annotations

from asyncio import BaseEventLoop

from trpc_sdk.abc import BaseClientTransportOption

from ._base import BaseConnectionPool
from ._dial import dial_connection
from ._connection import Connection


class ShortConnPool(BaseConnectionPool):
    """Dial a one-shot conn that is never returned to an idle queue.

    ``put`` always :meth:`~Connection.force_close` so callers can
    still use the pooled ``close`` path. ``is_short`` is True on the
    wrapper so logs can tell these apart from pooled conns.

    Args:
        None.
    """

    def __init__(self):  # pylint: disable=super-init-not-called
        self._address = None
        self._loop = None

    async def get(self, options: BaseClientTransportOption, timeout: float, loop: BaseEventLoop) -> Connection:
        """Dial through :func:`~trpc_sdk.transport._client._pool._dial.dial_connection`.

        Args:
            options: Transport options with the remote address / sock.
            timeout: Dial timeout in seconds.
            loop: Event loop the dial runs on.

        Returns:
            A short-lived :class:`Connection`.
        """
        reader, writer = await dial_connection(options=options, timeout=timeout, loop=loop)
        return Connection(reader=reader, writer=writer, loop=loop, pool=self, address=options.address, is_short=True)

    def put(self, pool_conn: Connection, _: bool = True) -> None:
        """Force-close ``pool_conn``; short conns are never cached.

        Args:
            pool_conn: Wrapper returned by :meth:`get`.
            _: Ignored; the pooled ``put`` signature is ``force_close``.
        """
        pool_conn.force_close()
