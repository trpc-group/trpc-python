# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Abstract contract for a client connection pool.

:class:`BasePool` is the get/put surface that
:class:`~trpc_sdk.transport._pool.PooledConnection` talks to when it is
returned. :class:`~trpc_sdk.transport._pool.ConnectionPool` is the
idle-queue implementation;
:class:`~trpc_sdk.transport._pool.ShortConnFactory` duck-types the same
``put`` so a short-lived conn can still call ``close``.
"""

from __future__ import annotations

from abc import ABC
from abc import abstractmethod
from asyncio import BaseEventLoop
from typing import TYPE_CHECKING
from typing import Optional

from trpc_sdk.abc import AddrType
from trpc_sdk.common import get_or_create_event_loop

if TYPE_CHECKING:
    from ._pooled_connection import PooledConnection


class BasePool(ABC):
    """Client pool that issues and takes back pooled connections.

    ``address`` is the remote the pool dials. ``loop`` is the event
    loop the idle queue and dials are bound to; a missing loop is
    resolved with :func:`get_or_create_event_loop` at construction.

    Args:
        address: Remote the pool serves.
        loop: Event loop for this pool, or ``None`` to pick the current
            one.
    """

    def __init__(self, address: AddrType, loop: Optional[BaseEventLoop]):
        self._address = address
        self._loop = loop or get_or_create_event_loop()

    @property
    def address(self) -> AddrType:
        """Remote address this pool dials."""
        return self._address

    @property
    def loop(self) -> BaseEventLoop:
        """Event loop the pool's idle queue and dials run on."""
        return self._loop

    @abstractmethod
    async def get(self, timeout: float) -> "PooledConnection":
        """Take one connection out of the pool, dialing if needed.

        Args:
            timeout: Dial timeout in seconds when a new conn is
                created.

        Returns:
            A connection the caller must ``close`` (or ``put``) when
            the RPC finishes.
        """

    @abstractmethod
    def put(self, pool_conn: "PooledConnection", force_close: bool = False) -> None:
        """Return a connection to the pool, or drop it.

        This is a return, not a socket close. ``force_close=True``
        discards the conn; otherwise it may be queued as idle.

        Args:
            pool_conn: Connection previously obtained from :meth:`get`.
            force_close: If True, close the socket instead of caching
                it as idle.
        """
