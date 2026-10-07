# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Address-scoped TCP/Unix client connection pools.

:class:`ConnectionPool` holds idle conns for one remote.
:class:`ConnectionPoolManager` maps a local+peer key onto those pools
and runs a periodic idle sweep.
:class:`~trpc_sdk.transport._pool.ShortConnFactory` dials a
one-shot conn that is force-closed on return and never queued; it lives
in :mod:`trpc_sdk.transport._pool._short_conn_factory` and is
re-exported here.
:func:`get_pool_connection` is the entry used by client transport:
short-conn options skip the manager; otherwise a per-loop manager is
cached in :class:`PoolCache`.

:class:`Pool` is a historical alias of :class:`ConnectionPoolManager`.
Types and the pooled wrapper live in
:mod:`trpc_sdk.transport._pool._types` and
:mod:`trpc_sdk.transport._pool._pooled_connection`; the
:class:`BasePool` contract lives in
:mod:`trpc_sdk.transport._pool._base`. Dialing lives in
:mod:`trpc_sdk.transport._pool._dial`.
"""

from __future__ import annotations

import asyncio
import socket
import threading
import time
from asyncio import BaseEventLoop
from typing import Callable
from typing import Dict
from typing import Optional

from trpc_sdk.abc import BaseClientTransportOption
from trpc_sdk.common import create_object_with_loop
from trpc_sdk.common import get_or_create_event_loop
from trpc_sdk.exceptions import ClientPoolClosedError
from trpc_sdk.exceptions import ClientPoolLimitError
from trpc_sdk.log import logger

from ._base import BasePool
from ._dial import dial_connection
from ._pooled_connection import PooledConnection
from ._short_conn_factory import ShortConnFactory
from ._types import ConnAddress
from ._types import ConnPoolOptions


class ConnectionPool(BasePool):
    """Idle-queue pool for one transport address.

    ``max_idle`` sizes the idle :class:`asyncio.Queue`. ``max_active``
    caps how many conns may exist; when the cap is hit, ``wait`` either
    blocks on that queue or raises :class:`ClientPoolLimitError`.

    Args:
        dial: Coroutine that opens a stream and returns its
            ``(reader, writer)`` pair.
        trans_opt: Client transport options, including the remote
            address.
        options: Idle / active / lifetime tunables. Must not be
            ``None`` at runtime; callers pass a :class:`ConnPoolOptions`.
        loop: Event loop bound to the idle queue, or ``None`` to pick
            the current one.
    """

    def __init__(
            self,
            dial: Callable,
            trans_opt: BaseClientTransportOption,
            options: Optional[ConnPoolOptions] = None,
            loop: Optional[BaseEventLoop] = None):
        super().__init__(address=trans_opt.address, loop=loop)
        self._dial = dial
        self._trans_opt = trans_opt
        self._pool_options = options
        self._closed = False
        self._active_conn_num = 0
        self._active_conns = set()
        self._idle_conns = create_object_with_loop(asyncio.Queue, self._pool_options.max_idle, loop=self.loop)

    @property
    def active_conn_num(self) -> int:
        """Number of conns this pool has created and not yet closed."""
        return self._active_conn_num

    @property
    def idle_conns_num(self) -> int:
        """How many conns currently sit in the idle queue."""
        return self._idle_conns.qsize()

    async def get(self, timeout: float) -> PooledConnection:
        """Take a healthy idle conn, or dial a new one.

        Idle entries that fail :meth:`_checker` are force-closed and
        skipped. If the active cap is reached, either wait on the idle
        queue (no re-check) or raise :class:`ClientPoolLimitError`.

        Args:
            timeout: Dial timeout in seconds for a new connection.

        Returns:
            A :class:`PooledConnection` the caller must ``close``.

        Raises:
            ClientPoolClosedError: :meth:`close` has already run.
            ClientPoolLimitError: Active cap hit and ``wait`` is False.
        """
        if self._closed:
            raise ClientPoolClosedError("ErrPoolClosed")

        for _ in range(self._idle_conns.qsize()):
            pool_conn: PooledConnection = self._idle_conns.get_nowait()
            if self._checker(pool_conn):
                pool_conn.close(True)
            else:
                return pool_conn

        if self._over_limit():
            if not self._pool_options.wait:
                raise ClientPoolLimitError("ErrPoolLimit")
            return await self._idle_conns.get()

        reader, writer = await self._dial(options=self._trans_opt, timeout=timeout, loop=self.loop)
        pool_conn = PooledConnection(reader=reader,
                                     writer=writer,
                                     loop=self.loop,
                                     pool=self,
                                     address=self.address,
                                     is_short=False)
        self._active_conn_num += 1
        self._active_conns.add(pool_conn)
        return pool_conn

    def _clear_all_conn(self) -> None:
        """Force-close every idle and in-use conn, then empty the active set."""
        for _ in range(self._idle_conns.qsize()):
            pool_conn: PooledConnection = self._idle_conns.get_nowait()
            pool_conn.force_close()
        for pool_conn in self._active_conns:
            pool_conn.force_close()
        self._active_conns.clear()

    def close(self) -> None:
        """Shut the pool and force-close every conn it still holds.

        Subtracts the idle queue size from ``_active_conn_num`` first so
        the subsequent force-closes line up with the live count.

        Args:
            None.
        """
        if self._closed:
            return

        self._closed = True
        self._active_conn_num -= self._idle_conns.qsize()
        self._clear_all_conn()

    def __del__(self):
        try:
            self.close()
        except Exception as err:  # pylint: disable=broad-exception-caught
            logger.error(f"address: {self.address}, total: {self._active_conn_num}, \
                        qsize: {self._idle_conns.qsize()} err: {err}")

    def put(self, pool_conn: PooledConnection, force_close: bool = False) -> None:
        """Return ``pool_conn`` to the idle queue, or close it.

        When the idle queue is already at ``max_idle``, the returning
        conn replaces the oldest idle entry and that oldest one is
        closed instead. A closed pool or ``force_close=True`` always
        tears the socket down and decrements the active count.

        Args:
            pool_conn: Conn previously issued by :meth:`get`.
            force_close: If True, skip the idle queue and close.
        """
        self._active_conns.discard(pool_conn)
        if not self._closed and not force_close:
            pool_conn.begin_time = time.time()
            if self._pool_options.max_idle > 0 and self._idle_conns.qsize() >= self._pool_options.max_idle:
                npc = self._idle_conns.get_nowait()
                self._idle_conns.put_nowait(pool_conn)
                pool_conn = npc
            else:
                self._idle_conns.put_nowait(pool_conn)
                pool_conn = None

        if pool_conn:
            self._active_conns.discard(pool_conn)
            pool_conn.force_close()
            self._active_conn_num -= 1

    def _over_limit(self) -> bool:
        """Return True when ``max_active`` is set and already reached."""
        return self._pool_options.max_active > 0 and self._active_conn_num >= self._pool_options.max_active

    def _checker(self, pool_conn: PooledConnection) -> bool:
        """Return True if ``pool_conn`` must not be handed out again.

        Remote errors always fail. Otherwise idle_timeout is measured
        from ``begin_time`` and max_conn_lifetime from ``created_time``.

        Args:
            pool_conn: Idle conn being considered for reuse.

        Returns:
            True when the caller should close this conn.
        """
        if pool_conn.check_remote_error():
            return True

        now = time.time()
        idle_timeout = self._pool_options.idle_timeout
        if idle_timeout > 0 and pool_conn.begin_time + idle_timeout < now:
            return True

        max_conn_lifetime = self._pool_options.max_conn_lifetime
        if max_conn_lifetime > 0 and pool_conn.created_time + max_conn_lifetime < now:
            return True

        return False

    def clean_idle_conn(self) -> None:
        """Drop idle conns that fail :meth:`_checker`, keep the rest.

        Called periodically by :class:`ConnectionPoolManager`. If the
        pool is already closed or its loop is not running, every conn
        is force-closed instead.

        Args:
            None.
        """
        if self._closed is False and self.loop.is_running():
            for _ in range(self._idle_conns.qsize()):
                pool_conn: PooledConnection = self._idle_conns.get_nowait()
                if self._checker(pool_conn):
                    logger.debug("pool conn remote error")
                    pool_conn.close(True)
                else:
                    self._idle_conns.put_nowait(pool_conn)
        else:
            self._clear_all_conn()


class ConnectionPoolManager:
    """Create and look up one :class:`ConnectionPool` per address key.

    The key is ``local_addr + peer`` plus a ``_sock`` / ``_addr``
    suffix, so the same remote with a different local bind does not
    share idle conns. Construction starts :meth:`__clean_idle_conn` on
    ``loop``, which sleeps 1000s between sweeps.

    Args:
        options: Shared :class:`ConnPoolOptions` for every pool this
            manager creates. ``None`` uses defaults.
        loop: Event loop for new pools and the idle-clean task.
    """

    def __init__(self, options: Optional[ConnPoolOptions] = None, loop: Optional[BaseEventLoop] = None):
        self._options = options or ConnPoolOptions()
        self._conn_pools: Dict[ConnAddress, ConnectionPool] = {}
        self._loop = loop or get_or_create_event_loop()
        self._loop.create_task(self.__clean_idle_conn())
        self._lock = asyncio.Lock()
        self._is_closed = False

    # Kept as a staticmethod so ``ConnectionPoolManager.dial`` and
    # ``ConnectionPool(dial=self.dial)`` keep working after the dial
    # logic moved to :mod:`trpc_sdk.transport._pool._dial`.
    dial = staticmethod(dial_connection)

    async def get(self, trans_opt: BaseClientTransportOption, timeout: float) -> PooledConnection:
        """Return a pooled conn for ``trans_opt``'s remote.

        ``timeout <= 0`` falls back to ``dial_timeout`` milliseconds
        converted to seconds. Missing pools are created under
        ``_lock``; a second lookup after the lock is not done, matching
        the historical race window.

        Args:
            trans_opt: Transport options, including sock / address /
                local_addr used as the pool key.
            timeout: Dial timeout in seconds, or ``0`` to use options.

        Returns:
            A :class:`PooledConnection` from that address's pool.
        """
        timeout = timeout if timeout and timeout > 0 else self._options.dial_timeout / 1000
        addr = trans_opt.sock or trans_opt.address
        index = ""
        if isinstance(addr, socket.socket):
            local_addr = addr.getsockname()
            peer_addr = addr.getpeername()
            if local_addr:
                index = f"{local_addr}"
            index = f"{index}_{peer_addr}_sock"
        else:
            if trans_opt.local_addr:
                index = f"{trans_opt.local_addr}"
            index = f"{index}_{addr}_addr"
        conn_pool: ConnectionPool = self._conn_pools.get(index, None)
        if conn_pool:
            return await conn_pool.get(timeout)

        async with self._lock:
            new_pool = ConnectionPool(dial=self.dial, trans_opt=trans_opt, options=self._options, loop=self._loop)
            self._conn_pools[index] = new_pool
            return await new_pool.get(timeout)

    async def __clean_idle_conn(self):
        while self._loop.is_running() and self._is_closed is False:
            await asyncio.sleep(1000)
            for pool in self._conn_pools.values():
                pool.clean_idle_conn()
        logger.warning(f"exist loop ({id(self._loop)}) because stop or close {self._is_closed}")

    def __del__(self):
        """Mark closed and shut every address pool this manager owns."""
        self._is_closed = True
        for pool in self._conn_pools.values():
            pool.close()
        self._conn_pools.clear()


Pool = ConnectionPoolManager


class PoolCache(threading.local):
    """Per-thread map from event loop (or address) to a pool manager.

    :func:`get_pool_connection` stores the manager under the loop id
    and also records ``address -> loop`` so a later call on a different
    loop can warn and drop the stale entry.

    Args:
        None.
    """

    def get(self, key, default=None):
        """Return the value for ``key``, or ``default`` if missing.

        Args:
            key: Loop or address used as the cache key.
            default: Returned when ``key`` is absent.

        Returns:
            Cached object, or ``default``.
        """
        return self.__dict__.get(key, default)

    def __getitem__(self, key):
        return self.__dict__[key]

    def __setitem__(self, key, value):
        self.__dict__[key] = value


_short_conn_factory = ShortConnFactory()

_default_connection_pools = PoolCache()


async def get_pool_connection(options: BaseClientTransportOption,
                              timeout: float,
                              loop: Optional[BaseEventLoop] = None) -> PooledConnection:
    """Return a pooled (or short) connection for ``options``.

    ``options.short_conn`` uses :class:`ShortConnFactory` and never
    touches the manager cache. Otherwise a :class:`ConnectionPoolManager`
    is created once per event loop and reused. If the same address was
    previously bound to a different loop, that stale loop entry is
    deleted after a warning.

    Args:
        options: Client transport options, including ``pool_opts`` when
            a new manager must be built.
        timeout: Dial timeout in seconds; ``<= 0`` uses pool options.
        loop: Event loop that owns the manager; ``None`` picks current.

    Returns:
        A :class:`PooledConnection` ready for ``read`` / ``write``.
    """
    loop = loop or get_or_create_event_loop()

    if options.short_conn:
        return await _short_conn_factory.get(options=options, timeout=timeout, loop=loop)

    conn_pool_mgr = _default_connection_pools.get(loop, None)
    if not conn_pool_mgr:
        old_loop: BaseEventLoop = _default_connection_pools.get(options.address, None)
        if old_loop:
            if id(old_loop) != id(loop):
                logger.warning(f"address: {options.address} exist different loop, \
                               old {id(old_loop)} != current loop: {loop}")
            if old_loop in _default_connection_pools.__dict__:
                del _default_connection_pools.__dict__[old_loop]
        _default_connection_pools[options.address] = loop

        pool_opts = getattr(options, "pool_opts", None)
        conn_pool_mgr = ConnectionPoolManager(options=pool_opts, loop=loop)
        _default_connection_pools[loop] = conn_pool_mgr

    return await conn_pool_mgr.get(options, timeout)
