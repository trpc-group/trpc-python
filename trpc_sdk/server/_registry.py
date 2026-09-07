# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Background naming-service registration for one :class:`~trpc_sdk.server._service.Service`.

A service with a configured registry starts a :class:`RegistryThread` so
register / heartbeat / deregister run on a dedicated event loop, not the
master's. The engine's ``registry_semaphore`` (initialised to
``service_num``) is the handshake that lets ``TrpcServerEngine.close``
wait for deregister before SIGTERM.
"""

from __future__ import annotations

import asyncio
import threading
import time
from typing import Awaitable
from typing import Callable
from typing import Optional

from trpc.utils import asynclib


class RegistryThread(threading.Thread):
    """Background thread that runs naming-service register/heartbeat on its own loop.

    Registration does **not** run on the master's event loop. This thread
    creates ``get_or_create_event_loop()`` in ``__init__`` (still on the
    caller's thread) and installs it with ``asyncio.set_event_loop`` once
    :meth:`run` starts.

    ``register_coro_func`` is expected to block until cancelled: after the
    plugin's ``register`` returns, the loop is idle, ``time.sleep(1)``
    pauses, and the coroutine is started again. That retry cadence is how
    heartbeat / re-register is driven, not a one-shot register.

    ``deregister_done`` is the engine's ``registry_semaphore``, initialised
    to ``service_num`` (see ``TrpcServerEngine.__init__``). :meth:`run`
    ``acquire``s one permit at start and ``release``s it after
    ``deregister_coro_func`` finishes, so ``TrpcServerEngine.close`` can
    ``acquire(True, 10)`` once per configured service before SIGTERM.
    A service with no registry never starts this thread and therefore
    never consumes a permit.

    :meth:`terminate` is called from another thread (master shutdown) and
    must cancel the in-flight task via ``call_soon_threadsafe``.
    """

    def __init__(
        self,
        register_coro_func: Callable[[], Awaitable[None]],
        deregister_coro_func: Callable[[], Awaitable[None]],
        deregister_done: threading.Semaphore,
        daemon: bool = True,
    ) -> None:
        """Record the coroutines and semaphore; the loop is created but not running.

        Args:
            register_coro_func: Coroutine factory that registers this service
                and stays alive for heartbeat / retry.
            deregister_coro_func: Coroutine factory that deregisters this
                service; run once after the retry loop exits.
            deregister_done: Engine ``registry_semaphore``. Acquired at the
                start of :meth:`run`, released after deregister.
            daemon: Passed through to ``threading.Thread.daemon``.
        """
        threading.Thread.__init__(self)
        self.register_coro_func: Callable[[], Awaitable[None]] = register_coro_func
        self.deregister_coro_func: Callable[[], Awaitable[None]] = deregister_coro_func
        self._task: Optional[asyncio.Task] = None
        self._running: bool = True
        self.deregister_done: threading.Semaphore = deregister_done
        self.daemon = daemon
        self._loop: asyncio.AbstractEventLoop = asynclib.new_event_loop()

    def terminate(self) -> None:
        """Ask the retry loop to stop and cancel the in-flight register task.

        Must use ``call_soon_threadsafe`` because the caller (typically
        :meth:`trpc_sdk.server._service.Service.close`) is on another thread.
        """
        self._running = False
        if self._task is not None:
            self._loop.call_soon_threadsafe(self._task.cancel)

    def run(self) -> None:
        """Acquire the semaphore, retry register until cancelled, then deregister.

        The ``while self._running`` + ``time.sleep(1)`` loop is the heartbeat
        retry cadence after ``register_coro_func`` returns, not a one-shot
        register. ``CancelledError`` from :meth:`terminate` is swallowed so
        the deregister path still runs.
        """
        self.deregister_done.acquire()
        asyncio.set_event_loop(self._loop)
        while self._running:
            self._task = asyncio.ensure_future(self.register_coro_func())
            try:
                self._loop.run_until_complete(self._task)
                time.sleep(1)
            except asyncio.CancelledError:
                pass

        self._loop.run_until_complete(asyncio.wait_for(self.deregister_coro_func(), 10))
        self.deregister_done.release()
