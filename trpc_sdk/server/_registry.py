# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Background naming-service registration for one :class:`~trpc_sdk.server._cython._service.ServiceAdapter`.

:func:`start_service_registry` gives every service with a configured
registry a :class:`ServiceRegister`, which runs register / heartbeat /
deregister on a :class:`RegistryThread` with its own event loop, not the
worker's. The server adapter's ``registry_semaphore`` (initialised to
``service_num``) is the handshake that lets ``TrpcServerAdapter.close``
wait for deregister before SIGTERM.
"""

from __future__ import annotations

import asyncio
import threading
import time
from typing import Any
from typing import Awaitable
from typing import Callable
from typing import Optional

from trpc_sdk.config import TrpcConfig
from trpc_sdk.utils import get_async_lib
from trpc_sdk.utils import register_spinlock
from trpc_sdk.utils import remove_lock_file
from trpc_sdk.utils import warm_up


class RegistryThread(threading.Thread):
    """Background thread that runs naming-service register/heartbeat on its own loop.

    Registration does **not** run on the worker's event loop. This thread
    creates ``get_or_create_event_loop()`` in ``__init__`` (still on the
    caller's thread) and installs it with ``asyncio.set_event_loop`` once
    :meth:`run` starts.

    ``register_coro_func`` is expected to block until cancelled: after the
    plugin's ``register`` returns, the loop is idle, ``time.sleep(1)``
    pauses, and the coroutine is started again. That retry cadence is how
    heartbeat / re-register is driven, not a one-shot register.

    ``deregister_done`` is the server adapter's ``registry_semaphore``, initialised
    to ``service_num`` (see ``TrpcServerAdapter.__init__``). :meth:`run`
    ``acquire``s one permit at start and ``release``s it after
    ``deregister_coro_func`` finishes, so ``TrpcServerAdapter.close`` can
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
            deregister_done: Server adapter ``registry_semaphore``. Acquired at the
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
        self._loop: asyncio.AbstractEventLoop = get_async_lib(
            TrpcConfig.global_config().enable_uvloop).new_event_loop()

    def terminate(self) -> None:
        """Ask the retry loop to stop and cancel the in-flight register task.

        Must use ``call_soon_threadsafe`` because the caller (typically
        :meth:`trpc_sdk.server._cython._service.ServiceAdapter.close`) is on another thread.
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


class ServiceRegister:
    """Naming-service registration for one service endpoint.

    Only built for a service whose ``opt.registry`` is set, so a service
    without a registry never starts a thread and never consumes a
    ``registry_semaphore`` permit. ``opt`` fields are read when the
    coroutines run, not captured at construction.
    """

    def __init__(self, opt: Any) -> None:
        """Record the endpoint options; no thread is started yet.

        Args:
            opt: The service's :class:`~trpc_sdk.server._option.ServiceOption`;
                ``registry``, ``registry_option``, ``registry_delay``,
                ``service_name``, ``address`` and ``app`` are read.
        """
        self.opt = opt
        self.thread: Optional[RegistryThread] = None

    def start(self) -> None:
        """Start the :class:`RegistryThread`.

        ``registry_option == 1`` warms shared-memory spinlocks before the
        thread starts.
        """
        if self.opt.registry_option == 1:
            warm_up()
        self.thread = RegistryThread(self._register,
                                     self._deregister,
                                     self.opt.app.server_adapter.registry_semaphore)
        self.thread.start()

    def stop(self) -> None:
        """Cancel the registry thread; it still deregisters before exiting."""
        if self.thread is not None:
            self.thread.terminate()

    async def _register(self) -> None:
        """Register this service; runs on ``RegistryThread``'s loop.

        ``registry_option == 1`` takes the shared-memory spinlock first;
        otherwise the thread sleeps ``registry_delay`` milliseconds.
        """
        if self.opt.registry_option == 1:
            await register_spinlock()
        else:
            await asyncio.sleep(self.opt.registry_delay / 1000)

        await self.opt.registry.register(self.opt.service_name, self.opt.address)

    async def _deregister(self) -> None:
        """Deregister this service; runs on ``RegistryThread``'s loop."""
        await self.opt.registry.deregister(self.opt.service_name, self.opt.address)
        remove_lock_file()


def start_service_registry(server: Any) -> None:
    """Start naming-service registration for every service on ``server``.

    :meth:`~trpc_sdk.server.Server.serve` appends this to the worker's
    ``ready_callbacks``, so it runs in the server process once the listen
    addresses are bound, after every child has been forked. Children never
    see a :class:`RegistryThread`: fork does not copy threads, and none
    exists before the fork any more. ``server.registry_after_fork`` no
    longer affects the timing; ``registry_delay`` is an extra wait after
    the bind.

    Args:
        server: The :class:`~trpc_sdk.server.Server` whose ``service_adapters``
            map is walked. Untyped to avoid importing ``_server``.
    """
    for service in server.service_adapters.values():
        if service.opt.registry is None:
            continue
        service_register = ServiceRegister(service.opt)
        service_register.start()
        service.service_register = service_register
