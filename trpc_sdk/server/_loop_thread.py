# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Event-loop threads that a merge-mode worker spreads connections over.

In ``merge`` mode one process both accepts connections and runs handlers. The
main thread only accepts (see :mod:`~trpc_sdk.server._acceptor`) and the accepted
sockets are served by ``worker_thread_num`` :class:`LoopThread` instances, each
owning one event loop for its whole lifetime.

Threads instead of processes means the GIL serializes Python bytecode, so the
only work that really overlaps is I/O waiting and C extensions that release the
GIL (protobuf, numpy, inference runtimes). A service whose cost is pure Python
gets more throughput from ``separate`` mode, where handlers run in rpc task
processes. A worker configured with ``worker_thread_num: 1`` creates no
:class:`LoopThread` at all and serves on the main loop.

Anything a handler touches must therefore be either thread-confined or built
per loop. Transports in particular are owned by the loop that created them:
``transport.write()`` from another thread is not safe. The worker <-> task
connections are still created once on the main loop, so a handler running on a
loop thread must not produce to a task until those connections are made
per-thread.
"""

from __future__ import annotations

import asyncio
import inspect
import socket
import threading
from typing import Any
from typing import Callable
from typing import Optional

from trpc_sdk.log import logger
from trpc_sdk.utils import asynclib


class LoopThread:
    """One OS thread running one event loop, fed sockets by the acceptor.

    The loop is created **inside** the thread, in :meth:`_run`. An event loop
    holds an epoll/kqueue fd and remembers the thread it belongs to, so it
    cannot be built by the caller and handed over.

    Every public method except :meth:`_run` is called from the thread that owns
    the worker (normally the main thread) and crosses over through
    ``call_soon_threadsafe`` / ``run_coroutine_threadsafe``.

    Attributes:
        index: Position in the worker's thread list. This is what the
            scheduler in :mod:`trpc_sdk.transport` selects, so it plays
            the role a worker index plays in ``dispatcher`` mode.
        loop: Event loop of this thread; ``None`` until :meth:`start` returns.
    """

    def __init__(self, index: int, name_prefix: str = "trpc-loop") -> None:
        """Create the thread object; no loop exists yet.

        Args:
            index: Position in the worker's thread list, used for scheduling
                and for the thread name.
            name_prefix: Prefix of the OS thread name, shown in stack dumps.
        """
        self.index: int = index
        self.loop: Optional[asyncio.AbstractEventLoop] = None
        self._thread = threading.Thread(target=self._run,
                                        name=f"{name_prefix}-{index}",
                                        daemon=True)
        # Set from inside the loop, so waiting on it means the loop is not
        # merely created but already turning and safe to schedule onto.
        self._running = threading.Event()

    def start(self) -> None:
        """Start the thread and block until its loop is running.

        Blocking here is what makes the caller's next :meth:`attach_socket` or
        :meth:`submit` legal: both need ``self.loop`` to exist and to be
        turning, otherwise the callback they queue would never fire.
        """
        self._thread.start()
        self._running.wait()

    def _run(self) -> None:
        """Thread entry point: create the loop, then serve until stopped.

        ``set_event_loop`` matters beyond bookkeeping: handler code that calls
        ``asyncio.get_event_loop()`` off the running-loop path must find this
        thread's loop and not the worker's main one.
        """
        loop = asynclib.new_event_loop()
        asyncio.set_event_loop(loop)
        self.loop = loop
        # Runs on the first turn of the loop, i.e. once run_forever is in.
        loop.call_soon(self._running.set)
        try:
            loop.run_forever()
        except Exception as err:  # pylint: disable=broad-except
            logger.error("[merge worker] loop thread %s exited with error: %s", self.index, repr(err))
        finally:
            self._running.clear()

    def submit(self, func: Callable[[], Any]) -> Any:
        """Run ``func`` on this thread's loop and block until it finishes.

        Used for the setup a front end has to perform on the loop that will
        serve it, such as an aiohttp ``AppRunner.setup()`` whose ``Server``
        binds to whichever loop is running when it is constructed.

        Args:
            func: Callable executed on the loop thread. If it returns an
                awaitable, that awaitable is awaited and its result returned.

        Returns:
            Whatever ``func`` returned, after awaiting it when applicable.

        Raises:
            RuntimeError: The loop is not running.
            Exception: Whatever ``func`` raised, re-raised in the caller.
        """
        if self.loop is None:
            raise RuntimeError(f"loop thread {self.index} is not started")

        async def _call() -> Any:
            result = func()
            if inspect.isawaitable(result):
                result = await result
            return result

        return asyncio.run_coroutine_threadsafe(_call(), self.loop).result()

    def attach_socket(self,
                      sock: socket.socket,
                      protocol_factory: Callable[[], asyncio.BaseProtocol],
                      ssl_context: Optional[Any] = None) -> None:
        """Hand an already accepted socket to this thread, without blocking.

        Called from the acceptor thread on the hot path, so it only queues the
        hand-off: the transport is built on the loop thread, which is the only
        thread allowed to touch it afterwards.

        Args:
            sock: Socket returned by ``accept()`` on the listen socket.
            protocol_factory: Builds the protocol serving this connection.
                Must not be shared across loops if the protocol captures a
                loop at construction time.
            ssl_context: Server-side ``SSLContext`` for a TLS endpoint, or
                ``None`` for plaintext.
        """
        try:
            self.loop.call_soon_threadsafe(self._connect_accepted_socket, sock, protocol_factory, ssl_context)
        except RuntimeError as err:
            # The loop closed between accept and hand-off (worker shutting
            # down). Drop the connection rather than leaking the fd.
            logger.warning("[merge worker] loop thread %s refused a connection: %s", self.index, repr(err))
            sock.close()

    def _connect_accepted_socket(self,
                                 sock: socket.socket,
                                 protocol_factory: Callable[[], asyncio.BaseProtocol],
                                 ssl_context: Optional[Any]) -> None:
        """Wrap ``sock`` in a transport; runs on the loop thread.

        ``connect_accepted_socket`` is a coroutine (it may have to perform a
        TLS handshake), so it is scheduled as a task rather than awaited.

        Args:
            sock: Socket handed over by :meth:`attach_socket`.
            protocol_factory: Builds the protocol serving this connection.
            ssl_context: Server-side ``SSLContext``, or ``None``.
        """
        task = self.loop.create_task(
            self.loop.connect_accepted_socket(protocol_factory, sock, ssl=ssl_context))
        task.add_done_callback(lambda done: self._on_socket_attached(done, sock))

    def _on_socket_attached(self, task: asyncio.Task, sock: socket.socket) -> None:
        """Close ``sock`` when the transport could not be built.

        Nothing else observes the task, so a failed hand-off would otherwise
        be an "exception never retrieved" warning plus a leaked fd.

        Args:
            task: The finished ``connect_accepted_socket`` task.
            sock: Socket that task was given.
        """
        if task.cancelled():
            sock.close()
            return
        err = task.exception()
        if err is not None:
            logger.error("[merge worker] loop thread %s failed to serve a connection: %s", self.index, repr(err))
            sock.close()

    def stop(self) -> None:
        """Stop the loop, join the thread, and close the loop.

        Safe to call on a thread that was never started. ``loop.close()`` runs
        in the caller only after the join, when no one is turning it anymore.
        """
        if self.loop is None:
            return
        try:
            self.loop.call_soon_threadsafe(self.loop.stop)
        except RuntimeError:
            # Already stopped or closed; the join below still applies.
            pass
        if self._thread.is_alive():
            self._thread.join()
        if not self.loop.is_closed():
            self.loop.close()
