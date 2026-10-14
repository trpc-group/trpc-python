# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Event-loop threads that a merge-mode worker spreads its work over.

In ``merge`` mode one process both accepts connections and runs handlers.
With ``io_thread_num > 1`` the main thread only accepts (see
:class:`~trpc_sdk.transport.Acceptor`) and the accepted sockets are served by
``io_thread_num`` :class:`LoopThread` instances, each owning one event loop
for its whole lifetime. ``io_handle_type: separate`` uses the same class for
both of its thread kinds: handle threads that run user code, and extra I/O
threads that only read, frame and write.

Threads instead of processes means the GIL serializes Python bytecode, so the
only work that really overlaps is I/O waiting and C extensions that release the
GIL (protobuf, numpy, inference runtimes). A service whose cost is pure Python
gets more throughput from ``server.mode: separate``, where handlers run in rpc
task processes. A worker running with ``io_thread_num: 1`` (and the merge
thread model) creates no :class:`LoopThread` at all and serves on the main
loop.

Anything a handler touches must therefore be either thread-confined or built
per loop. Transports in particular are owned by the loop that created them:
``transport.write()`` from another thread is not safe. That is why every
serving loop opens its own connections to the task processes, and why the
separate thread model hands responses back through
:class:`~trpc_sdk.protocol.ConnWriter`.
"""

from __future__ import annotations

import asyncio
import inspect
import socket
import threading
import time
from concurrent.futures import Future
from concurrent.futures import TimeoutError as FutureTimeoutError
from typing import Any
from typing import Awaitable
from typing import Callable
from typing import List
from typing import Optional
from typing import Sequence

from trpc_sdk.config import TrpcConfig
from trpc_sdk.log import logger
from trpc_sdk.utils import get_async_lib

# ``init(loop)``: runs on the new thread before the loop turns.
LoopInit = Callable[[asyncio.AbstractEventLoop], None]
# ``before_stop()``: coroutine factory run on the loop right before it stops.
BeforeStop = Callable[[], Awaitable[None]]

# Seconds every join gets after the shared stop deadline is spent.
_JOIN_GRACE = 0.5


def connect_accepted(loop: asyncio.AbstractEventLoop,
                     sock: socket.socket,
                     protocol_factory: Callable[[], asyncio.BaseProtocol],
                     ssl_context: Optional[Any],
                     label: str) -> None:
    """Wrap an accepted socket in a transport on ``loop``; runs on ``loop``'s thread.

    ``connect_accepted_socket`` is a coroutine (it may have to perform a TLS
    handshake), so it is scheduled as a task rather than awaited. Nothing else
    observes that task, so a failed hand-off closes ``sock`` here instead of
    leaking the fd behind an "exception never retrieved" warning.

    Args:
        loop: Loop that will own the transport. Must be the running loop of
            the calling thread.
        sock: Socket returned by ``accept()``, already non-blocking.
        protocol_factory: Builds the protocol serving this connection.
        ssl_context: Server-side ``SSLContext``, or ``None`` for plaintext.
        label: Who owns ``loop``, for log messages.
    """

    def _on_attached(task: asyncio.Task) -> None:
        if task.cancelled():
            sock.close()
            return
        err = task.exception()
        if err is not None:
            logger.error("[merge worker] %s failed to serve a connection: %s", label, repr(err))
            sock.close()

    task = loop.create_task(loop.connect_accepted_socket(protocol_factory, sock, ssl=ssl_context))
    task.add_done_callback(_on_attached)


class LoopThread:
    """One OS thread running one event loop.

    The loop is created **inside** the thread, in :meth:`_run`. An event loop
    holds an epoll/kqueue fd and remembers the thread it belongs to, so it
    cannot be built by the caller and handed over. The loop is also closed
    on that thread, once it has stopped: :meth:`stop` joins with a timeout,
    so the caller can never be sure the thread is gone by the time it
    returns.

    Every public method is called from the thread that owns the worker
    (normally the main thread) and crosses over through
    ``call_soon_threadsafe`` / ``run_coroutine_threadsafe``.

    Attributes:
        index: Position in the worker's thread list. This is what the
            scheduler in :mod:`trpc_sdk.transport` selects, so it plays
            the role a worker index plays in ``dispatcher`` mode.
        loop: Event loop of this thread; ``None`` until the thread created it.
        start_error: What ``init`` raised, re-raised by :meth:`wait_ready`.
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
        self.start_error: Optional[BaseException] = None
        self._init: Optional[LoopInit] = None
        self._thread = threading.Thread(target=self._run, name=f"{name_prefix}-{index}", daemon=True)
        # Set once ``init`` finished, from inside the loop on success: waiting
        # on it means the loop is not merely created but already turning and
        # safe to schedule onto. Also set when ``init`` failed.
        self._ready = threading.Event()

    @property
    def name(self) -> str:
        """OS thread name, for log messages."""
        return self._thread.name

    def start(self, init: Optional[LoopInit] = None) -> None:
        """Start the thread and return at once; see :meth:`wait_ready`.

        Starting every thread before waiting on any of them is what lets
        several slow ``init`` calls (an ``on_start`` hook loading a model)
        run in parallel instead of one after another.

        Args:
            init: Runs on the new thread, before ``run_forever``. The loop is
                not running yet, so ``init`` drives asynchronous steps with
                ``loop.run_until_complete``; that is also why it is a plain
                function and not a coroutine, since a user protocol starter
                called from it may itself call ``run_until_complete``.
        """
        self._init = init
        self._thread.start()

    def wait_ready(self, timeout: float) -> None:
        """Block until ``init`` finished and the loop turns.

        Args:
            timeout: Seconds to wait.

        Raises:
            TimeoutError: ``init`` did not finish in time. The thread keeps
                running ``init``; Python cannot interrupt it.
            BaseException: Whatever ``init`` raised.
        """
        if not self._ready.wait(max(0.0, timeout)):
            raise TimeoutError(f"{self.name} not ready within {timeout:.3f}s")
        if self.start_error is not None:
            raise self.start_error

    def _run(self) -> None:
        """Thread entry point: create the loop, run ``init``, serve, then clean up.

        ``set_event_loop`` matters beyond bookkeeping: handler code that calls
        ``asyncio.get_event_loop()`` off the running-loop path must find this
        thread's loop and not the worker's main one.
        """
        loop = get_async_lib(TrpcConfig.global_config().enable_uvloop).new_event_loop()
        asyncio.set_event_loop(loop)
        self.loop = loop
        try:
            if self._init is not None:
                self._init(loop)
        except BaseException as err:  # pylint: disable=broad-except
            self.start_error = err
            self._shutdown_loop(loop)
            self._ready.set()
            return

        # Runs on the first turn of the loop, i.e. once run_forever is in.
        loop.call_soon(self._ready.set)
        try:
            loop.run_forever()
        except BaseException as err:  # pylint: disable=broad-except
            logger.error("[merge worker] %s exited with error: %s", self.name, repr(err))
        finally:
            self._shutdown_loop(loop)

    def _shutdown_loop(self, loop: asyncio.AbstractEventLoop) -> None:
        """Cancel what is left on the stopped loop, then close it; runs on this thread.

        Cancelling and awaiting the leftovers is what keeps "Task was
        destroyed but it is pending" out of the shutdown log: handlers still
        in flight when the drain period ran out end here.

        Args:
            loop: This thread's loop, no longer running.
        """
        try:
            pending = [task for task in asyncio.all_tasks(loop) if not task.done()]
            for task in pending:
                task.cancel()
            if pending:
                loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
            loop.run_until_complete(loop.shutdown_asyncgens())
        except BaseException as err:  # pylint: disable=broad-except
            logger.error("[merge worker] %s failed to clean up its loop: %s", self.name, repr(err))
        finally:
            loop.close()

    def submit(self, func: Callable[[], Any], timeout: Optional[float] = None) -> Any:
        """Run ``func`` on this thread's loop and block until it finishes.

        Used for the setup a listener has to perform on the loop that will
        serve it, such as an aiohttp ``AppRunner.setup()`` whose ``Server``
        binds to whichever loop is running when it is constructed.

        Args:
            func: Callable executed on the loop thread. If it returns an
                awaitable, that awaitable is awaited and its result returned.
            timeout: Seconds to wait; ``None`` waits forever.

        Returns:
            Whatever ``func`` returned, after awaiting it when applicable.

        Raises:
            RuntimeError: The loop is not running (not started, or closed).
            TimeoutError: ``func`` did not finish in time; it was cancelled.
            Exception: Whatever ``func`` raised, re-raised in the caller.
        """
        future = self._schedule(func)
        try:
            return future.result(timeout)
        except FutureTimeoutError as err:
            future.cancel()
            raise TimeoutError(f"{self.name}: call did not finish within {timeout}s") from err

    def _schedule(self, func: Callable[[], Any]) -> Future:
        """Start ``func`` on the loop without waiting; see :meth:`submit`.

        Args:
            func: Callable executed on the loop thread.

        Returns:
            Future resolved with the result of ``func``.

        Raises:
            RuntimeError: The loop is not running.
        """
        loop = self.loop
        if loop is None or loop.is_closed():
            raise RuntimeError(f"{self.name} is not running")

        async def _call() -> Any:
            result = func()
            if inspect.isawaitable(result):
                result = await result
            return result

        return asyncio.run_coroutine_threadsafe(_call(), loop)

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
            self.loop.call_soon_threadsafe(connect_accepted, self.loop, sock, protocol_factory, ssl_context,
                                           self.name)
        except RuntimeError as err:
            # The loop closed between accept and hand-off (worker shutting
            # down). Drop the connection rather than leaking the fd.
            logger.warning("[merge worker] %s refused a connection: %s", self.name, repr(err))
            sock.close()

    def stop(self, timeout: float, before_stop: Optional[BeforeStop] = None) -> bool:
        """Run ``before_stop`` on the loop, stop it and join the thread.

        Safe to call on a thread that never started, or whose ``init``
        failed.

        Args:
            timeout: Seconds for the whole sequence.
            before_stop: Coroutine factory awaited on the loop first; its
                errors and timeout are only logged.

        Returns:
            False if the thread did not exit in time. It is a daemon thread,
            so it does not keep the process alive.
        """
        hook = (lambda _: before_stop()) if before_stop is not None else None
        return stop_loop_threads([self], hook, timeout)

    def _request_stop(self) -> None:
        """Ask the loop to stop after the callbacks already queued on it."""
        loop = self.loop
        if loop is None or loop.is_closed():
            return
        try:
            loop.call_soon_threadsafe(loop.stop)
        except RuntimeError:
            # Closed in the meantime; the join below still applies.
            pass

    def _join(self, timeout: float) -> bool:
        """Join the thread for at most ``timeout`` seconds.

        Returns:
            True if the thread is gone (or was never started).
        """
        if self._thread.ident is None:
            return True
        self._thread.join(max(0.0, timeout))
        return not self._thread.is_alive()


def stop_loop_threads(threads: Sequence[LoopThread],
                      before_stop: Optional[Callable[[int], Awaitable[None]]],
                      timeout: float) -> bool:
    """Stop several loop threads in parallel, under one shared deadline.

    Every ``before_stop`` is started before any is waited on, so N threads
    each needing a few seconds of cleanup take a few seconds in total, not N
    times that. Then every loop is asked to stop and every thread is joined,
    still against the same deadline.

    ``loop.stop`` is queued behind whatever callbacks are already on the
    loop, so work another thread posted there (a response handed back by a
    handle thread) still runs before the loop stops.

    Args:
        threads: Threads to stop; never-started ones are skipped.
        before_stop: Called as ``before_stop(thread.index)`` on each thread's
            loop; errors and timeouts are only logged.
        timeout: Seconds for the whole sequence.

    Returns:
        False if any thread did not exit in time.
    """
    deadline = time.monotonic() + timeout
    pending: List[tuple] = []
    for thread in threads:
        if before_stop is None or thread.loop is None or thread.loop.is_closed():
            continue
        try:
            pending.append((thread, thread._schedule(  # pylint: disable=protected-access
                lambda index=thread.index: before_stop(index))))
        except RuntimeError:
            continue
    for thread, future in pending:
        try:
            future.result(max(0.0, deadline - time.monotonic()))
        except FutureTimeoutError:
            future.cancel()
            logger.error("[merge worker] %s: cleanup did not finish within %.1fs", thread.name, timeout)
        except BaseException as err:  # pylint: disable=broad-except
            logger.error("[merge worker] %s: cleanup failed: %s", thread.name, repr(err))

    for thread in threads:
        thread._request_stop()  # pylint: disable=protected-access

    # A cleanup that used the whole deadline was just cancelled; its loop
    # still needs a turn to stop, so the join always gets a short grace.
    deadline = max(deadline, time.monotonic() + _JOIN_GRACE)
    all_exited = True
    for thread in threads:
        if not thread._join(deadline - time.monotonic()):  # pylint: disable=protected-access
            logger.error("[merge worker] %s did not exit within %.1fs; leaving it behind", thread.name, timeout)
            all_exited = False
    return all_exited
