# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Thread model of one serving process: which loops a server transport serves on.

A server transport is created before fork, but the loops it serves on only
exist after it, so they cannot be handed to its constructor. Instead the
runtime worker builds a :class:`ThreadModel` after fork, before any loop thread
starts, and installs it with :func:`install_thread_model`; every built-in
transport reads it with :func:`current_thread_model` in ``listen``. The
transport decides *what* is served, the model *where*.

With no model installed, :func:`current_thread_model` returns a single-reactor
model on the calling thread's loop, which is what a transport used outside a
runtime worker (a test, a script) gets.

A model must not be installed before fork: installing records the pid, and a
model inherited by a child is rejected rather than silently naming loops that
do not exist there.

Resources a transport cannot release in ``stop`` (datagram endpoints, aiohttp
runners) are kept on the model, per reactor, so the runtime can release them on
that reactor's loop when it stops.

Every reactor knows the thread that owns its loop. A call made on that thread
runs directly; only a call from another thread is posted with
``run_coroutine_threadsafe`` / ``call_soon_threadsafe``. That is what keeps a
reactor correct whichever thread its caller happens to be on.
"""

from __future__ import annotations

import asyncio
import inspect
import os
import socket
import threading
from concurrent.futures import Future
from concurrent.futures import TimeoutError as FutureTimeoutError
from contextlib import contextmanager
from dataclasses import dataclass
from dataclasses import field
from enum import Enum
from typing import Any
from typing import Callable
from typing import Dict
from typing import Iterator
from typing import List
from typing import Optional
from typing import Protocol
from typing import Sequence
from typing import Tuple

from trpc_sdk.log import logger

class BaseReactor(Protocol):
    """One event loop a server transport can serve on; owned by the runtime.

    ``submit`` and ``attach_socket`` are usually called from the acceptor
    thread, the thread ``listen`` runs on, but may be called from any thread:
    a reactor runs the call directly on its own thread and posts it otherwise.

    Attributes:
        index: Position of the reactor in its group, read by the scheduler.
        loop: Its event loop; ``None`` until a loop thread created it.
        owner_ident: ``threading.get_ident()`` of the thread owning ``loop``;
            ``None`` until that thread exists.
    """

    index: int
    loop: Optional[asyncio.AbstractEventLoop]
    owner_ident: Optional[int]

    def in_loop_thread(self) -> bool:
        """Whether the calling thread owns the reactor's loop."""

    def submit(self, func: Callable[[], Any], timeout: Optional[float] = None) -> Any:
        """Run ``func`` on the reactor's loop, await an awaitable result, and return it."""

    def attach_socket(self,
                      sock: socket.socket,
                      protocol_factory: Callable[[], asyncio.BaseProtocol],
                      ssl_context: Optional[Any] = None) -> None:
        """Serve an accepted socket on the reactor's loop, without blocking."""


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
        loop: Loop that will own the transport. Must belong to the calling
            thread; it need not be running yet.
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
            logger.error("[thread model] %s failed to serve a connection: %s", label, repr(err))
            sock.close()

    task = loop.create_task(loop.connect_accepted_socket(protocol_factory, sock, ssl=ssl_context))
    task.add_done_callback(_on_attached)


def attach_on_loop(loop: Optional[asyncio.AbstractEventLoop],
                   sock: socket.socket,
                   protocol_factory: Callable[[], asyncio.BaseProtocol],
                   ssl_context: Optional[Any],
                   *,
                   in_loop_thread: bool,
                   label: str) -> None:
    """Serve an accepted socket on ``loop``, directly or by posting it there; never blocks.

    A loop that does not exist or is closed (the worker is shutting down)
    refuses the connection: the socket is closed rather than leaked.

    Args:
        loop: Loop that will own the transport.
        sock: Socket returned by ``accept()``, already non-blocking.
        protocol_factory: Builds the protocol serving this connection.
        ssl_context: Server-side ``SSLContext``, or ``None`` for plaintext.
        in_loop_thread: Whether the calling thread owns ``loop``.
        label: Who owns ``loop``, for log messages.
    """
    if in_loop_thread and loop is not None:
        connect_accepted(loop, sock, protocol_factory, ssl_context, label)
        return
    try:
        if loop is None or loop.is_closed():
            raise RuntimeError(f"{label} is not running")
        loop.call_soon_threadsafe(connect_accepted, loop, sock, protocol_factory, ssl_context, label)
    except RuntimeError as err:
        # Also raised by call_soon_threadsafe when the loop closed between
        # the check and the call.
        logger.warning("[thread model] %s refused a connection: %s", label, repr(err))
        sock.close()


def schedule_on_loop(loop: Optional[asyncio.AbstractEventLoop], func: Callable[[], Any], label: str) -> Future:
    """Start ``func`` on another thread's ``loop`` without waiting for it.

    Args:
        loop: The target loop.
        func: Callable executed on the loop's thread. An awaitable result is
            awaited there.
        label: Who owns ``loop``, for the error message.

    Returns:
        Future resolved with the result of ``func``.

    Raises:
        RuntimeError: ``loop`` does not exist or is closed.
    """
    if loop is None or loop.is_closed():
        raise RuntimeError(f"{label} is not running")

    async def _call() -> Any:
        result = func()
        if inspect.isawaitable(result):
            result = await result
        return result

    return asyncio.run_coroutine_threadsafe(_call(), loop)


def call_on_loop(loop: Optional[asyncio.AbstractEventLoop],
                 func: Callable[[], Any],
                 timeout: Optional[float],
                 *,
                 in_loop_thread: bool,
                 label: str) -> Any:
    """Run ``func`` on ``loop`` and return its result, awaiting an awaitable one.

    On the loop's own thread there is nothing to post to: posting would wait
    on a loop that only this thread can turn. So ``func`` runs directly, and
    an awaitable result is driven with ``run_until_complete`` while the loop
    is not running yet (``init`` of a loop thread, the main thread before
    ``run_forever``). With the loop already running that is impossible, and
    the caller is told to ``await`` instead of being left blocked.

    Args:
        loop: The target loop.
        func: Callable to run on the loop's thread.
        timeout: Seconds a cross-thread call may take; ``None`` waits
            forever. Unused on the loop's own thread.
        in_loop_thread: Whether the calling thread owns ``loop``.
        label: Who owns ``loop``, for error messages.

    Returns:
        Whatever ``func`` returned, after awaiting it when applicable.

    Raises:
        RuntimeError: ``loop`` is not running (cross-thread), or ``func``
            returned an awaitable on the loop's own thread while it runs.
        TimeoutError: A cross-thread call did not finish in time; it was
            cancelled.
        Exception: Whatever ``func`` raised.
    """
    if in_loop_thread and loop is not None:
        result = func()
        if not inspect.isawaitable(result):
            return result
        if loop.is_running():
            if inspect.iscoroutine(result):
                result.close()
            raise RuntimeError(f"{label}: submit() from inside its own running loop got an awaitable; "
                               "await it instead")
        return loop.run_until_complete(result)
    future = schedule_on_loop(loop, func, label)
    try:
        return future.result(timeout)
    except FutureTimeoutError as err:
        future.cancel()
        raise TimeoutError(f"{label}: call did not finish within {timeout}s") from err


class InlineReactor:
    """A loop owned by the thread that created the reactor, usually not running yet.

    On its own thread, :meth:`submit` runs the call directly, driving the loop
    with ``run_until_complete`` for an awaitable result, and
    :meth:`attach_socket` wraps the socket at once. From any other thread both
    are posted to the loop instead.

    Attributes:
        index: Position of the reactor in its group.
        loop: The owning thread's loop.
        owner_ident: ``threading.get_ident()`` of the thread that created the
            reactor.
    """

    def __init__(self, loop: asyncio.AbstractEventLoop, index: int = 0) -> None:
        self.index = index
        self.loop = loop
        self.owner_ident: Optional[int] = threading.get_ident()

    @property
    def name(self) -> str:
        """Owner of the loop, for log messages."""
        return "inline loop"

    def in_loop_thread(self) -> bool:
        """Whether the calling thread is the one that created this reactor."""
        return threading.get_ident() == self.owner_ident

    def submit(self, func: Callable[[], Any], timeout: Optional[float] = None) -> Any:
        """Run ``func`` on this loop; see :func:`call_on_loop`.

        Args:
            func: Callable to run.
            timeout: Seconds a call from another thread may take; ``None``
                waits forever.

        Returns:
            Whatever ``func`` returned, after awaiting it when applicable.
        """
        return call_on_loop(self.loop, func, timeout, in_loop_thread=self.in_loop_thread(), label=self.name)

    def attach_socket(self,
                      sock: socket.socket,
                      protocol_factory: Callable[[], asyncio.BaseProtocol],
                      ssl_context: Optional[Any] = None) -> None:
        """Wrap an accepted socket in a transport on this loop; see :func:`attach_on_loop`.

        Args:
            sock: Socket returned by ``accept()``, already non-blocking.
            protocol_factory: Builds the protocol serving this connection.
            ssl_context: Server-side ``SSLContext``, or ``None`` for plaintext.
        """
        attach_on_loop(self.loop,
                       sock,
                       protocol_factory,
                       ssl_context,
                       in_loop_thread=self.in_loop_thread(),
                       label=self.name)


class MainLoopReactor(InlineReactor):
    """The worker's main loop as a reactor of its thread model.

    The acceptor runs on this loop too, so an accepted socket is normally
    wrapped directly; an acceptor on another thread would post it instead.
    """

    @property
    def name(self) -> str:
        """Owner of the loop, for log messages."""
        return "main loop"


@dataclass
class ReactorGroup:
    """Reactors that serve one kind of connection, and how an acceptor picks among them.

    Attributes:
        reactors: The reactors, in scheduling order.
        schedule_target: ``SchedulerParams.target`` an acceptor passes to the
            connection selector.
    """

    reactors: Sequence[BaseReactor]
    schedule_target: str = ""


class ResourceKind(Enum):
    """What a resource kept on the model is, which decides when the runtime releases it."""

    # Datagram transports; released with ``close()``.
    ENDPOINT = "endpoint"
    # aiohttp runners; released with ``await cleanup()``.
    RUNNER = "runner"


@dataclass
class ThreadModel:
    """Where one serving process serves its endpoints; built by the runtime after fork.

    Attributes:
        acceptor: The main thread's reactor. Every listen socket is watched
            there, since only that thread accepts.
        io: Reactors serving tRPC TCP connections and UDP sockets.
        handle: Reactors serving HTTP connections. aiohttp runs handlers on the
            connection's loop, so these follow the handle threads.
        handler_pool: ``SharedHandlerPool`` of ``io_handle_type: separate``;
            the tRPC connections hand their requests to it. Filled once the
            handle loops run.
        reuse_port: ``SO_REUSEPORT`` of stream listen sockets and datagram
            sockets. Datagram sockets get it anyway when several reactors bind
            the same address.
        ready_timeout: Seconds a ``submit`` onto a reactor may take; ``None``
            waits forever.
        worker: The serving process, handed to user protocol starters.
        view_factory: ``view_factory(worker, loop)`` builds what a starter
            running on ``loop`` sees as ``worker``, when that is not the
            worker's own loop.
    """

    acceptor: BaseReactor
    io: ReactorGroup
    handle: ReactorGroup
    handler_pool: Optional[Any] = None
    reuse_port: bool = False
    ready_timeout: Optional[float] = None
    worker: Any = None
    view_factory: Optional[Callable[[Any, asyncio.AbstractEventLoop], Any]] = None
    # ``id(reactor)`` -> ``(kind, resource)`` in the order they were kept.
    # Kept on the acceptor thread during listen and released on the reactor's
    # own thread while it stops, never at the same time, so no lock.
    _resources: Dict[int, List[Tuple[ResourceKind, Any]]] = field(default_factory=dict, repr=False)

    @classmethod
    def single(cls,
               loop: asyncio.AbstractEventLoop,
               *,
               worker: Any = None,
               reuse_port: bool = False) -> ThreadModel:
        """Serve everything on ``loop``, owned by the calling thread.

        Args:
            loop: The calling thread's loop, not running yet.
            worker: The serving process, if any.
            reuse_port: ``SO_REUSEPORT`` of stream and datagram sockets.
        """
        reactor = InlineReactor(loop)
        return cls(acceptor=reactor,
                   io=ReactorGroup([reactor]),
                   handle=ReactorGroup([reactor]),
                   reuse_port=reuse_port,
                   worker=worker)

    @classmethod
    def main_loop(cls,
                  loop: asyncio.AbstractEventLoop,
                  *,
                  worker: Any,
                  reuse_port: bool,
                  ready_timeout: Optional[float] = None) -> ThreadModel:
        """Serve everything on a worker's main loop: accept, I/O and handlers on one thread.

        The model of every multi-process role (separate I/O process, rpc
        task, peer worker, task process) and of a merge worker with a single
        serving loop. Must be built on the main thread, which owns ``loop``.

        Args:
            loop: The worker's main loop, not running yet.
            worker: The serving process.
            reuse_port: ``SO_REUSEPORT`` of stream and datagram sockets.
            ready_timeout: Seconds a ``submit`` onto the reactor may take.
        """
        main = MainLoopReactor(loop)
        return cls(acceptor=main,
                   io=ReactorGroup([main]),
                   handle=ReactorGroup([main]),
                   reuse_port=reuse_port,
                   ready_timeout=ready_timeout,
                   worker=worker)

    @property
    def is_single_thread(self) -> bool:
        """Whether every reactor is the acceptor, i.e. the process serves on one loop."""
        return all(reactor is self.acceptor for reactor in (*self.io.reactors, *self.handle.reactors))

    def worker_view(self, loop: asyncio.AbstractEventLoop) -> Any:
        """Return what code running on ``loop`` should see as the worker.

        Args:
            loop: The calling thread's loop.

        Returns:
            The worker itself on its own loop, else ``view_factory(worker, loop)``.
        """
        if self.view_factory is None or self.worker is None or loop is getattr(self.worker, "loop", None):
            return self.worker
        return self.view_factory(self.worker, loop)

    def keep_resource(self, reactor: BaseReactor, resource: Any, kind: ResourceKind) -> None:
        """Keep a resource the runtime releases on ``reactor``'s loop when it stops.

        Args:
            reactor: Reactor whose loop owns the resource.
            resource: A datagram transport or an aiohttp runner.
            kind: What ``resource`` is.
        """
        self._resources.setdefault(id(reactor), []).append((kind, resource))

    def pop_resources(self, reactor: BaseReactor, kind: Optional[ResourceKind] = None) -> List[Any]:
        """Take the resources kept for ``reactor``.

        Args:
            reactor: Reactor they were kept for.
            kind: Only take this kind; ``None`` takes all of them.

        Returns:
            The resources, in the order they were kept.
        """
        kept = self._resources.get(id(reactor), [])
        taken = [resource for item_kind, resource in kept if kind is None or item_kind is kind]
        rest = [(item_kind, resource) for item_kind, resource in kept if kind is not None and item_kind is not kind]
        if rest:
            self._resources[id(reactor)] = rest
        else:
            self._resources.pop(id(reactor), None)
        return taken

    async def release_resources(self, reactor: BaseReactor, kind: Optional[ResourceKind] = None) -> None:
        """Release the resources kept for ``reactor``; await on that reactor's loop.

        aiohttp runners are cleaned up, which closes their connections;
        datagram transports are closed. One failure does not keep the rest open.

        Args:
            reactor: Reactor they were kept for.
            kind: Only release this kind; ``None`` releases all of them.
        """
        for resource in self.pop_resources(reactor, kind):
            try:
                cleanup = getattr(resource, "cleanup", None)
                if cleanup is not None:
                    await cleanup()
                else:
                    resource.close()
            except Exception as err:  # pylint: disable=broad-except
                logger.error("[thread model] failed to release %r: %s", resource, repr(err))


# ``(pid, model)`` of the installed model. A module global rather than a
# ``ContextVar``: loop threads do not inherit the main thread's context.
_installed: Optional[Tuple[int, ThreadModel]] = None


def current_thread_model() -> ThreadModel:
    """Return this process's thread model.

    Returns:
        The installed model; with none installed, a new single-reactor model
        on the calling thread's loop. Not cached, since callers may run on
        different threads.

    Raises:
        RuntimeError: The installed model was installed in another process,
            i.e. before fork.
    """
    installed = _installed
    if installed is None:
        return ThreadModel.single(asyncio.get_event_loop())
    pid, model = installed
    if pid != os.getpid():
        raise RuntimeError("thread model installed before fork")
    return model


def install_thread_model(model: Optional[ThreadModel]) -> None:
    """Install this process's thread model; ``None`` uninstalls it.

    Must be called after fork, before any loop thread starts.

    Args:
        model: The model, or ``None``.
    """
    global _installed  # pylint: disable=global-statement
    _installed = None if model is None else (os.getpid(), model)


@contextmanager
def use_thread_model(model: ThreadModel) -> Iterator[ThreadModel]:
    """Install ``model`` for the ``with`` block, then restore the previous one.

    Args:
        model: The model to install.

    Yields:
        ``model``.
    """
    global _installed  # pylint: disable=global-statement
    previous = _installed
    install_thread_model(model)
    try:
        yield model
    finally:
        _installed = previous
