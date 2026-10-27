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
"""

from __future__ import annotations

import asyncio
import inspect
import os
import socket
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

# BaseReactor
class Reactor(Protocol):
    """One event loop a server transport can serve on; owned by the runtime.

    ``submit`` and ``attach_socket`` are called from the acceptor thread, the
    thread ``listen`` runs on.

    Attributes:
        index: Position of the reactor in its group, read by the scheduler.
        loop: Its event loop; ``None`` until a loop thread created it.
    """

    index: int
    loop: Optional[asyncio.AbstractEventLoop]

    def submit(self, func: Callable[[], Any], timeout: Optional[float] = None) -> Any:
        """Run ``func`` on the reactor's loop, await an awaitable result, and return it."""

    def attach_socket(self,
                      sock: socket.socket,
                      protocol_factory: Callable[[], asyncio.BaseProtocol],
                      ssl_context: Optional[Any] = None) -> None:
        """Serve an accepted socket on the reactor's loop, without blocking."""


class InlineReactor:
    """A loop owned by the calling thread and not running yet.

    Everything runs on the calling thread: :meth:`submit` drives the loop with
    ``run_until_complete``, and :meth:`attach_socket` assumes the acceptor
    already runs on this loop.

    Attributes:
        index: Position of the reactor in its group.
        loop: The calling thread's loop.
    """

    def __init__(self, loop: asyncio.AbstractEventLoop, index: int = 0) -> None:
        self.index = index
        self.loop = loop

    def submit(self, func: Callable[[], Any], timeout: Optional[float] = None) -> Any:
        """Run ``func`` here; an awaitable result is driven to completion on the loop.

        Args:
            func: Callable to run.
            timeout: Unused: the call runs on this thread.

        Returns:
            Whatever ``func`` returned, after awaiting it when applicable.
        """
        del timeout
        result = func()
        if inspect.isawaitable(result):
            result = self.loop.run_until_complete(result)
        return result

    def attach_socket(self,
                      sock: socket.socket,
                      protocol_factory: Callable[[], asyncio.BaseProtocol],
                      ssl_context: Optional[Any] = None) -> None:
        """Wrap an accepted socket in a transport on this loop; runs on its thread.

        Args:
            sock: Socket returned by ``accept()``, already non-blocking.
            protocol_factory: Builds the protocol serving this connection.
            ssl_context: Server-side ``SSLContext``, or ``None`` for plaintext.
        """

        def _on_attached(task: asyncio.Task) -> None:
            if task.cancelled() or task.exception() is not None:
                sock.close()

        task = self.loop.create_task(self.loop.connect_accepted_socket(protocol_factory, sock, ssl=ssl_context))
        task.add_done_callback(_on_attached)


@dataclass
class ReactorGroup:
    """Reactors that serve one kind of connection, and how an acceptor picks among them.

    Attributes:
        reactors: The reactors, in scheduling order.
        schedule_target: ``SchedulerParams.target`` an acceptor passes to the
            connection selector.
    """

    reactors: Sequence[Reactor]
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

    acceptor: Reactor
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

    def keep_resource(self, reactor: Reactor, resource: Any, kind: ResourceKind) -> None:
        """Keep a resource the runtime releases on ``reactor``'s loop when it stops.

        Args:
            reactor: Reactor whose loop owns the resource.
            resource: A datagram transport or an aiohttp runner.
            kind: What ``resource`` is.
        """
        self._resources.setdefault(id(reactor), []).append((kind, resource))

    def pop_resources(self, reactor: Reactor, kind: Optional[ResourceKind] = None) -> List[Any]:
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

    async def release_resources(self, reactor: Reactor, kind: Optional[ResourceKind] = None) -> None:
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
