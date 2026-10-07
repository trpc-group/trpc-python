# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Worker of ``merge`` mode: one process accepts, decodes and runs handlers.

There is no dispatcher and no worker child process here. This worker *is* the
server process: it binds the public addresses, accepts on its main thread, and
serves each connection on one of ``worker_thread_num`` event-loop threads. A
request therefore never crosses a process boundary, which is what removes the
IPC hop that ``separate`` mode pays.

The listeners are the same three this framework has always served — tRPC over
TCP, tRPC over UDP, and HTTP through aiohttp — plus whatever a user registered
with :func:`~trpc_sdk.server.register_protocol_handle`. Which listener serves
an address is decided by :func:`~trpc_sdk.server._listeners.classify_address`,
the same function ``separate`` mode uses. What changed is where they run:

* TCP and HTTP go through :class:`~trpc_sdk.server._acceptor.Acceptor`: one bind,
  accept on the main thread, ``connect_accepted_socket`` onto a loop thread.
* UDP has nothing to accept, so each loop thread binds its own ``SO_REUSEPORT``
  socket on the same address and the kernel spreads datagrams over them. That
  spreading is a Linux behaviour; BSD-derived kernels (macOS) deliver to the
  most recently bound socket instead, which makes UDP effectively single
  threaded there. It still serves correctly, just without the parallelism.

``worker_thread_num: 1`` skips all of that: no thread is created and every
listener is attached straight to the main loop, exactly as the multi-listener
executor did. This is both the cheapest configuration and the one that proves
the threaded paths did not change behaviour.

The GIL is the trade-off to understand before choosing this mode. Threads
overlap on I/O waits and on C extensions that release the GIL (protobuf, numpy,
inference runtimes), but Python bytecode stays serialized. A CPU-bound service
wants ``separate`` mode, where handlers run in separate rpc task processes.
"""

from __future__ import annotations

import asyncio
import sys
from functools import partial
from typing import Any
from typing import Dict
from typing import List
from typing import Optional

from aiohttp import web

from trpc_sdk.abc import ProcessType
from trpc_sdk.http import HttpMessageHandle
from trpc_sdk.log import logger
from trpc_sdk.transport import TCPConnection
from trpc_sdk.transport import create_server_socket
from ._acceptor import Acceptor
from ._constants import LISTEN_BACKLOG
from ._constants import AddressKind
from ._listeners import classify_address
from ._listeners import create_http_msg_handle
from ._listeners import create_udp_socket
from ._listeners import setup_http_runner
from ._listeners import start_http_site
from ._listeners import start_udp
from ._listeners import start_user_protocol
from ._listeners import stop_http_sites
from ._listeners import udp_protocol_factory
from ._loop_thread import LoopThread
from ._task import worker_producer_name
from ._types import AcceptTarget
from ._utils import get_https_certs
from ._worker import BaseWorker


class MergeWorker(BaseWorker):
    """The server process in ``merge`` mode: it listens and it handles.

    Not a ``Process`` subclass, unlike the executor it replaces: this object
    describes the process it runs in, so there is nobody to fork it.

    Attributes:
        thread_num: Event-loop threads used to serve connections. ``1`` means
            the main loop serves them and no thread is created.
        loop_threads: The threads themselves; empty when ``thread_num`` is 1.
        acceptors: One per stream endpoint that hands connections to threads.
    """

    def __init__(self, engine, name: str = "merge_worker") -> None:
        """Copy the endpoint tables out of the engine; nothing is bound yet.

        Sockets, threads and the event loop are all created later, in
        :meth:`~trpc_sdk.server._worker.BaseWorker.run`. Nothing here may outlive a
        fork boundary either, since a merge worker forks its tasks after being
        constructed and before its loop exists.

        Args:
            engine: Engine holding ``process_info`` (the per-endpoint tables
                filled during ``Service.run``) and ``opts``.
            name: Process name shown in logs and process listings.
        """
        super().__init__(name, ProcessType.WORKER)
        self._bind_process_info(engine)

        # A worker always serves on at least one loop; 0 or a negative value in
        # YAML means "no threads", which is the main-loop path.
        self.thread_num: int = max(1, engine.opts.worker_thread_num)
        self.loop_threads: List[LoopThread] = []
        self.acceptors: List[Acceptor] = []

        # Bridge between aiohttp requests and the tRPC handler, one per HTTP
        # address index. Shared by every thread serving that address: it holds
        # no loop state, only the address index and this worker.
        self._http_msg_handles: Dict[int, HttpMessageHandle] = {}
        # aiohttp runners kept alive for cleanup, one per (address, loop).
        self._http_runners: List[web.AppRunner] = []
        # Listen servers and aiohttp sites bound on the main loop when
        # ``thread_num`` is 1; closed by ``_stop_accepting``.
        self._servers: List[asyncio.AbstractServer] = []
        self._http_sites: List[web.BaseSite] = []

    def start_worker_process(self) -> None:
        """Start the loop threads, then bind every configured endpoint.

        Called by :meth:`~trpc_sdk.server._worker.BaseWorker._prepare_to_serve`
        once plugins and task connections are up. Threads come first because a
        listener needs its targets to exist before it can route to them.

        Each listen address goes to the listener
        :func:`~trpc_sdk.server._listeners.classify_address` picks for it: a
        registered user protocol first, so a custom protocol can replace the
        built-in ones on that port, then TCP, UDP or HTTP by network type. An
        unknown network type is a configuration error with no sane fallback,
        so the process exits with status 1 rather than starting up
        half-listening.
        """
        logger.info("[merge worker] MergeWorker start_worker_process(), threads: %s", self.thread_num)

        self._start_loop_threads()

        for i, _ in enumerate(self.addresses):
            kind = classify_address(self, i)
            if kind is AddressKind.USER:
                start_user_protocol(self, i)
            elif kind is AddressKind.TCP:
                self._start_tcp(i)
            elif kind is AddressKind.UDP:
                self._start_udp(i)
            elif kind is AddressKind.HTTP:
                self._http_msg_handles[i] = create_http_msg_handle(self, i)
                self._start_http(i)
            else:
                logger.error("[merge worker] unsupported network %r for address %s", self.networks[i],
                             self.addresses[i])
                sys.exit(1)

    def _task_producer_names(self) -> List[str]:
        """Give every serving loop its own producer identity, and its own pipes.

        An asyncio transport may only be written from the loop that created
        it, so N serving loops need N socketpairs to each task rather than one
        shared pipe. Sizing this from ``thread_num`` works before any thread
        exists, which matters because the pipes are created before the fork
        and the threads only after it.

        This is the method that sizes the mesh, so it is the one to override:
        ``_owned_producer_names`` defaults to it, and every identity here does
        belong to this process. Overriding only the ownership side would leave
        the mesh with a single pipe and every thread but the first with none.
        """
        return [worker_producer_name(index) for index in range(self.thread_num)]

    def _connect_tasks(self) -> None:
        """Connect to every task once per serving loop.

        Starts the threads first: each has to open its own connections, on its
        own loop, and it can only do that once its loop is turning. The
        single-loop configuration keeps the base behaviour.
        """
        self._start_loop_threads()
        if not self.loop_threads:
            super()._connect_tasks()
            return

        for thread in self.loop_threads:
            thread.submit(partial(self._connect_tasks_for_thread, thread))

    async def _connect_tasks_for_thread(self, thread: LoopThread) -> None:
        """Open one serving thread's task connections; runs on that thread.

        Awaited by :meth:`LoopThread.submit` on the thread's own loop, which is
        already running by then, so the connections are created rather than
        driven to completion from outside.

        Args:
            thread: The loop thread whose producer pipes are being attached.
        """
        if self.task_ipc_mesh is None:
            # No task was ever declared, or this worker was not started
            # through ``_spawn_children``.
            return
        pipes = self.task_ipc_mesh.producer_pipes(worker_producer_name(thread.index))
        await self._connect_tasks_on(thread.loop, pipes)

    def _start_loop_threads(self) -> None:
        """Create and start the serving threads, unless there is only one loop.

        Leaves ``loop_threads`` empty for ``thread_num == 1``: that is the
        signal every ``_start_*`` method reads to attach itself to the main
        loop instead of going through an acceptor.

        Idempotent, because task setup needs the threads before
        :meth:`start_worker_process` does.
        """
        if self.thread_num <= 1 or self.loop_threads:
            return
        for index in range(self.thread_num):
            thread = LoopThread(index)
            thread.start()
            self.loop_threads.append(thread)

    def _start_acceptor(self,
                        index: int,
                        targets: List[AcceptTarget],
                        ssl_context: Optional[Any] = None) -> None:
        """Bind one stream address and start handing its connections to threads.

        Args:
            index: Position of the address in ``addresses``.
            targets: Loop threads and the protocol factory each should use.
            ssl_context: Server-side ``SSLContext``, or ``None`` for plaintext.
        """
        sock = create_server_socket(self.addresses[index], self.socket_types[index], True)
        acceptor = Acceptor(sock=sock,
                            targets=targets,
                            loop=self.loop,
                            address=self.addresses[index],
                            ssl_context=ssl_context)
        acceptor.start()
        self.acceptors.append(acceptor)

    def _start_tcp(self, index: int) -> None:
        """Listen for tRPC-over-TCP on one address.

        Every thread serves this endpoint with the same protocol factory:
        :class:`TCPConnection` takes its loop from whichever one is running
        when it is constructed, so one factory is safe to share.

        Args:
            index: Position of the address in ``addresses``; the same index
                selects the checker, handler and timeout to use.
        """
        handle = partial(TCPConnection,
                         app=self,
                         checker=self.checkers[index],
                         handle=self.handlers[index],
                         timeout=self.timeouts[index])

        if not self.loop_threads:
            sock = create_server_socket(self.addresses[index], self.socket_types[index], True)
            server = self.loop.create_server(handle, sock=sock, reuse_port=True, backlog=LISTEN_BACKLOG)
            self._servers.append(self.loop.run_until_complete(server))
            return

        self._start_acceptor(index, [AcceptTarget(thread, handle) for thread in self.loop_threads])

    def _start_udp(self, index: int) -> None:
        """Listen for tRPC-over-UDP on one address, once per serving loop.

        Datagrams cannot be handed over the way accepted sockets can, so each
        loop binds its own ``SO_REUSEPORT`` socket on the same address and lets
        the kernel do the spreading. See the module docstring for what that
        means outside Linux.

        Args:
            index: Position of the address in ``addresses``; the same index
                selects the checker, handler and timeout to use.
        """
        if not self.loop_threads:
            start_udp(self, index, self.loop)
            return

        handle = udp_protocol_factory(self, index)
        for thread in self.loop_threads:
            sock = create_udp_socket(self, index)
            thread.submit(partial(thread.loop.create_datagram_endpoint, protocol_factory=handle, sock=sock))

    def _start_http(self, index: int) -> None:
        """Serve one HTTP endpoint, one aiohttp application per serving loop.

        Unlike the tRPC protocols, aiohttp cannot share a listener across
        loops: ``AppRunner.setup()`` builds a ``web.Server`` bound to the loop
        running at that moment, and every request handler it produces inherits
        that loop. So each thread gets its own application, runner and protocol
        factory, all built on the thread that will serve them.

        The single-loop path keeps using ``web.TCPSite``, which binds for
        itself and needs no acceptor.

        Args:
            index: Position of the address in ``addresses``. Its entry in
                ``_http_msg_handles`` must already exist, and its ``tran_opts``
                entry decides whether the endpoint is HTTPS.
        """
        if not self.loop_threads:
            runner, site = start_http_site(self, index, self._http_msg_handles[index], self.loop)
            self._http_runners.append(runner)
            self._http_sites.append(site)
            return

        # None unless the transport options carry a cert/key pair.
        ssl_context = get_https_certs(self.tran_opts[index])
        targets = []
        for thread in self.loop_threads:
            runner = thread.submit(partial(self._setup_http_runner, index))
            targets.append(AcceptTarget(thread, runner.server))
        self._start_acceptor(index, targets, ssl_context=ssl_context)

    async def _setup_http_runner(self, index: int) -> web.AppRunner:
        """Build an aiohttp runner on the calling loop thread and keep it alive.

        Args:
            index: Position of the address in ``addresses``.

        Returns:
            A runner whose ``server`` attribute is usable as a protocol
            factory.
        """
        runner = await setup_http_runner(self, index, self._http_msg_handles[index])
        self._http_runners.append(runner)
        return runner

    def _stop_accepting(self) -> None:
        """Close every listen socket; open connections keep being served.

        Acceptors for the threaded path, the asyncio servers and aiohttp
        sites for the single-loop one. UDP endpoints stay open: there is no
        connection to refuse, and in-flight datagrams are answered on them.
        """
        for acceptor in self.acceptors:
            acceptor.close()
        self.acceptors.clear()
        for server in self._servers:
            server.close()
        self._servers.clear()
        stop_http_sites(self.loop, self._http_sites)
        self._http_sites.clear()

    def _on_graceful_exit(self) -> None:
        """Stop accepting, then shut the serving threads down.

        Order matters: an acceptor closed after its targets would hand sockets
        to loops that are no longer turning, and those connections would hang
        until the process died rather than being refused.
        """
        for acceptor in self.acceptors:
            acceptor.close()
        self.acceptors.clear()

        for thread in self.loop_threads:
            thread.stop()
        self.loop_threads.clear()
