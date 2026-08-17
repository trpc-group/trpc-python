# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Executor child process: the worker of ``multi-listener`` mode.

In ``multi-listener`` mode there is no dispatcher. Every executor binds the
same public addresses with ``SO_REUSEPORT`` and lets the kernel spread
incoming connections across them, so a request is accepted, decoded and
handled inside a single process. That removes the IPC hop of ``dispatcher``
mode at the cost of one listen socket per process per endpoint.

An executor is a :class:`~trpc.server._worker.BaseWorker`, so it keeps the
task connections, the inter-worker message pipe and the timers; only the front
end differs. Each configured endpoint is served by the protocol its network
type asks for — TCP, UDP or an aiohttp application for HTTP — or by a handler
the user registered for a custom protocol.
"""

from __future__ import annotations

import sys
from functools import partial
from typing import Callable
from typing import Dict

from aiohttp import hdrs
from aiohttp import web

from trpc_sdk.config import TrpcConfig
from trpc_sdk.http import HttpMessageHandle
from trpc_sdk.log import logger
from trpc_sdk.transport import ExecutorUDPConnection
from trpc_sdk.transport import TCPConnection
from trpc_sdk.transport import new_server_socket
from ._constants import NETWORK_HTTP
from ._constants import NETWORK_TCP
from ._constants import NETWORK_UDP
from ._utils import add_user_route
from ._utils import get_https_certs
from ._utils import reg_static_file
from ._worker import BaseWorker
from ._worker import WorkerInfo


class ExecutorProcess(BaseWorker):
    """Worker of ``multi-listener`` mode: it listens for itself.

    There is no dispatcher here. Every executor binds the same public
    addresses with ``SO_REUSEPORT`` and the kernel spreads incoming
    connections across them, so accept, decode and handler execution all
    happen inside this one process. Compared with
    :class:`~trpc.server._worker.WorkerProcess`, that saves an IPC hop per
    request, at the cost of one listen socket per process per endpoint.

    Everything behind the front end — task connections, the inter-worker
    message pipe, timers — is inherited unchanged from
    :class:`~trpc.server._worker.BaseWorker`.
    """

    def __init__(self, worker: WorkerInfo, engine):
        """Copy the per-endpoint tables out of the engine; nothing is bound yet.

        Runs in the master before fork. Sockets are created later, in
        :meth:`start_worker_process`.

        Args:
            worker: Pre-fork record with this worker's id and IPC pipe ends.
            engine: Master engine, inherited through fork. Its
                ``process_info`` lists are all index-aligned with
                ``addresses``.
        """
        super().__init__(worker, engine)
        # Network type of each listen address ("tcp", "udp", "http", ...);
        # picks the protocol in ``start_worker_process``.
        self.networks = engine.process_info.networks
        # Request handler for each listen address.
        self.handlers = engine.process_info.handlers
        # Frame checker/parser for each listen address.
        self.checkers = engine.process_info.checkers
        # Idle/request timeout (ms) for each listen address.
        self.timeouts = engine.process_info.timeouts
        # Service adapter for each listen address; carries the protocol name
        # and service name.
        self.services = engine.process_info.services
        # Bridge between aiohttp requests and the tRPC handler, per address
        # index. Only populated for HTTP endpoints.
        self._http_msg_handles = {}
        # Overrides the ``worker-{id}`` name set by BaseWorker.
        self.name = f"merge_worker-{worker.id}"
        # Protocol name -> starter, registered by the user for custom
        # protocols. Takes precedence over the built-in tcp/udp/http
        # dispatch; the callable receives the address index and is expected
        # to bind its own socket.
        self.user_protocol_handles: Dict[str, Callable[[int], None]] = {}

    def start_worker_process(self) -> None:
        """Bind every configured endpoint in this process.

        Called by :meth:`~trpc.server._worker.BaseWorker._prepare_to_serve`
        once plugins and task connections are up. For each listen address, in
        priority order:

        1. A protocol registered in ``user_protocol_handles`` wins, so a
           custom protocol can replace the built-in ones on that port.
        2. Otherwise the network type selects TCP, UDP or HTTP.
        3. An unknown network type is a configuration error with no sane
           fallback, so the process exits rather than starting up
           half-listening.
        """
        is_base64 = TrpcConfig.server_config().encode_base64
        logger.info("[worker] ExecutorProcess start_worker_process()")
        for i, _ in enumerate(self.addresses):
            service_adapter = self.services[i]
            if service_adapter.opt.protocol in self.user_protocol_handles:
                self.user_protocol_handles[service_adapter.opt.protocol](i)
            elif self.networks[i].find(NETWORK_TCP) != -1:
                self.start_executor_tcp(i)
            elif self.networks[i].find(NETWORK_UDP) != -1:
                self.start_executor_udp(i)
            elif self.networks[i].find(NETWORK_HTTP) != -1:
                msg_handle = HttpMessageHandle(i, self, is_base64=is_base64)
                # Replaced, not extended: the entry is read back by the very
                # next call and nothing looks it up afterwards.
                self._http_msg_handles = {i: msg_handle}
                self.start_executor_http(i)
            else:
                sys.exit()

    def start_executor_tcp(self, index: int) -> None:
        """Listen for tRPC-over-TCP on one address.

        The socket is created with ``SO_REUSEPORT`` and the server is started
        with ``reuse_port=True``, which is what lets every executor bind the
        same address and have the kernel balance connections between them.

        Args:
            index: Position of the address in ``addresses``; the same index
                selects the checker, handler and timeout to use.
        """
        sock = new_server_socket(self.addresses[index], self.socket_types[index], True)
        handle = partial(TCPConnection,
                         app=self,
                         checker=self.checkers[index],
                         handle=self.handlers[index],
                         timeout=self.timeouts[index])
        ss = self.loop.create_server(handle, sock=sock, reuse_port=True, backlog=1024)
        self.loop.run_until_complete(ss)

    def start_executor_udp(self, index: int) -> None:
        """Listen for tRPC-over-UDP on one address.

        Same ``SO_REUSEPORT`` sharing as the TCP path; the trailing
        ``stream=False`` is what makes ``new_server_socket`` hand back a
        ``SOCK_DGRAM`` socket.

        Args:
            index: Position of the address in ``addresses``; the same index
                selects the checker, handler and timeout to use.
        """
        sock = new_server_socket(self.addresses[index], self.socket_types[index], True, False)
        handle = partial(ExecutorUDPConnection,
                         app=self,
                         checker=self.checkers[index],
                         handle=self.handlers[index],
                         timeout=self.timeouts[index])
        ss = self.loop.create_datagram_endpoint(protocol_factory=handle, sock=sock)

        self.loop.run_until_complete(ss)

    def start_executor_http(self, index: int) -> None:
        """Serve one HTTP endpoint through an aiohttp application.

        Builds an aiohttp application for this address and serves it on a
        ``reuse_port`` site, so HTTP endpoints are shared across executors the
        same way TCP and UDP ones are. Routes are added most specific first:
        user routes, then static files, then a catch-all that forwards
        anything left to the tRPC handler. Registering the catch-all earlier
        would shadow the other two.

        Args:
            index: Position of the address in ``addresses``. Its entry in
                ``_http_msg_handles`` must already exist, and its
                ``tran_opts`` entry decides whether the site is HTTPS.
        """
        service_adapter = self.services[index]
        service_name = service_adapter.opt.service_name
        app = web.Application(client_max_size=TrpcConfig.server_config().max_rbuf_size)
        msg_handle: HttpMessageHandle = self._http_msg_handles[index]

        add_user_route(app, service_name, msg_handle)

        reg_static_file(app)

        resource = app.router.add_resource(r'/{name:.*}')
        resource.add_route(hdrs.METH_ANY, msg_handle.handle_http_request)

        # None unless the transport options carry a cert/key pair.
        ssl_context = get_https_certs(self.tran_opts[index])

        runner = web.AppRunner(app)
        self.loop.run_until_complete(runner.setup())
        site = web.TCPSite(runner,
                           host=self.addresses[index][0],
                           port=self.addresses[index][1],
                           reuse_port=True,
                           backlog=1024,
                           ssl_context=ssl_context)
        self.loop.run_until_complete(site.start())
