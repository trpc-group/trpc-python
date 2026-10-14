# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""HTTP server transport: serve an aiohttp application on the endpoint.

:class:`HttpServerTransport` records the options before fork and serves the
endpoint after it. The aiohttp application itself is built by the runtime,
which owns the services and the routes, and handed over as
``ListenOption.http_runner_factory``. The aiohttp-to-tRPC request bridge
lives in :mod:`~trpc_sdk.http._message_handle`.
"""

from __future__ import annotations

from typing import Optional

from aiohttp import web

from trpc_sdk.abc import BasePluginFactory
from trpc_sdk.abc import BaseServerTransportOption
from trpc_sdk.abc import PluginInitPos
from trpc_sdk.abc import PluginType
from trpc_sdk.plugin import register_plugin
from trpc_sdk.transport import AcceptTarget
from trpc_sdk.transport import ListenOption
from trpc_sdk.transport import TrpcServerTransport
from trpc_sdk.transport import ssl_context_from_opts
from ._constants import HTTP


class HttpServerTransport(TrpcServerTransport):
    """Server transport that serves one HTTP endpoint with aiohttp.

    aiohttp cannot share a listener across loops: ``AppRunner.setup()``
    builds a ``web.Server`` bound to the loop running at that moment, and
    every request handler it produces inherits that loop. So every serving
    loop gets its own runner, built on that loop.
    """

    def bind(self, opts: BaseServerTransportOption) -> None:
        """Record the options and set the network to ``http``. Called before fork.

        The YAML entry of an HTTP service says ``network: tcp``. The rewrite
        is what makes the runtime serve the address as HTTP.

        Args:
            opts: Listen options (address, handler, timeout).
        """
        super().bind(opts)
        opts.set_network(HTTP)

    def listen(self, opt: Optional[ListenOption] = None) -> None:
        """Serve the aiohttp application on the loops in ``opt``.

        With no serving loops the runner is served on ``opt.loop`` through a
        ``web.SockSite`` on our own listen socket. Otherwise each serving loop
        builds its own runner and an acceptor on ``opt.loop`` hands every
        connection to one of them. Each runner is kept for
        :meth:`pop_loop_resources`, keyed by the loop to clean it up on.

        Args:
            opt: Needs ``http_runner_factory``.

        Raises:
            ValueError: ``opt`` carries no ``http_runner_factory``.
        """
        if opt is None or opt.http_runner_factory is None:
            raise ValueError(f"HTTP endpoint {self.opts.address} needs an http_runner_factory")
        ssl_context = opt.ssl_context or ssl_context_from_opts(self.opts)
        if not opt.serving_loops:
            runner = opt.loop.run_until_complete(opt.http_runner_factory())
            self.keep_loop_resource(None, runner)
            site = web.SockSite(runner,
                                self.create_socket(opt.reuse_port),
                                ssl_context=ssl_context,
                                backlog=self.opts.backlog)
            opt.loop.run_until_complete(site.start())
            self.keep_site(opt.loop, site)
            return

        targets = []
        for target in opt.serving_loops:
            runner = target.submit(opt.http_runner_factory, timeout=opt.ready_timeout)
            self.keep_loop_resource(target.index, runner)
            targets.append(AcceptTarget(target, runner.server))
        self.serve_via_acceptor(opt, targets, ssl_context=ssl_context)


@register_plugin(PluginType.SERVER_TRANSPORT, HTTP, init_pos=PluginInitPos.WORKER)
class HttpServerTransportFactory(BasePluginFactory):
    """Plugin factory that produces :class:`HttpServerTransport`."""

    def create(self) -> HttpServerTransport:
        """Create a new :class:`HttpServerTransport`.

        Returns:
            A fresh transport instance.
        """
        return HttpServerTransport()
