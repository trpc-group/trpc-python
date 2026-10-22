# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""HTTP server transport: serve an aiohttp application on the endpoint.

:class:`HttpServerTransport` records the options before fork and serves the
endpoint after it. The aiohttp application itself is built by the runtime,
which owns the services and the routes, and handed over as
:meth:`HttpServerTransport.set_runner_factory`. The aiohttp-to-tRPC request bridge
lives in :mod:`~trpc_sdk.http._message_handle`.
"""

from __future__ import annotations

from typing import Awaitable
from typing import Callable
from typing import Dict
from typing import Optional

from aiohttp import web

from trpc_sdk.abc import BasePluginFactory
from trpc_sdk.abc import BaseServerTransportOption
from trpc_sdk.abc import PluginInitPos
from trpc_sdk.abc import PluginType
from trpc_sdk.plugin import register_plugin
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

    def __init__(self) -> None:
        super().__init__()
        # Builds and starts one runner; awaited on the loop that will serve it.
        self._runner_factory: Optional[Callable[[], Awaitable[web.AppRunner]]] = None

    def bind(self, opts: BaseServerTransportOption) -> None:
        """Record the options and set the network to ``http``. Called before fork.

        The YAML entry of an HTTP service says ``network: tcp``. The rewrite
        is what makes the runtime serve the address as HTTP.

        Args:
            opts: Listen options (address, handler, timeout).
        """
        super().bind(opts)
        opts.set_network(HTTP)

    def set_runner_factory(self, factory: Callable[[], Awaitable[web.AppRunner]]) -> None:
        """Set how a runner is built; called by the runtime after fork, before :meth:`listen`.

        The application is built from the worker's services and routes, which
        only the runtime has, so it cannot be recorded at :meth:`bind` time.

        Args:
            factory: Coroutine factory returning a started ``AppRunner``.
        """
        self._runner_factory = factory

    def listen(self) -> None:
        """Build one runner per ``handle`` reactor, on that reactor's loop, and serve them.

        Each runner is kept on the thread model, to be cleaned up on its own
        loop when it stops; cleaning a runner up closes its connections.

        Raises:
            ValueError: No runner factory was set.
        """
        # Local import: trpc_sdk.runtime imports this package at module level.
        from trpc_sdk.runtime import ResourceKind
        from trpc_sdk.runtime import current_thread_model

        if self._runner_factory is None:
            raise ValueError(f"HTTP endpoint {self.opts.address} needs a runner factory")
        model = current_thread_model()
        group = model.handle
        runners: Dict[int, web.AppRunner] = {}
        for reactor in group.reactors:
            runner = reactor.submit(self._runner_factory, timeout=model.ready_timeout)
            model.keep_resource(reactor, runner, ResourceKind.RUNNER)
            runners[id(reactor)] = runner
        self._serve_stream(model, group, lambda reactor: runners[id(reactor)].server,
                           ssl_context_from_opts(self.opts))


@register_plugin(PluginType.SERVER_TRANSPORT, HTTP, init_pos=PluginInitPos.WORKER)
class HttpServerTransportFactory(BasePluginFactory):
    """Plugin factory that produces :class:`HttpServerTransport`."""

    def create(self) -> HttpServerTransport:
        """Create a new :class:`HttpServerTransport`.

        Returns:
            A fresh transport instance.
        """
        return HttpServerTransport()
