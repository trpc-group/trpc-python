# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""HTTP server transport: register an HTTP listen endpoint on the engine.

:class:`HttpServerTransport` only tells the engine to add an HTTP
endpoint. The actual accept loop and aiohttp app run inside a worker.
The aiohttp-to-tRPC request bridge lives in
:mod:`~trpc_sdk.http._message_handle`.
"""

from __future__ import annotations

from trpc_sdk.abc import BasePluginFactory
from trpc_sdk.abc import BaseServerTransport
from trpc_sdk.abc import BaseServerTransportOption
from trpc_sdk.abc import BaseTrpcServerEngine
from trpc_sdk.abc import PluginInitPos
from trpc_sdk.abc import PluginType
from trpc_sdk.common import get_or_create_event_loop
from trpc_sdk.plugin import register_plugin
from ._constants import HTTP


class HttpServerTransport(BaseServerTransport):
    """Server transport that registers an HTTP listen endpoint.

    ``listen_and_serve`` does not bind a socket itself. It tells the
    engine to add an HTTP endpoint so a worker can later start aiohttp
    on that address.
    """

    def __init__(self) -> None:
        super().__init__()
        self.opts = None
        self.loop = None
        self.server_engine = None

    def listen_and_serve(self, opts: BaseServerTransportOption, server_engine: BaseTrpcServerEngine) -> None:
        """Register this HTTP service on ``server_engine``.

        Args:
            opts: Listen options (address, port, handle, timeout).
            server_engine: Engine that will fork workers for this endpoint.
        """
        self.opts = opts
        self.server_engine = server_engine

        self.loop = get_or_create_event_loop()

        # Registered as ``http`` rather than ``opts.network``: the YAML entry
        # of an HTTP service says ``network: tcp``, and the worker would start
        # a plain TCP front end for it.
        self.server_engine.add_listen_endpoint(self.opts, HTTP)

    def close(self) -> None:
        """Close the engine that owns this transport."""
        self.server_engine.close()


@register_plugin(PluginType.SERVER_TRANSPORT, HTTP, init_pos=PluginInitPos.WORKER)
class HttpServerTransportFactory(BasePluginFactory):
    """Plugin factory that produces :class:`HttpServerTransport`."""

    def create(self) -> HttpServerTransport:
        """Create a new :class:`HttpServerTransport`.

        Returns:
            A fresh transport instance.
        """
        return HttpServerTransport()
