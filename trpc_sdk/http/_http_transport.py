# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""HTTP server transport: serve an aiohttp application on the endpoint.

:class:`HttpServerTransport` records the options before fork and serves the
endpoint after it. It builds the aiohttp application itself, from its own
options and the process-wide routes of :mod:`~trpc_sdk.http._routes`, and
bridges every request to tRPC: catch-all paths go through
:meth:`HttpServerTransport._handle_rpc`, ``@route`` paths through
:meth:`HttpServerTransport._handle_route`.
"""

from __future__ import annotations

from functools import partial
from typing import Dict
from typing import Tuple

from aiohttp import hdrs
from aiohttp import web
from aiohttp.web_request import Request
from aiohttp.web_response import Response

from trpc_sdk.abc import BasePluginFactory
from trpc_sdk.abc import PluginInitPos
from trpc_sdk.abc import PluginType
from trpc_sdk.abc import TrpcContext
from trpc_sdk.abc import get_message
from trpc_sdk.config import TrpcConfig
from trpc_sdk.plugin import register_plugin
from trpc_sdk.transport import TrpcServerTransport
from trpc_sdk.transport import ssl_context_from_opts
from trpc_sdk.utils import now_ms
from trpc_sdk.utils import set_addr_to_ctx_from_transport
from ._codec import ServerHttpCodec
from ._constants import HTTP
from ._constants import LOCAL_ADDR
from ._constants import TRANS_INFO_BASE64_KEY
from ._constants import USER_ROUTE_KEY
from ._context import pop_http_rsp_header
from ._routes import HttpRoute
from ._routes import RegisteredRoutes
from ._routes import StaticFileRouters
from ._routes import get_user_http_decode_func


class HttpServerTransport(TrpcServerTransport):
    """Server transport that serves one HTTP endpoint with aiohttp.

    aiohttp cannot share a listener across loops: ``AppRunner.setup()``
    builds a ``web.Server`` bound to the loop running at that moment, and
    every request handler it produces inherits that loop. So every serving
    loop gets its own runner, built on that loop. The request handlers hold
    no loop state, so every runner of the endpoint shares them.
    """

    def __init__(self) -> None:
        super().__init__()
        # ``server.encode_base64``, read by :meth:`listen`, after fork.
        self._is_base64: bool = False

    @property
    def ipc_forwardable(self) -> bool:
        """False: aiohttp parses and answers requests itself, so they cannot be forwarded over a pipe."""
        return False

    def listen(self) -> None:
        """Build one runner per ``handle`` reactor, on that reactor's loop, and serve them.

        Each runner is kept on the thread model, to be cleaned up on its own
        loop when it stops; cleaning a runner up closes its connections.
        """
        # Local import: trpc_sdk.runtime imports this package at module level.
        from trpc_sdk.runtime import ResourceKind
        from trpc_sdk.runtime import current_thread_model

        self._is_base64 = TrpcConfig.server_config().encode_base64
        model = current_thread_model()
        group = model.handle
        runners: Dict[int, web.AppRunner] = {}
        for reactor in group.reactors:
            runner = reactor.submit(self._setup_runner, timeout=model.ready_timeout)
            model.keep_resource(reactor, runner, ResourceKind.RUNNER)
            runners[id(reactor)] = runner
        self._serve_stream(model, group, lambda reactor: runners[id(reactor)].server,
                           ssl_context_from_opts(self.opts))

    async def _setup_runner(self) -> web.AppRunner:
        """Build and start an aiohttp runner on the loop that will serve it.

        Must be awaited on the target loop: ``setup()`` is where the runner's
        ``web.Server`` captures its loop.

        Returns:
            A runner whose ``server`` attribute is usable as a protocol factory.
        """
        runner = web.AppRunner(self._build_app())
        await runner.setup()
        return runner

    def _build_app(self) -> web.Application:
        """Assemble the aiohttp application of this endpoint.

        Routes are added most specific first: ``@route`` routes of this
        service (or of no service in particular), then static files, then a
        catch-all that forwards anything left to the tRPC handler.
        Registering the catch-all earlier would shadow the other two.

        Returns:
            An application ready to be handed to an ``AppRunner``.
        """
        app = web.Application(client_max_size=TrpcConfig.server_config().max_rbuf_size)

        service_name = self.opts.service_adapter.opt.service_name
        for user_route in RegisteredRoutes.values():
            if user_route.service_name and user_route.service_name != service_name:
                continue
            resource = app.router.add_resource(user_route.path)
            method = user_route.method if user_route.method else hdrs.METH_ANY
            resource.add_route(method, partial(self._handle_route, user_route))

        for prefix, path_to_file in StaticFileRouters.items():
            app.router.add_static(prefix, path_to_file)

        resource = app.router.add_resource(r'/{name:.*}')
        resource.add_route(hdrs.METH_ANY, self._handle_rpc)
        return app

    def _create_context(self, request: Request) -> Tuple[TrpcContext, web.Response]:
        """Build the tRPC context of one request, with request/response on its message.

        The returned ``web.Response`` is the same object stored as
        ``message.server_raw_rsp``, so later header/body writes are what
        aiohttp sends.

        Args:
            request: Incoming aiohttp request.

        Returns:
            The context, and the empty ``web.Response`` filled after the handler.
        """
        ctx = TrpcContext()
        ctx.timeout = self.opts.idle_timeout
        ctx.recv_time = now_ms()

        response = web.Response()
        ctx.with_meta_data(TRANS_INFO_BASE64_KEY, self._is_base64)
        ctx.with_meta_data(LOCAL_ADDR, f"{self.opts.address[0]}:{self.opts.address[1]}")

        message = get_message(ctx)
        message.set_server_raw_req(request)
        message.set_server_raw_rsp(response)
        set_addr_to_ctx_from_transport(ctx, request.transport)
        return ctx, response

    async def _handle_rpc(self, request: Request) -> Response:
        """RPC catch-all: run the tRPC handler and fill the HTTP response.

        Body is not passed as bytes; :class:`ServerHttpCodec` reads it
        from the request on ``ctx``. The handler return value becomes
        ``response.body``.

        Args:
            request: Incoming aiohttp request.

        Returns:
            The aiohttp response to send.
        """
        ctx, response = self._create_context(request)
        ret = await self.opts.rpc_handler(ctx, b"")
        header = pop_http_rsp_header(ctx)
        response.headers.update(header)

        response.body = ret
        return response

    async def _handle_route(self, user_route: HttpRoute, request: Request) -> Response:
        """Serve a ``@route`` handler without the full RPC codec.

        Only tRPC headers are decoded, or the callback registered with
        :func:`~trpc_sdk.http.register_user_http_decode_func` runs instead.
        The handler return value may replace the whole ``Response``, or be
        written as body bytes/str.

        Args:
            user_route: Registered route record (path, method, handler).
            request: Incoming aiohttp request.

        Returns:
            The aiohttp response to send.
        """
        ctx, response = self._create_context(request)
        ctx.with_meta_data(USER_ROUTE_KEY, user_route)

        user_http_decode = get_user_http_decode_func()
        if user_http_decode is None:
            self._default_route_decode(ctx, request)
        else:
            user_http_decode(ctx)

        ret = await self.opts.service_adapter.handle_http(ctx, user_route.handler)
        ctx.with_meta_data(USER_ROUTE_KEY, None)

        if isinstance(ret, web.StreamResponse):
            response = ret
        elif isinstance(ret, (bytes, str)):
            response.body = ret
        header = pop_http_rsp_header(ctx)
        response.headers.update(header)
        return response

    def _default_route_decode(self, ctx: TrpcContext, request: Request) -> None:
        """Fill rpc_name and tRPC request headers without reading the body.

        Args:
            ctx: tRPC context that already has a TrpcMessage.
            request: Incoming aiohttp request.
        """
        message = get_message(ctx)
        message.set_server_rpc_name(request.path)
        ServerHttpCodec.decode_trpc_req_header(ctx, message, request)


@register_plugin(PluginType.SERVER_TRANSPORT, HTTP, init_pos=PluginInitPos.WORKER)
class HttpServerTransportFactory(BasePluginFactory):
    """Plugin factory that produces :class:`HttpServerTransport`."""

    def create(self) -> HttpServerTransport:
        """Create a new :class:`HttpServerTransport`.

        Returns:
            A fresh transport instance.
        """
        return HttpServerTransport()
