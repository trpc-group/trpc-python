# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Address classification and the aiohttp application of an HTTP endpoint.

:func:`classify_address` is the one place that decides what kind of endpoint a
listen address is, so the modes can never disagree about it. They only differ
in which process and loop call ``listen``:

* ``merge``: the server process serves everything.
* ``separate``: the I/O process serves the ``TCP`` addresses, whose tRPC
  frames it can cut and forward. ``USER``, ``UDP`` and ``HTTP`` addresses are
  served by every rpc task itself, with ``SO_REUSEPORT``, because a frame the
  I/O process cannot delimit (or an aiohttp request) cannot be forwarded over
  the pipe.

Sockets, servers and sites are the transport's. What stays here is the
aiohttp application, because it is built from the worker's services and the
registered routes: :func:`http_runner_factory` hands it to an
:class:`~trpc_sdk.http.HttpServerTransport` as
``HttpServerTransport.set_runner_factory``.
"""

from __future__ import annotations

from functools import partial
from typing import Any
from typing import Awaitable
from typing import Callable

from aiohttp import hdrs
from aiohttp import web

from trpc_sdk.config import TrpcConfig
from trpc_sdk.http import HttpMessageHandle
from trpc_sdk.http import RegisteredRoutes
from trpc_sdk.http import StaticFileRouters
from trpc_sdk.transport import UserProtocolTransport
from ._constants import AddressKind
from ._constants import Network


def classify_address(worker: Any, index: int) -> AddressKind:
    """Decide which listener serves the listen address at ``index``.

    Priority: a protocol registered with ``register_protocol_handle`` (its
    service got a :class:`~trpc_sdk.transport.UserProtocolTransport`), then
    the network type. A composite network such as ``tcp,http`` matches TCP
    first.

    Args:
        worker: Worker whose per-address tables describe the address.
        index: Position of the address in ``worker.addresses``.

    Returns:
        The kind of listener, :attr:`AddressKind.UNKNOWN` when none applies.
    """
    if isinstance(worker.transports[index], UserProtocolTransport):
        return AddressKind.USER
    network = worker.networks[index]
    if network.find(Network.TCP) != -1:
        return AddressKind.TCP
    if network.find(Network.UDP) != -1:
        return AddressKind.UDP
    if network.find(Network.HTTP) != -1:
        return AddressKind.HTTP
    return AddressKind.UNKNOWN


def create_http_msg_handle(worker: Any, index: int) -> HttpMessageHandle:
    """Build the aiohttp-to-tRPC bridge for the HTTP address at ``index``.

    It holds no loop state, so one instance may serve every loop serving that
    address.

    Args:
        worker: Worker that owns the handler and timeout tables.
        index: Position of the address in ``worker.addresses``.
    """
    return HttpMessageHandle(index, worker, is_base64=TrpcConfig.server_config().encode_base64)


def reg_static_file(app: web.Application) -> None:
    """Serve every directory registered through ``trpc_sdk.http.register_static``."""
    for prefix, path_to_file in StaticFileRouters.items():
        app.router.add_static(prefix, path_to_file)


def add_user_route(app: web.Application, service_name: str, msg_handle: HttpMessageHandle) -> None:
    """Add user-registered HTTP routes belonging to one service."""
    for route_func in RegisteredRoutes.values():
        if route_func.service_name and route_func.service_name != service_name:
            continue
        resource = app.router.add_resource(route_func.path)
        http_handle = partial(msg_handle.handle_user_http_request, route_func)
        method = route_func.method if route_func.method else hdrs.METH_ANY
        resource.add_route(method, http_handle)


def build_http_app(worker: Any, index: int, msg_handle: HttpMessageHandle) -> web.Application:
    """Assemble the aiohttp application for one HTTP endpoint.

    Routes are added most specific first: user routes, then static files,
    then a catch-all that forwards anything left to the tRPC handler.
    Registering the catch-all earlier would shadow the other two.

    Args:
        worker: Worker serving the address.
        index: Position of the address in ``worker.addresses``.
        msg_handle: Bridge from :func:`create_http_msg_handle`.

    Returns:
        An application ready to be handed to an ``AppRunner``.
    """
    service_adapter = worker.services[index]
    app = web.Application(client_max_size=TrpcConfig.server_config().max_rbuf_size)

    add_user_route(app, service_adapter.opt.service_name, msg_handle)

    reg_static_file(app)

    resource = app.router.add_resource(r'/{name:.*}')
    resource.add_route(hdrs.METH_ANY, msg_handle.handle_http_request)

    return app


async def setup_http_runner(worker: Any, index: int, msg_handle: HttpMessageHandle) -> web.AppRunner:
    """Build and start an aiohttp runner on the loop that will serve it.

    Must be awaited on the target loop: ``setup()`` is where the runner's
    ``web.Server`` captures its loop.

    Args:
        worker: Worker serving the address.
        index: Position of the address in ``worker.addresses``.
        msg_handle: Bridge from :func:`create_http_msg_handle`.

    Returns:
        A runner whose ``server`` attribute is usable as a protocol factory.
    """
    runner = web.AppRunner(build_http_app(worker, index, msg_handle))
    await runner.setup()
    return runner


def http_runner_factory(worker: Any, index: int, msg_handle: HttpMessageHandle) -> Callable[[], Awaitable[Any]]:
    """Return the factory given to ``HttpServerTransport.set_runner_factory`` for one HTTP address.

    Args:
        worker: Worker serving the address.
        index: Position of the address in ``worker.addresses``.
        msg_handle: Bridge from :func:`create_http_msg_handle`, shared by
            every runner the factory builds.
    """
    return partial(setup_http_runner, worker, index, msg_handle)
