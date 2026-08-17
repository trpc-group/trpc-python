# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Helpers that build the aiohttp application served by an HTTP listener.

An HTTP endpoint in ``multi-listener`` mode is an ``aiohttp`` application whose
routes come from three places: the handlers the user registered with
``@trpc.http.route``, the static directories registered with
``trpc.http.register_static``, and a catch-all route that feeds anything else
back into the tRPC service. TLS, when configured, is layered on top of the
same application.

These functions are stateless and take the application explicitly, so they
depend on the HTTP stack only and not on the process model around it.
"""

from __future__ import annotations

import os
import ssl
from functools import partial
from typing import Optional

from aiohttp import hdrs
from aiohttp import web

from trpc_sdk.abc import BaseServerTransportOption
from trpc_sdk.http import HttpMessageHandle
from trpc_sdk.http import routes


def get_https_certs(service_config: BaseServerTransportOption) -> Optional[ssl.SSLContext]:
    """Build the server-side TLS context for one listen endpoint.

    Args:
        service_config: Transport options of the endpoint, holding the paths
            to the certificate and its private key.

    Returns:
        An ``SSLContext`` loaded with the certificate chain, or ``None`` when
        the endpoint is configured without TLS.

    Raises:
        KeyError: If the private key path is set but no such file exists.
        ValueError: If the certificate path is set but no such file exists.
    """
    key = service_config.tls_key
    cert = service_config.tls_cert

    if len(key) == 0 or len(cert) == 0:
        return None

    if not os.path.exists(key):
        raise KeyError(f"cert key file not exist: {key}")

    if not os.path.exists(cert):
        raise ValueError(f"cert key file not exist: {cert}")

    ssl_context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
    ssl_context.load_cert_chain(cert, key)
    return ssl_context


def reg_static_file(app: web.Application) -> None:
    """Serve every directory registered through ``trpc.http.register_static``.

    Args:
        app: Application that receives the static routes.
    """
    for prefix, path_to_file in routes.StaticFileRouters.items():
        app.router.add_static(prefix, path_to_file)


def add_user_route(app: web.Application, service_name: str, msg_handle: HttpMessageHandle) -> None:
    """Add the user-registered HTTP routes that belong to one service.

    Routes are registered process-wide, but each HTTP listener serves a single
    service, so a route bound to another service name is skipped here and
    picked up by that service's own listener. A route without a service name
    is served by every listener.

    Args:
        app: Application that receives the routes.
        service_name: Service this listener serves.
        msg_handle: Bridge that turns an aiohttp request into a tRPC call.
    """
    for route_func in routes.RegisteredRoutes.values():
        if route_func.service_name and route_func.service_name != service_name:
            continue
        resource = app.router.add_resource(route_func.path)
        http_handle = partial(msg_handle.handle_user_http_request, route_func)
        method = route_func.method if route_func.method else hdrs.METH_ANY
        resource.add_route(method, http_handle)
