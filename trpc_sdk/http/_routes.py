# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""User-registered HTTP routes, static-file prefixes and the ``@route`` decode callback.

``@route`` writes into :data:`RegisteredRoutes`; :func:`register_static`
fills :data:`StaticFileRouters`. :class:`~trpc_sdk.http.HttpServerTransport`
reads both when it builds the aiohttp app, after fork: precise user routes
first, then static prefixes, then the RPC catch-all.
:func:`register_user_http_decode_func` replaces how a ``@route`` request is
decoded; the transport reads it on every request.
"""

from __future__ import annotations

from typing import Callable
from typing import Dict
from typing import Optional

from aiohttp import hdrs
from aiohttp.web_request import Request

from trpc_sdk.abc import TrpcContext


class HttpRoute:
    """One user-registered HTTP callback.

    Stored in :data:`RegisteredRoutes` and later bound onto aiohttp by
    :class:`~trpc_sdk.http.HttpServerTransport`.
    ``service_name`` limits the route to one HTTP service when set.
    """

    def __init__(
            self,
            handler: Callable[[TrpcContext, Request], bytes],
            method: str,
            path: str,
            service_name: Optional[str] = None,
    ) -> None:
        self.handler = handler
        self.method = method
        self.path = path
        self.service_name = service_name


RegisteredRoutes: Dict[str, HttpRoute] = {}
StaticFileRouters = {}

HttpDecodeCallback = Callable[[TrpcContext], None]
_user_http_decode_cb: Optional[HttpDecodeCallback] = None


def register_user_http_decode_func(user_http_decode: HttpDecodeCallback) -> None:
    """Replace the default header-only decode used by ``@route`` handlers.

    A registered callback fully replaces the default, including tRPC header
    parsing. It takes effect for requests served after the call.

    Args:
        user_http_decode: Callback that receives the tRPC context.
    """
    global _user_http_decode_cb
    _user_http_decode_cb = user_http_decode


def get_user_http_decode_func() -> Optional[HttpDecodeCallback]:
    """Return the callback set by :func:`register_user_http_decode_func`, or ``None`` for the default decode."""
    return _user_http_decode_cb


def route(path: str, method: str = hdrs.METH_ANY, service_name: Optional[str] = None):
    """Register a custom HTTP handler on ``path``.

    The decorator records the function into :data:`RegisteredRoutes`
    keyed by ``path_method`` (plus ``_service_name`` when given). It
    does not wrap the function; the worker looks the record up later.

    Args:
        path: HTTP path to match.
        method: HTTP method; defaults to any method.
        service_name: Optional service that should bind this route.

    Returns:
        A decorator that records ``f`` and returns ``None``.
    """

    def decorator(f):
        global RegisteredRoutes
        handler = HttpRoute(f, method, path, service_name)
        prefix = f"{path}_{method}"
        if service_name:
            prefix = f"{prefix}_{service_name}"
        RegisteredRoutes[prefix] = handler

    return decorator


def register_static(prefix: str, path_to_folder: str) -> None:
    """Register a static-file directory under ``prefix``.

    Args:
        prefix: HTTP path prefix, for example ``/static``.
        path_to_folder: Local directory that aiohttp should serve.
    """
    global StaticFileRouters
    StaticFileRouters[prefix] = path_to_folder
