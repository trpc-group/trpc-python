# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Per-listen-port adapter between aiohttp and tRPC.

:class:`HttpMessageHandle` is bound to one HTTP listen index on a
:class:`~trpc_sdk.server.MergeWorker`. Catch-all paths go through
:meth:`HttpMessageHandle.handle_http_request`; ``@route`` paths go
through :meth:`HttpMessageHandle.handle_user_http_request`. Both share
:meth:`HttpMessageHandle._init_request_context`.
"""

from __future__ import annotations

from typing import Callable
from typing import Optional

from aiohttp import web
from aiohttp.web_request import Request
from aiohttp.web_response import Response

from trpc_sdk.abc import TrpcContext
from trpc_sdk.abc import get_message
from trpc_sdk.utils import now_ms
from trpc_sdk.utils import set_addr_to_ctx_from_transport
from ._codec import ServerHttpCodec
from ._constants import HTTP_REQ_KEY
from ._constants import HTTP_RSP_KEY
from ._constants import LOCAL_ADDR
from ._constants import TRANS_INFO_BASE64_KEY
from ._constants import USER_ROUTE_KEY
from ._context import pop_http_rsp_header
from ._routes import HttpRoute


HttpDecodeCallback = Callable[[TrpcContext], None]
_user_http_decode_cb: Optional[HttpDecodeCallback] = None


def register_user_http_decode_func(user_http_decode: HttpDecodeCallback) -> None:
    """Replace the default header-only decode used by ``@route`` handlers.

    When set, :meth:`HttpMessageHandle.handle_user_http_request` calls
    this instead of :meth:`HttpMessageHandle._default_user_http_decode`.
    A registered callback fully replaces the default, including tRPC
    header parsing.

    Args:
        user_http_decode: Callback that receives the tRPC context.
    """
    global _user_http_decode_cb
    _user_http_decode_cb = user_http_decode


class HttpMessageHandle:
    """Per-listen-port adapter between aiohttp and tRPC.

    One instance is bound to one listen index on a
    :class:`~trpc_sdk.server.MergeWorker`. Catch-all paths go through
    :meth:`handle_http_request`; ``@route`` paths go through
    :meth:`handle_user_http_request`. Both share
    :meth:`_init_request_context`.
    """

    def __init__(self, index: int, process_info, is_base64: bool = False) -> None:
        # process_info is a trpc_sdk.server.MergeWorker; left untyped to
        # avoid a runtime import cycle with trpc_sdk.server.
        self._index = index
        self._is_base64 = is_base64
        self._process_info = process_info
        self._addr = f"{self._process_info.addresses[self._index][0]}:{self._process_info.addresses[self._index][1]}"

    def _init_request_context(self, ctx: TrpcContext, request: Request) -> web.Response:
        """Stash timeout, request/response and TrpcMessage onto ``ctx``.

        The returned ``web.Response`` is the same object stored under
        ``HTTP_RSP_KEY``, so later header/body writes are what aiohttp
        sends.

        Args:
            ctx: Fresh tRPC context for this request.
            request: Incoming aiohttp request.

        Returns:
            Empty ``web.Response`` that will be filled after the handler.
        """
        ctx.timeout = self._process_info.timeouts[self._index]
        ctx.recv_time = now_ms()

        response = web.Response()
        ctx.with_meta_data(HTTP_REQ_KEY, request)
        ctx.with_meta_data(HTTP_RSP_KEY, response)
        ctx.with_meta_data(TRANS_INFO_BASE64_KEY, self._is_base64)
        ctx.with_meta_data(LOCAL_ADDR, self._addr)

        get_message(ctx)
        set_addr_to_ctx_from_transport(ctx, request.transport)

        return response

    async def handle_http_request(self, request: Request) -> Response:
        """RPC catch-all: run the tRPC handler and fill the HTTP response.

        Body is not passed as bytes; :class:`ServerHttpCodec` reads it
        from the request on ``ctx``. The handler return value becomes
        ``response.body``.

        Args:
            request: Incoming aiohttp request.

        Returns:
            The aiohttp response to send.
        """
        ctx = TrpcContext()
        response = self._init_request_context(ctx, request)
        ret = await self._process_info.handlers[self._index](ctx, b"")
        header = pop_http_rsp_header(ctx)
        response.headers.update(header)

        response.body = ret
        return response

    async def handle_user_http_request(self, user_route: HttpRoute, request: Request) -> Response:
        """Serve a ``@route`` handler without the full RPC codec.

        Only tRPC headers are decoded (or a user callback, if
        registered). The handler return value may replace the whole
        ``Response``, or be written as body bytes/str.

        Args:
            user_route: Registered route record (path, method, handler).
            request: Incoming aiohttp request.

        Returns:
            The aiohttp response to send.
        """
        ctx = TrpcContext()
        response = self._init_request_context(ctx, request)
        ctx.with_meta_data(USER_ROUTE_KEY, user_route)

        global _user_http_decode_cb
        if _user_http_decode_cb is None:
            self._default_user_http_decode(ctx, request)
        else:
            _user_http_decode_cb(ctx)

        ret = await self._process_info.services[self._index].handle_http(ctx, user_route.handler)
        ctx.with_meta_data(USER_ROUTE_KEY, None)

        if isinstance(ret, web.StreamResponse):
            response = ret
        elif isinstance(ret, (bytes, str)):
            response.body = ret
        header = pop_http_rsp_header(ctx)
        response.headers.update(header)
        return response

    def _default_user_http_decode(self, ctx: TrpcContext, request: Request) -> None:
        """Fill rpc_name and tRPC request headers without reading the body.

        Args:
            ctx: tRPC context that already has a TrpcMessage.
            request: Incoming aiohttp request.
        """
        message = get_message(ctx)
        message.set_server_rpc_name(request.path)
        ServerHttpCodec.decode_trpc_req_header(message, request)
