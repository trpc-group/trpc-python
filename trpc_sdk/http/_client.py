# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""HTTP client with tRPC filters, naming and a plain HTTP API.

:class:`Client` wraps :class:`trpc_sdk.client.Client` so ``get`` / ``post``
/ ``request`` go through the same filter chain and selector, while the
caller still writes ordinary URLs.
"""

from __future__ import annotations

from typing import Any
from urllib.parse import ParseResult

from aiohttp import hdrs

from trpc_sdk.client import Client as TrpcClient
from trpc_sdk.codec import clone_client_message
from ._constants import HTTP_ARG_STREAM_KEY
from ._constants import HTTP_ARGS_META_KEY
from ._ctx_manager import RequestContextManager
from ._exceptions import InvalidURL
from ._http_request import HttpClientReqHeader
from ._http_response import TrpcHttpResponse
from ._http_response import create_rsp_object
from ._options import get_options
from ._types import UrlParser


class Client:
    """HTTP client that reuses tRPC filters, naming and invoke.

    ``get`` / ``post`` / ``request`` return an async context manager that
    yields a :class:`~trpc_sdk.http.TrpcHttpResponse` and releases the
    underlying aiohttp response on exit.
    """

    def __init__(self, name: str = "") -> None:
        self.trpc_client = TrpcClient()
        self.name = name

    def create_http_context(self, ctx, url: ParseResult, headers: HttpClientReqHeader):
        """Clone ``ctx`` and attach callee name plus request headers.

        Args:
            ctx: Caller's tRPC context.
            url: Already-parsed URL; ``path`` becomes ``client_rpc_name``.
            headers: Request header record to store on the message.

        Returns:
            ``(cloned_ctx, message)`` ready for ``invoke``.
        """
        ctx, msg = clone_client_message(ctx)
        msg.set_client_rpc_name(url.path)

        service_name = self.name
        if not service_name:
            service_name = url.netloc

        msg.set_callee_service_name(service_name)
        msg.set_client_req_head(headers)

        return ctx, msg

    def post(
            self,
            ctx,
            url: str,
            options=None,
            *,
            data: Any = None,
            headers: Any = None,
            **kwargs: Any,
    ) -> RequestContextManager:
        """Send an HTTP POST.

        Args:
            ctx: tRPC context.
            url: Target URL.
            options: Extra tRPC client options.
            data: Request body.
            headers: Extra HTTP headers.
            **kwargs: Extra aiohttp kwargs stored on the context.

        Returns:
            Async context manager yielding a :class:`TrpcHttpResponse`.
        """
        return RequestContextManager(
            self._request(ctx, hdrs.METH_POST, url, options, data=data, headers=headers, **kwargs))

    def get(self, ctx, url: str, options=None, *, headers: Any = None, **kwargs: Any) -> RequestContextManager:
        """Send an HTTP GET.

        Args:
            ctx: tRPC context.
            url: Target URL.
            options: Extra tRPC client options.
            headers: Extra HTTP headers.
            **kwargs: Extra aiohttp kwargs stored on the context.

        Returns:
            Async context manager yielding a :class:`TrpcHttpResponse`.
        """
        return RequestContextManager(self._request(ctx, hdrs.METH_GET, url, options, headers=headers, **kwargs))

    def request(
            self,
            ctx,
            method,
            url,
            options=None,
            *,
            data: Any = None,
            headers: Any = None,
            **kwargs: Any,
    ) -> RequestContextManager:
        """Send an HTTP request with an arbitrary method.

        Args:
            ctx: tRPC context.
            method: HTTP method.
            url: Target URL.
            options: Extra tRPC client options.
            data: Request body.
            headers: Extra HTTP headers.
            **kwargs: Extra aiohttp kwargs stored on the context.

        Returns:
            Async context manager yielding a :class:`TrpcHttpResponse`.
        """
        return RequestContextManager(
            self._request(ctx, method, url, options, data=data, headers=headers, **kwargs))

    async def _request(
            self,
            ctx,
            method,
            url,
            options=None,
            *,
            data: Any = None,
            headers: Any = None,
            **kwargs: Any,
    ) -> TrpcHttpResponse:
        """Parse the URL, invoke, and wrap ``client_rsp_head`` as :class:`TrpcHttpResponse`.

        Args:
            ctx: tRPC context; remote/local addr are copied back after invoke.
            method: HTTP method.
            url: Target URL string.
            options: Extra tRPC client options.
            data: Request body.
            headers: Extra HTTP headers.
            **kwargs: Extra aiohttp kwargs stored under ``HTTP_ARGS_META_KEY``.

        Returns:
            :class:`TrpcHttpResponse` wrapping the transport's response head.

        Raises:
            InvalidURL: The URL has no scheme or netloc.
        """
        url_parser = UrlParser(url)

        result = url_parser.parse()

        if not url_parser.check_url():
            raise InvalidURL(f"invalid url {url}")

        headers = headers or {}

        stream = kwargs.get(HTTP_ARG_STREAM_KEY, True)
        trpc_http_header = HttpClientReqHeader(method=method, stream=stream, headers=headers, url_parser=url_parser)

        cli_ctx, cli_msg = self.create_http_context(ctx, result, trpc_http_header)

        opts = get_options(result, options)

        cli_ctx.with_meta_data(HTTP_ARGS_META_KEY, kwargs)
        await self.trpc_client.invoke(cli_ctx, data, create_rsp_object, opts)

        ctx.remote_ip = cli_ctx.remote_ip
        ctx.remote_port = cli_ctx.remote_port
        ctx.local_ip = cli_ctx.local_ip
        ctx.local_port = cli_ctx.local_port

        return TrpcHttpResponse(cli_msg.client_rsp_head)
