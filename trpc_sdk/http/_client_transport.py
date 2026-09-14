# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""HTTP client transport: turn a Message into an aiohttp round trip.

:class:`HttpClientTransport.round_trip` is the plugin behind protocol
``http``. It resolves the URL (logical scheme such as ``polaris://``
becomes ``http://ip:port/...``), copies tRPC identity onto headers, and
issues the request on a per-thread :class:`~trpc_sdk.http.HttpSession`.
"""

from __future__ import annotations

import asyncio
import base64
import json
import os
from typing import Any
from typing import Dict

import aiohttp
from aiohttp import hdrs

from trpc_sdk.abc import BaseClientTransport
from trpc_sdk.abc import PluginInitPos
from trpc_sdk.abc import PluginType
from trpc_sdk.abc import TrpcContext
from trpc_sdk.codec import Message
from trpc_sdk.codec import SerializationType
from trpc_sdk.exceptions import ClientInvokeError
from trpc_sdk.exceptions import ClientTimeoutError
from trpc_sdk.plugin import register_plugin
from trpc_sdk.transport import RoundTripOptions
from trpc_sdk.utils import now_ms
from ._constants import DEFAULT_SESSION_NAME
from ._constants import HTTP
from ._constants import HTTP_ARG_JSON
from ._constants import HTTP_ARG_SSL
from ._constants import HTTP_ARG_STREAM_KEY
from ._constants import HTTP_ARGS_META_KEY
from ._constants import HTTPS
from ._http_request import HttpClientReqHeader
from ._http_response import HttpClientRspHeader
from ._types import HTTP_TRPC_HEADER
from ._types import HttpSession
from ._types import UrlParser
from ._types import find_content_encoding
from ._types import find_content_type


_http_session_params: Dict[str, Dict[str, Any]] = {}


def register_http_session_params(params: dict, name: str = DEFAULT_SESSION_NAME) -> None:
    """Register kwargs passed to ``aiohttp.ClientSession`` for ``name``.

    Args:
        params: Keyword arguments forwarded to ``ClientSession``.
        name: Session name; ``DEFAULT_SESSION_NAME`` is the shared one.
    """
    global _http_session_params
    _http_session_params[name] = params


@register_plugin(PluginType.CLIENT_TRANSPORT, HTTP, init_pos=PluginInitPos.MASTER)
class HttpClientTransport(BaseClientTransport):
    """aiohttp-backed client transport registered as protocol ``http``.

    Sessions are stored on :class:`HttpSession` (thread-local) keyed by
    name plus pid, so forked workers and extra threads never share a
    loop-bound session.
    """

    _aio_sessions = {DEFAULT_SESSION_NAME: HttpSession()}

    @classmethod
    def _get_http_session(cls, name: str = DEFAULT_SESSION_NAME) -> aiohttp.ClientSession:
        """Get http session"""
        name = f"{name}_{os.getpid()}"
        session_params = _http_session_params.get(name, {})
        if name not in cls._aio_sessions:
            cls._aio_sessions[name] = HttpSession()
        if not cls._aio_sessions[name].instance:
            cls._aio_sessions[name].instance = aiohttp.ClientSession(**session_params)
        return cls._aio_sessions[name].instance

    @staticmethod
    def _get_url(req_header: HttpClientReqHeader, opts: RoundTripOptions, msg: Message):
        """Resolve a logical URL into an aiohttp-ready HTTP URL.

        Non-http schemes (or a missing ``parsed_url``) use
        ``opts.address`` as netloc. Direct ``http://...`` URLs are
        returned unchanged, with ``Host`` filled if needed.

        Args:
            req_header: Request header carrying scheme / parser.
            opts: Round-trip options with the selector-resolved address.
            msg: Client message, used for ``client_rpc_name`` as path.

        Returns:
            HTTP URL string.
        """
        addr = None
        if req_header.scheme not in {HTTP, HTTPS} or not req_header.parsed_url:
            req_header.scheme = HTTP
            addr = opts.address[0]
            if opts.address[1]:
                addr = f"{addr}:{opts.address[1]}"
        if not req_header.url_parser:
            if req_header.parsed_url:
                url_parser = UrlParser(result=req_header.parsed_url)
            else:
                url_parser = UrlParser(url=f"{req_header.scheme}://{addr}{msg.client_rpc_name}")
                url_parser.parse()
            req_header.url_parser = url_parser

        return req_header.url_parser.get_http_url(req_header.headers, addr)

    @staticmethod
    def _get_headers(msg: Message, req_header: HttpClientReqHeader):
        """Copy caller/callee/timeout/content-type/trans_info onto headers.

        Args:
            msg: Client message.
            req_header: Request header whose ``headers`` dict is mutated.

        Returns:
            The mutated headers dict.

        Raises:
            TypeError: Existing ``trpc-trans-info`` is neither dict nor JSON object.
        """
        headers = req_header.headers or {}

        headers[HTTP_TRPC_HEADER.trpc_caller] = msg.caller_service_name
        headers[HTTP_TRPC_HEADER.trpc_callee] = msg.callee_service_name
        headers[HTTP_TRPC_HEADER.trpc_timeout] = str(msg.timeout)

        if msg.serialization_type != SerializationType.Noop:
            headers[hdrs.CONTENT_TYPE] = find_content_type(msg.serialization_type, "")
        if msg.compress_type:
            headers[hdrs.CONTENT_ENCODING] = find_content_encoding(msg.compress_type, "")

        if msg.client_meta_data:
            trans_info = headers.get(HTTP_TRPC_HEADER.trpc_trans_info, {})
            if isinstance(trans_info, str):
                trans_info = json.loads(trans_info)
            if not isinstance(trans_info, dict):
                raise TypeError(f" 'trpc-trans-info' in headers is not dict {trans_info} ")
            for k, v in msg.client_meta_data.items():
                if not isinstance(v, (str, bytes)):
                    v = str(v)
                if isinstance(v, str):
                    v = v.encode("utf-8")
                trans_info[k] = base64.b64encode(v).decode("utf-8")
            headers[HTTP_TRPC_HEADER.trpc_trans_info] = json.dumps(trans_info)

        return headers

    async def round_trip(self, ctx: TrpcContext, req_data: bytes, options: RoundTripOptions) -> bytes:
        """Send the HTTP request and store the response on ``msg.client_rsp_head``.

        Unary calls read the whole body; stream calls return the live
        aiohttp response without reading. The invoke path always gets
        ``b''`` back — the body lives on the response head.

        Args:
            ctx: Call context.
            req_data: Serialized request body.
            options: Round-trip options (address, SSL, short-conn).

        Returns:
            ``b''``; the response is on ``message.client_rsp_head``.

        Raises:
            ClientInvokeError: ``client_req_head`` is missing or the
                request fails.
            ClientTimeoutError: The remaining deadline has expired.
        """
        msg = ctx.msg
        req_header: HttpClientReqHeader = msg.client_req_head
        if not req_header:
            raise ClientInvokeError(
                'RetClientEncodeFail, "http client transport: ReqHead should be type of http.HttpClientReqHeader")')

        method = req_header.method

        url = self._get_url(req_header, options, msg)

        headers = self._get_headers(msg, req_header)

        kwargs = ctx.get_meta_data(HTTP_ARGS_META_KEY, {})

        if HTTP_ARG_JSON in kwargs:
            req_data = None

        if HTTP_ARG_SSL in kwargs:
            options.ssl_context = kwargs[HTTP_ARG_SSL]
            del kwargs[HTTP_ARG_SSL]

        timeout = None
        if ctx.deadline > 0:
            remain = (ctx.deadline - now_ms()) / 1000.0
            if remain <= 0:
                raise ClientTimeoutError("http client transport reach deadline")
            timeout = aiohttp.ClientTimeout(total=remain)

        session_name = url if options.disable_reuse_http_session else DEFAULT_SESSION_NAME

        if HTTP_ARG_STREAM_KEY in kwargs:
            kwargs.pop(HTTP_ARG_STREAM_KEY)

        try:
            session_instance = self._get_http_session(session_name)
            if not req_header.stream:
                async with session_instance.request(
                        method,
                        url,
                        headers=headers,
                        data=req_data,
                        timeout=timeout,
                        ssl=options.ssl_context,
                        **kwargs,
                ) as resp:
                    rsp_data = await resp.content.read()
                    rsp_head = HttpClientRspHeader(resp.status, resp.headers, rsp_data, req_header.stream)
            else:
                resp = await session_instance.request(
                    method,
                    url,
                    headers=headers,
                    data=req_data,
                    ssl=options.ssl_context,
                    timeout=timeout,
                    **kwargs,
                )
                rsp_head = HttpClientRspHeader(resp.status, resp.headers, resp, req_header.stream)

            if isinstance(msg.client_rsp_head, HttpClientRspHeader):
                msg.client_rsp_head.status_code = rsp_head.status_code
                msg.client_rsp_head.headers = rsp_head.headers
                msg.client_rsp_head.body = rsp_head.body
                msg.client_rsp_head.stream = rsp_head.stream
            else:
                msg.set_client_rsp_head(rsp_head)
        except asyncio.TimeoutError as err:
            raise ClientTimeoutError(f"http client timeout to {url} err: {err}") from err
        except Exception as err:  # pylint: disable=broad-except
            raise ClientInvokeError(str(err)) from err

        if options.short_conn:
            await self._close_http_session(session_name)

        return b""

    @classmethod
    async def _close_http_session(cls, name: str = DEFAULT_SESSION_NAME) -> None:
        if name not in cls._aio_sessions:
            return
        if cls._aio_sessions[name].instance:
            await cls._aio_sessions[name].instance.close()
        cls._aio_sessions[name].instance = None
