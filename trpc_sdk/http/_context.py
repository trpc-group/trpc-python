# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""HTTP request/response accessors on :class:`~trpc_sdk.abc.TrpcContext`.

Handlers use these functions to reach the aiohttp ``Request`` /
``Response`` held on a context's message, and the HTTP client uses them to
pull status and headers off a :class:`~trpc_sdk.http.TrpcHttpResponse`.
Extra response headers and accepted status codes travel through
dedicated meta keys so codec encode can merge them after the user
handler returns.
"""

from __future__ import annotations

from typing import Dict
from typing import Set

from aiohttp.web_request import CIMultiDictProxy
from aiohttp.web_request import Request
from aiohttp.web_response import Response

from trpc_sdk.abc import TrpcContext
from trpc_sdk.abc import get_message
from ._constants import HTTP_RSP_HEADER_KEY
from ._constants import HTTP_RSP_OK_CODE_KEY
from ._http_response import TrpcHttpResponse


def get_http_request(ctx: TrpcContext) -> Request:
    """Return the aiohttp request stored on ``ctx``'s message.

    Args:
        ctx: Server-side tRPC context of an HTTP call.

    Returns:
        The aiohttp ``Request`` held as ``message.server_raw_req``, or
        None outside an HTTP server call.
    """
    return get_message(ctx).server_raw_req


def get_http_response(ctx: TrpcContext) -> Response:
    """Return the aiohttp response stored on ``ctx``'s message.

    Args:
        ctx: Server-side tRPC context of an HTTP call.

    Returns:
        The aiohttp ``Response`` held as ``message.server_raw_rsp``, or
        None outside an HTTP server call.
    """
    return get_message(ctx).server_raw_rsp


def get_http_header_in_client(rsp: TrpcHttpResponse) -> CIMultiDictProxy:
    """Return HTTP headers from a client response.

    Args:
        rsp: Client response wrapper.

    Returns:
        Case-insensitive multi-dict of response headers.
    """
    return rsp.header


def get_http_status_in_client(rsp: TrpcHttpResponse) -> int:
    """Return the HTTP status code from a client response.

    Args:
        rsp: Client response wrapper.

    Returns:
        HTTP status code.
    """
    return rsp.status


def set_http_rsp_header(ctx: TrpcContext, header: Dict) -> None:
    """Store extra response headers on ``ctx`` for later merge.

    Args:
        ctx: tRPC context.
        header: Headers the handler wants on the HTTP response.
    """
    ctx.with_meta_data(HTTP_RSP_HEADER_KEY, header)


def get_http_rsp_header(ctx: TrpcContext) -> Dict:
    """Return extra response headers previously stored on ``ctx``.

    Args:
        ctx: tRPC context.

    Returns:
        Header dict, or ``{}`` if none was set.
    """
    return ctx.get_meta_data(HTTP_RSP_HEADER_KEY, {})


def pop_http_rsp_header(ctx: TrpcContext) -> Dict:
    """Pop extra response headers off ``ctx``.

    :class:`~trpc_sdk.http.HttpMessageHandle` calls this after the handler
    returns so the extra headers can be merged into the aiohttp
    response exactly once.

    Args:
        ctx: tRPC context.

    Returns:
        Header dict, or ``{}`` if none was set.
    """
    header = {}
    if HTTP_RSP_HEADER_KEY in ctx.meta_data:
        header = ctx.pop_meta_data(HTTP_RSP_HEADER_KEY)
    return header


def set_http_ok_codes(ctx: TrpcContext, ok_codes: Set) -> None:
    """Store the set of HTTP status codes the client treats as success.

    Args:
        ctx: tRPC context.
        ok_codes: Status codes that should not become a client error.
    """
    ctx.with_meta_data(HTTP_RSP_OK_CODE_KEY, ok_codes)


def get_http_ok_codes(ctx: TrpcContext) -> Set:
    """Return the accepted HTTP status codes stored on ``ctx``.

    Args:
        ctx: tRPC context.

    Returns:
        Set of status codes, or ``set()`` if none was set.
    """
    return ctx.get_meta_data(HTTP_RSP_OK_CODE_KEY, set())


def pop_http_ok_codes(ctx: TrpcContext) -> Set:
    """Pop the accepted HTTP status codes off ``ctx``.

    :class:`~trpc_sdk.http.ClientHttpCodec` calls this during decode so a
    non-OK HTTP status can be turned into a client-side error unless the
    caller listed it as acceptable.

    Args:
        ctx: tRPC context.

    Returns:
        Set of status codes, or ``set()`` if none was set.
    """
    err_code = set()
    if HTTP_RSP_OK_CODE_KEY in ctx.meta_data:
        err_code = ctx.pop_meta_data(HTTP_RSP_OK_CODE_KEY)
    return err_code
