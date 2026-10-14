# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.
"""Tests for the context-first HTTP codec plugins."""

import http
import json

import pytest
from aiohttp import hdrs
from aiohttp import test_utils as aiohttp_test_utils
from aiohttp.web_response import Response
from multidict import CIMultiDict
from multidict import CIMultiDictProxy

from trpc_sdk.abc import TrpcContext
from trpc_sdk.abc import get_message
from trpc_sdk.exceptions import ClientInvokeError
from trpc_sdk.exceptions import TrpcErrorType
from trpc_sdk.exceptions import TrpcException
from trpc_sdk.http import HTTP_TRPC_HEADER
from trpc_sdk.http import ClientHttpCodec
from trpc_sdk.http import HttpClientReqHeader
from trpc_sdk.http import HttpClientRspHeader
from trpc_sdk.http import ServerHttpCodec
from trpc_sdk.http import set_http_ok_codes
from trpc_sdk.serialization import SerializationType


def _server_ctx(method: str, path: str, headers=None, payload: bytes = b"") -> TrpcContext:
    ctx = TrpcContext()
    msg = get_message(ctx)
    request = aiohttp_test_utils.make_mocked_request(method, path, headers=headers or {}, payload=None)
    if payload:
        async def _read():
            return payload
        request.read = _read
    msg.set_server_raw_req(request)
    msg.set_server_raw_rsp(Response())
    return ctx


def _client_ctx_with_rsp(status: int, headers=None, body: bytes = b"") -> TrpcContext:
    ctx = TrpcContext()
    rsp = HttpClientRspHeader(status, CIMultiDictProxy(CIMultiDict(headers or {})), body)
    get_message(ctx).set_client_rsp_head(rsp)
    return ctx


@pytest.mark.asyncio
async def test_server_decode_get_uses_query_string():
    ctx = _server_ctx("GET", "/trpc.app.server.Greeter/SayHello?a=1&b=2",
                      headers={HTTP_TRPC_HEADER.trpc_request_id: "7"})
    body = await ServerHttpCodec().decode_data(ctx, b"")
    msg = get_message(ctx)
    assert body == b"a=1&b=2"
    assert msg.serialization_type == SerializationType.Form
    assert msg.server_rpc_name == "/trpc.app.server.Greeter/SayHello"
    assert msg.callee_service_name == "trpc.app.server.Greeter"
    assert msg.server_req_head.request_id == 7


@pytest.mark.asyncio
async def test_server_decode_post_reads_body_and_trans_info():
    ctx = _server_ctx(
        "POST", "/svc/Method",
        headers={
            hdrs.CONTENT_TYPE: "application/json",
            HTTP_TRPC_HEADER.trpc_trans_info: json.dumps({"a": "b"}),
        },
        payload=b'{"msg": "hi"}',
    )
    body = await ServerHttpCodec().decode_data(ctx, b"")
    msg = get_message(ctx)
    assert body == b'{"msg": "hi"}'
    assert msg.serialization_type == SerializationType.JSON
    assert msg.server_meta_data["a"] == "b"


def test_server_encode_sets_content_type_and_error_headers():
    ctx = _server_ctx("POST", "/svc/Method", headers={hdrs.CONTENT_TYPE: "application/x-protobuf"})
    msg = get_message(ctx)
    msg.set_server_rsp_err(TrpcException(TrpcErrorType.BUSINESS, 1001, "biz failed"))

    assert ServerHttpCodec().encode_data(ctx, b"payload") == b"payload"

    rsp = msg.server_raw_rsp
    assert rsp.headers[hdrs.CONTENT_TYPE] == "application/x-protobuf"
    assert rsp.headers[HTTP_TRPC_HEADER.trpc_error_message] == "biz failed"
    assert rsp.headers[HTTP_TRPC_HEADER.trpc_user_func_error_code] == "1001"
    assert rsp.status == http.HTTPStatus.OK


def test_client_encode_fills_method_and_caller():
    ctx = TrpcContext()
    assert ClientHttpCodec().encode_data(ctx, b"") == b""
    msg = get_message(ctx)
    assert msg.client_req_head.method == hdrs.METH_GET
    assert msg.caller_service

    ctx = TrpcContext()
    ClientHttpCodec().encode_data(ctx, b"body")
    assert get_message(ctx).client_req_head.method == hdrs.METH_POST


def test_client_encode_keeps_existing_head():
    ctx = TrpcContext()
    msg = get_message(ctx)
    msg.set_client_req_head(HttpClientReqHeader(hdrs.METH_PUT))
    ClientHttpCodec().encode_data(ctx, b"body")
    assert msg.client_req_head.method == hdrs.METH_PUT


def test_client_decode_success_returns_body():
    ctx = _client_ctx_with_rsp(200, body=b"ok")
    assert ClientHttpCodec().decode_data(ctx, b"") == b"ok"
    assert get_message(ctx).client_rsp_err is None


def test_client_decode_maps_error_headers():
    ctx = _client_ctx_with_rsp(200, headers={
        HTTP_TRPC_HEADER.trpc_user_func_error_code: "42",
        HTTP_TRPC_HEADER.trpc_error_message: "bad",
    })
    ClientHttpCodec().decode_data(ctx, b"")
    err = get_message(ctx).client_rsp_err
    assert err.type == TrpcErrorType.BUSINESS
    assert err.code == 42


def test_client_decode_status_codes():
    ctx = _client_ctx_with_rsp(404)
    ClientHttpCodec().decode_data(ctx, b"")
    assert isinstance(get_message(ctx).client_rsp_err, ClientInvokeError)

    ctx = _client_ctx_with_rsp(404)
    set_http_ok_codes(ctx, {404})
    ClientHttpCodec().decode_data(ctx, b"")
    assert get_message(ctx).client_rsp_err is None


def test_client_decode_requires_rsp_head():
    with pytest.raises(ClientInvokeError):
        ClientHttpCodec().decode_data(TrpcContext(), b"")
