# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.
"""Extra HTTP helper module coverage."""

from unittest.mock import MagicMock

import pytest

from tests import helloworld_pb2
from trpc_sdk.abc import TrpcContext
from trpc_sdk.exceptions import ServerDecodeError
from trpc_sdk.http import BaseRequestContextManager
from trpc_sdk.http import FormSerializer
from trpc_sdk.http import HttpClientRspHeader
from trpc_sdk.http import RequestContextManager
from trpc_sdk.http import TrpcHttpResponse
from trpc_sdk.http import get_http_ok_codes
from trpc_sdk.http import get_http_rsp_header
from trpc_sdk.http import pop_http_ok_codes
from trpc_sdk.http import pop_http_rsp_header
from trpc_sdk.http import set_http_ok_codes
from trpc_sdk.http import set_http_rsp_header
from trpc_sdk.utils import dict_to_protobuf


def test_api_header_and_ok_code_roundtrip():
    ctx = TrpcContext()
    set_http_rsp_header(ctx, {"X-A": "1"})
    assert get_http_rsp_header(ctx) == {"X-A": "1"}
    assert pop_http_rsp_header(ctx) == {"X-A": "1"}
    assert pop_http_rsp_header(ctx) == {}

    set_http_ok_codes(ctx, {200, 201})
    assert get_http_ok_codes(ctx) == {200, 201}
    assert pop_http_ok_codes(ctx) == {200, 201}
    assert pop_http_ok_codes(ctx) == set()


def test_http_client_rsp_header_str_and_body_paths():
    headers = MagicMock()
    body_bytes = b"hello"
    rsp = HttpClientRspHeader(200, headers, body_bytes, stream=False)
    text = str(rsp)
    assert "status_code: 200" in text
    assert "stream: False" in text

    wrapped = TrpcHttpResponse(rsp)
    assert wrapped.body == "hello"
    assert wrapped.header is headers

    stream_body = MagicMock()
    stream_rsp = HttpClientRspHeader(200, headers, stream_body, stream=True)
    wrapped2 = TrpcHttpResponse(stream_rsp)
    assert wrapped2.body is stream_body

    wrapped2.release()
    stream_body.close.assert_called_once()
    wrapped2.release()
    assert stream_body.close.call_count == 1


@pytest.mark.asyncio
async def test_http_response_read():
    body = MagicMock()

    async def _read():
        return b"x"

    body.read = _read
    wrapped = TrpcHttpResponse(HttpClientRspHeader(200, MagicMock(), body, stream=True))
    assert await wrapped.read() == b"x"


@pytest.mark.asyncio
async def test_request_context_manager_coro_protocol():
    async def make_resp():
        resp = MagicMock()
        resp.release = MagicMock()
        return resp

    mgr = RequestContextManager(make_resp())
    async with mgr as resp:
        assert resp is not None
    resp.release.assert_called_once()

    async def simple():
        return 42

    base = BaseRequestContextManager(simple())
    assert await base == 42

    base2 = BaseRequestContextManager(simple())
    base2.close()

    base3 = BaseRequestContextManager(simple())
    with pytest.raises(ValueError):
        base3.throw(ValueError("x"))

    base4 = BaseRequestContextManager(simple())
    it = base4.__iter__()
    with pytest.raises(StopIteration) as exc:
        while True:
            next(it)
    assert exc.value.value == 42


def test_dict_to_protobuf_non_strict_and_ignore_none():
    req = helloworld_pb2.HelloRequest()
    dict_to_protobuf(req, {"msg": "hi", "unknown": 1}, strict=False)
    assert req.msg == "hi"

    req2 = helloworld_pb2.HelloRequest()
    dict_to_protobuf(req2, {"msg": None}, ignore_none=True)
    assert req2.msg == ""


def test_form_serializer_serialize_and_bad_deserialize():
    req = helloworld_pb2.HelloRequest(msg="x")
    data = FormSerializer.serialize(req)
    assert isinstance(data, bytes)

    with pytest.raises(ServerDecodeError):
        FormSerializer.deserialize(b"not=valid&for=pb", object())
