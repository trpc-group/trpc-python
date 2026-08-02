# -*- coding: utf-8 -*-
"""Extra HTTP helper module coverage."""

from unittest.mock import MagicMock

import pytest

from trpc_sdk.context import TrpcContext
from trpc_sdk.exceptions import ServerDecodeError
from trpc_sdk.http import _api
from trpc_sdk.http._http_response import HttpClientRspHeader, TrpcHttpResponse
from trpc_sdk.http.dict_pb_convertor import dict_to_protobuf
from trpc_sdk.http.http_ctx_manager import _BaseRequestContextManager, _RequestContextManager
from trpc_sdk.http.serialization_form import FormSerializer

from . import helloworld_pb2


def test_api_header_and_ok_code_roundtrip():
    ctx = TrpcContext()
    _api.set_http_rsp_header(ctx, {"X-A": "1"})
    assert _api.get_http_rsp_header(ctx) == {"X-A": "1"}
    assert _api.pop_http_rsp_header(ctx) == {"X-A": "1"}
    assert _api.pop_http_rsp_header(ctx) == {}

    _api.set_http_ok_codes(ctx, {200, 201})
    assert _api.get_http_ok_codes(ctx) == {200, 201}
    assert _api.pop_http_ok_codes(ctx) == {200, 201}
    assert _api.pop_http_ok_codes(ctx) == set()


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

    mgr = _RequestContextManager(make_resp())
    async with mgr as resp:
        assert resp is not None
    resp.release.assert_called_once()

    async def simple():
        return 42

    base = _BaseRequestContextManager(simple())
    assert await base == 42

    base2 = _BaseRequestContextManager(simple())
    base2.close()

    base3 = _BaseRequestContextManager(simple())
    with pytest.raises(ValueError):
        base3.throw(ValueError("x"))

    base4 = _BaseRequestContextManager(simple())
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
