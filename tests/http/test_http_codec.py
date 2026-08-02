# -*- coding: utf-8 -*-
"""Test http codec module"""
import base64
import http
import json

import pytest

from trpc_sdk.trpcpb import ResponseProtocol
from trpc_sdk.abc.context import Context  # pylint: disable=no-name-in-module
from trpc_sdk.codec import SerializationType
from trpc_sdk.codec.compress import CompressType
from trpc_sdk.codec.message import new_message  # pylint: disable=no-name-in-module
from trpc_sdk.exceptions import ClientInvokeError
from trpc_sdk.exceptions import ErrorCode
from trpc_sdk.exceptions import ErrorType
from trpc_sdk.exceptions import FuncNotFoundError
from trpc_sdk.exceptions import NewBusinessError
from trpc_sdk.exceptions import TRPCBaseError
from trpc_sdk.http import defines as http_defines
from trpc_sdk.http.codec import ClientHttpCodec
from trpc_sdk.http.codec import ClientReqHeader
from trpc_sdk.http.codec import ClientRspHeader
from trpc_sdk.http.codec import ServerHttpCodec


def test_trpc_header():
    """Test trpc_header"""
    assert "trpc-version" == http_defines.HTTP_TRPC_HEADER.trpc_version
    assert "trpc-call-type" == http_defines.HTTP_TRPC_HEADER.trpc_call_type
    assert "trpc-message-type" == http_defines.HTTP_TRPC_HEADER.trpc_message_type
    assert "trpc-request-id" == http_defines.HTTP_TRPC_HEADER.trpc_request_id
    assert "trpc-timeout" == http_defines.HTTP_TRPC_HEADER.trpc_timeout
    assert "trpc-caller" == http_defines.HTTP_TRPC_HEADER.trpc_caller
    assert "trpc-callee" == http_defines.HTTP_TRPC_HEADER.trpc_callee
    assert "trpc-trans-info" == http_defines.HTTP_TRPC_HEADER.trpc_trans_info
    assert "trpc-env" == http_defines.HTTP_TRPC_HEADER.trpc_env
    assert "trpc-dyeing-key" == http_defines.HTTP_TRPC_HEADER.trpc_dyeing_key
    assert "trpc-error-msg" == http_defines.HTTP_TRPC_HEADER.trpc_error_message
    assert "trpc-ret" == http_defines.HTTP_TRPC_HEADER.trpc_framework_error_code
    assert "trpc-func-ret" == http_defines.HTTP_TRPC_HEADER.trpc_user_func_error_code
    assert "Connection" == http_defines.HTTP_TRPC_HEADER.connection


def test_get_trpc_error_code():
    """Test find_serialization_type"""
    val = http_defines.get_trpc_error_code(ErrorType.Framework)
    assert val is http_defines.HTTP_TRPC_HEADER.trpc_framework_error_code
    val = http_defines.get_trpc_error_code(ErrorType.Business)
    assert val is http_defines.HTTP_TRPC_HEADER.trpc_user_func_error_code
    val = http_defines.get_trpc_error_code(100)
    assert val is None
    val = http_defines.get_trpc_error_code(-2, "test")
    assert val == "test"


def test_find_serialization_type():
    """Test find_serialization_type"""
    serialization_type = http_defines.find_serialization_type("application/json")
    assert SerializationType.JSON is serialization_type
    serialization_type = http_defines.find_serialization_type("application/protobuf")
    assert SerializationType.PB is serialization_type
    serialization_type = http_defines.find_serialization_type("application/x-protobuf")
    assert SerializationType.PB is serialization_type
    serialization_type = http_defines.find_serialization_type("application/pb")
    assert SerializationType.PB is serialization_type
    serialization_type = http_defines.find_serialization_type("application/proto")
    assert SerializationType.PB is serialization_type
    serialization_type = http_defines.find_serialization_type("application/jce")
    assert SerializationType.JCE is serialization_type
    serialization_type = http_defines.find_serialization_type("application/flatbuffer")
    assert SerializationType.FlatBuffer is serialization_type
    serialization_type = http_defines.find_serialization_type("application/octet-stream")
    assert SerializationType.Noop is serialization_type
    serialization_type = http_defines.find_serialization_type("application/x-www-form-urlencoded")
    assert SerializationType.Form is serialization_type
    # unknown
    serialization_type = http_defines.find_serialization_type("application/xxxx", SerializationType.JSON)
    assert SerializationType.JSON is serialization_type


def test_find_content_type():
    """Test find_content_type"""
    content_type = http_defines.find_content_type(SerializationType.JSON)
    assert "application/json" == content_type
    content_type = http_defines.find_content_type(SerializationType.PB)
    assert content_type
    content_type = http_defines.find_content_type(SerializationType.JCE)
    assert "application/jce" == content_type
    content_type = http_defines.find_content_type(SerializationType.FlatBuffer)
    assert "application/flatbuffer" == content_type
    content_type = http_defines.find_content_type(SerializationType.Noop)
    assert "application/octet-stream" == content_type
    content_type = http_defines.find_content_type(SerializationType.Form)
    assert "application/x-www-form-urlencoded" == content_type
    content_type = http_defines.find_content_type(-1)
    assert not content_type
    content_type = http_defines.find_content_type(10000, "test_content_type")
    assert "test_content_type" == content_type


def test_find_compress_type():
    """Test find_compress_type"""
    compress_type = http_defines.find_compress_type("gzip")
    assert CompressType.Gzip == compress_type
    compress_type = http_defines.find_compress_type("gzip1")
    assert not compress_type
    compress_type = http_defines.find_compress_type("gzip1", 1000)
    assert compress_type == 1000

    compress_type = http_defines.find_content_encoding(CompressType.Gzip)
    assert "gzip" == compress_type
    compress_type = http_defines.find_content_encoding(1000)
    assert not compress_type
    compress_type = http_defines.find_content_encoding(1000, "test_compress_type")
    assert compress_type == "test_compress_type"


def test_find_http_status():
    """Test find_http_status"""
    http_status = http_defines.find_http_status(ErrorCode.TRPC_INVOKE_SUCCESS)
    assert http.HTTPStatus.OK == http_status
    http_status = http_defines.find_http_status(ErrorCode.TRPC_SERVER_DECODE_ERR)
    assert http.HTTPStatus.BAD_REQUEST == http_status
    http_status = http_defines.find_http_status(ErrorCode.TRPC_SERVER_ENCODE_ERR)
    assert http.HTTPStatus.INTERNAL_SERVER_ERROR == http_status
    http_status = http_defines.find_http_status(ErrorCode.TRPC_SERVER_NOFUNC_ERR)
    assert http.HTTPStatus.NOT_FOUND == http_status
    http_status = http_defines.find_http_status(ErrorCode.TRPC_SERVER_NOSERVICE_ERR)
    assert http.HTTPStatus.NOT_FOUND == http_status
    http_status = http_defines.find_http_status(ErrorCode.TRPC_SERVER_TIMEOUT_ERR)
    assert http.HTTPStatus.GATEWAY_TIMEOUT == http_status
    http_status = http_defines.find_http_status(ErrorCode.TRPC_SERVER_OVERLOAD_ERR)
    assert http.HTTPStatus.TOO_MANY_REQUESTS == http_status
    http_status = http_defines.find_http_status(ErrorCode.TRPC_SERVER_SYSTEM_ERR)
    assert http.HTTPStatus.INTERNAL_SERVER_ERROR == http_status
    http_status = http_defines.find_http_status(ErrorCode.TRPC_SERVER_AUTH_FAIL)
    assert http.HTTPStatus.NON_AUTHORITATIVE_INFORMATION == http_status
    http_status = http_defines.find_http_status(ErrorCode.TRPC_SERVER_VALIDATE_FAIL)
    assert http.HTTPStatus.BAD_REQUEST == http_status
    http_status = http_defines.find_http_status(ErrorCode.TRPC_INVOKE_UNKNOWN_ERR)
    assert http.HTTPStatus.INTERNAL_SERVER_ERROR == http_status


class _MockHttpRequest:
    """Test Request"""

    def __init__(self, content_type: str, method: str):
        self.headers = {"Content-Type": content_type}
        self.method = method
        self.path = "/test"
        self.query_string = 'a=1&b=2'

    async def read(self):
        """Read data"""
        return b'123'


class _MockHttpResponse:
    """Test Response"""

    def __init__(self, content_type: str = None):
        self.headers = {}
        self.status = 0
        self.reason = "OK"
        if content_type:
            self.headers["Content-Type"] = content_type

    def set_status(self, status, reason="OK"):
        """set status"""
        self.status = status
        self.reason = reason


def test_server_codec_encode():
    """Test server codec encode"""
    ctx = Context()
    ctx, msg = new_message(ctx)

    # test content type
    ctx.meta_data[http_defines.HTTP_REQ_KEY] = _MockHttpRequest("", "GET")
    ctx.meta_data[http_defines.HTTP_RSP_KEY] = _MockHttpResponse()
    ServerHttpCodec.encode(msg, b'')
    assert ctx.meta_data[http_defines.HTTP_RSP_KEY].headers["Content-Type"] == "application/json"
    ctx.meta_data[http_defines.HTTP_REQ_KEY] = _MockHttpRequest("test", "GET")
    ctx.meta_data[http_defines.HTTP_RSP_KEY] = _MockHttpResponse()
    ServerHttpCodec.encode(msg, b'')
    assert ctx.meta_data[http_defines.HTTP_RSP_KEY].headers["Content-Type"] == "application/json"
    ctx.meta_data[http_defines.HTTP_REQ_KEY] = _MockHttpRequest("test", "POST")
    ctx.meta_data[http_defines.HTTP_RSP_KEY] = _MockHttpResponse()
    ServerHttpCodec.encode(msg, b'')
    assert ctx.meta_data[http_defines.HTTP_RSP_KEY].headers["Content-Type"] == "test"

    # test rsp err
    msg.server_rsp_err = FuncNotFoundError("test")
    ctx.meta_data[http_defines.HTTP_REQ_KEY] = _MockHttpRequest("", "GET")
    ctx.meta_data[http_defines.HTTP_RSP_KEY] = _MockHttpResponse("application/json")
    ServerHttpCodec.encode(msg, b'')
    assert ctx.meta_data[http_defines.HTTP_RSP_KEY].headers["trpc-error-msg"] == "test"
    assert ctx.meta_data[http_defines.HTTP_RSP_KEY].headers["trpc-ret"] == str(msg.server_rsp_err.code)
    assert ctx.meta_data[http_defines.HTTP_RSP_KEY].status == 404
    msg.server_rsp_err = NewBusinessError(1000, "test")
    ctx.meta_data[http_defines.HTTP_REQ_KEY] = _MockHttpRequest("", "GET")
    ctx.meta_data[http_defines.HTTP_RSP_KEY] = _MockHttpResponse()
    ServerHttpCodec.encode(msg, b'')
    assert ctx.meta_data[http_defines.HTTP_RSP_KEY].headers["trpc-func-ret"] == str(msg.server_rsp_err.code)
    # 非框架定义的错误码，返回200，由用户自己处理
    assert ctx.meta_data[http_defines.HTTP_RSP_KEY].status == 200

    # http 标准错误码，直接返回
    msg.server_rsp_err = NewBusinessError(501, "test")
    ctx.meta_data[http_defines.HTTP_REQ_KEY] = _MockHttpRequest("", "GET")
    ctx.meta_data[http_defines.HTTP_RSP_KEY] = _MockHttpResponse()
    ServerHttpCodec.encode(msg, b'')
    assert ctx.meta_data[http_defines.HTTP_RSP_KEY].status == 501
    assert ctx.meta_data[http_defines.HTTP_RSP_KEY].headers["trpc-func-ret"] == str(msg.server_rsp_err.code)
    assert ctx.meta_data[http_defines.HTTP_RSP_KEY].headers["trpc-func-ret"] == "501"

    msg.server_rsp_err = NewBusinessError(1000, "test")
    ctx.meta_data[http_defines.HTTP_REQ_KEY] = _MockHttpRequest("", "GET")
    ctx.meta_data[http_defines.HTTP_RSP_KEY] = _MockHttpResponse()
    ctx.meta_data[http_defines.HTTP_RSP_KEY].set_status(400)
    ServerHttpCodec.encode(msg, b'')
    assert ctx.meta_data[http_defines.HTTP_RSP_KEY].status == 400
    assert ctx.meta_data[http_defines.HTTP_RSP_KEY].headers["trpc-func-ret"] == str(msg.server_rsp_err.code)
    assert ctx.meta_data[http_defines.HTTP_RSP_KEY].headers["trpc-func-ret"] == "1000"

    # test rsp pb header
    trpc_rsp = ResponseProtocol()
    trpc_rsp.call_type = 2
    trpc_rsp.request_id = 5
    trpc_rsp.message_type = 8
    trpc_rsp.trans_info["aa"] = b"cc"
    msg.server_rsp_head = trpc_rsp
    meta_data = {"s1": "k1"}
    msg.server_meta_data = meta_data
    ctx.meta_data.clear()
    ctx.meta_data[http_defines.HTTP_REQ_KEY] = _MockHttpRequest("", "GET")
    ctx.meta_data[http_defines.HTTP_RSP_KEY] = _MockHttpResponse("application/json")
    ServerHttpCodec.encode(msg, b'')
    headers = ctx.meta_data[http_defines.HTTP_RSP_KEY].headers
    assert headers[http_defines.HTTP_TRPC_HEADER.trpc_call_type] == "2"
    assert headers[http_defines.HTTP_TRPC_HEADER.trpc_request_id] == "5"
    assert headers[http_defines.HTTP_TRPC_HEADER.trpc_message_type] == "8"
    assert headers[http_defines.HTTP_TRPC_HEADER.trpc_version] == "0"
    trans_info = headers[http_defines.HTTP_TRPC_HEADER.trpc_trans_info]
    trans_info = json.loads(trans_info)
    assert trans_info["aa"] == "cc"
    assert trans_info["s1"] == "k1"

    # test trpc_trans_info
    meta_data = {"s1": "k1", "t": (1, 2)}
    msg.server_meta_data = meta_data
    msg.server_rsp_head = None
    trpc_rsp = ResponseProtocol()
    trpc_rsp.trans_info["aa"] = b"cc"
    msg.server_rsp_head = trpc_rsp
    ctx.meta_data.clear()
    ctx.meta_data[http_defines.HTTP_REQ_KEY] = _MockHttpRequest("", "GET")
    ctx.meta_data[http_defines.HTTP_RSP_KEY] = _MockHttpResponse("application/json")
    ctx.meta_data[http_defines.TRANS_INFO_BASE64_KEY] = True
    ServerHttpCodec.encode(msg, b'')
    headers = ctx.meta_data[http_defines.HTTP_RSP_KEY].headers
    trans_info = headers[http_defines.HTTP_TRPC_HEADER.trpc_trans_info]
    trans_info = json.loads(trans_info)
    print(trans_info)


@pytest.mark.asyncio
async def test_server_codec_decode():
    """Test server codec decode"""
    ctx = Context()
    ctx, msg = new_message(ctx)

    http_req = _MockHttpRequest("", "GET")
    http_req.headers["trpc-version"] = "1"
    http_req.headers["trpc-call-type"] = "0"
    http_req.headers["trpc-message-type"] = "0"
    http_req.headers["trpc-request-id"] = "1"
    http_req.headers["trpc-timeout"] = "1000"
    http_req.headers["trpc-caller"] = "trpc_sdk.app.client.service"
    http_req.headers["trpc-callee"] = "trpc_sdk.app.server.service"
    http_req.headers["trpc-trans-info"] = '{"a": 1, "b": "cc"}'
    ctx.meta_data[http_defines.HTTP_REQ_KEY] = http_req
    ret = await ServerHttpCodec.decode(msg, b'')
    assert ret == b'a=1&b=2'
    trpc_req = msg.server_req_head
    assert trpc_req.version == 1
    assert trpc_req.call_type == 0
    assert trpc_req.message_type == 0
    assert trpc_req.request_id == 1
    assert trpc_req.timeout == 1000
    assert trpc_req.caller == b"trpc_sdk.app.client.service"
    assert trpc_req.callee == b"trpc_sdk.app.server.service"
    d1 = {}
    for key in trpc_req.trans_info:
        d1[key] = trpc_req.trans_info[key]
    assert d1["a"] == b'1'
    assert d1["b"] == b'cc'
    ctx.meta_data[http_defines.HTTP_REQ_KEY] = _MockHttpRequest("", "POST")
    ret = await ServerHttpCodec.decode(msg, b'')
    assert ret == b'123'
    ctx.meta_data[http_defines.TRANS_INFO_BASE64_KEY] = True
    trans_info = {"a": "aa", "b": "cc"}
    for k, v in trans_info.items():
        trans_info[k] = base64.b64encode(v.encode()).decode()
    http_req.headers["trpc-trans-info"] = json.dumps(trans_info)
    http_req.headers[http_defines.HTTP_TRPC_HEADER.trpc_caller_method] = "SayHello"
    ctx.meta_data[http_defines.HTTP_REQ_KEY] = http_req
    ret = await ServerHttpCodec.decode(msg, b'')


@pytest.mark.asyncio
async def test_client_codec():
    """Test client codec"""
    ctx = Context()
    ctx, msg = new_message(ctx)

    try:
        ClientHttpCodec.decode(msg, b'')
        assert False
    except Exception:  # pylint: disable=broad-exception-caught
        assert True

    # 测试 decode
    rsp_head = ClientRspHeader(0, {}, b'')
    rsp_head.status_code = http.HTTPStatus.OK
    msg.client_rsp_head = rsp_head
    rsp_head.headers["Content-Encoding"] = "gzip"
    ClientHttpCodec.decode(msg, b'')
    assert msg.compress_type == 1

    rsp_head.headers["trpc-ret"] = "121"
    ClientHttpCodec.decode(msg, b'')
    assert msg.client_rsp_err.type == 3
    assert msg.client_rsp_err.code == 121

    rsp_head.headers["trpc-ret"] = "0"
    rsp_head.headers["trpc-func-ret"] = "141"
    ClientHttpCodec.decode(msg, b'')
    assert msg.client_rsp_err.type == 2
    assert msg.client_rsp_err.code == 141

    rsp_head.headers["trpc-func-ret"] = "0"
    rsp_head.status_code = http.HTTPStatus.BAD_REQUEST
    ClientHttpCodec.decode(msg, b'')
    assert msg.client_rsp_err.type == 1
    assert msg.client_rsp_err.code == 999

    # test encode
    msg.client_req_head = None
    ret = ClientHttpCodec.encode(msg, b'test')
    assert ret == b'test'

    msg.client_req_head = ClientReqHeader()
    msg.caller_service = ""
    ClientHttpCodec.encode(msg, b'')

    msg.caller_service = "not none"
    msg.caller_server = ""
    msg.caller_service_name = "trpc_sdk.app.server.service"
    ClientHttpCodec.encode(msg, b'')

    msg.caller_server = "not none"
    msg.callee_server = ""
    msg.callee_service_name = "trpc_sdk.app.server.service"
    msg.client_rpc_name = "/trpc_sdk.app.server.service/method"
    ClientHttpCodec.encode(msg, b'')


def test_codec_encode():
    """Test codec encode"""
    ctx = Context()

    ctx, msg = new_message(ctx)
    ClientHttpCodec.encode(msg, b'')
    assert msg.client_req_head.method == "GET"
    assert msg.caller_app == "http"
    assert msg.caller_server == "pytest"
    assert msg.caller_service == "service"
    assert msg.caller_service_name == "trpc_sdk.http.pytest.service"
    assert msg.callee_app == ""
    assert msg.callee_server == ""
    assert msg.callee_service == ""
    assert msg.callee_method == ""

    ClientHttpCodec.encode(msg, b'\n\x05hello')
    assert msg.client_req_head.method == "GET"

    ctx, msg = new_message(ctx)
    ClientHttpCodec.encode(msg, b'\n\x05hello')
    assert msg.client_req_head.method == "POST"


def test_codec_decode():
    """Test codec decode"""
    ctx = Context()
    ctx, msg = new_message(ctx)

    with pytest.raises(ClientInvokeError):
        ClientHttpCodec.decode(msg, b'')

    rsp_head = ClientRspHeader(http.HTTPStatus.OK, {"trpc-ret": "1"}, b'')
    msg.set_client_rsp_head(rsp_head)
    ClientHttpCodec.decode(msg, b'')
    assert isinstance(msg.client_rsp_err, TRPCBaseError)
    assert msg.client_rsp_err.type == 3

    rsp_head = ClientRspHeader(http.HTTPStatus.OK, {"trpc-ret": "0", "trpc-func-ret": "1"}, b'')
    msg.set_client_rsp_head(rsp_head)
    ClientHttpCodec.decode(msg, b'')
    assert isinstance(msg.client_rsp_err, TRPCBaseError)
    assert msg.client_rsp_err.type == 2

    rsp_head = ClientRspHeader(http.HTTPStatus.FORBIDDEN, {"trpc-ret": "0", "trpc-func-ret": "0"}, b'')
    msg.set_client_rsp_head(rsp_head)
    ClientHttpCodec.decode(msg, b'')
    assert isinstance(msg.client_rsp_err, ClientInvokeError)

    rsp_head = ClientRspHeader(http.HTTPStatus.OK, {"trpc-ret": "0", "trpc-func-ret": "0"}, b'hello')
    msg.set_client_rsp_head(rsp_head)
    body = ClientHttpCodec.decode(msg, b'')
    assert body == b'hello'
