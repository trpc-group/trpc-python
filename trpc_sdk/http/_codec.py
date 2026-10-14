# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""tRPC-over-HTTP codec: encode and decode between TrpcMessage and HTTP.

The codec never talks to a socket. On the server it reads the aiohttp
``Request`` / ``Response`` from context meta, fills
:class:`~trpc_sdk.abc.TrpcMessage`, and writes status plus tRPC headers back
onto the response. On the client it only fills caller identity and
method defaults; :class:`~trpc_sdk.http.HttpClientTransport` builds the
actual HTTP request.
"""

from __future__ import annotations

import base64
import http
import json
import os
import sys
from typing import Any

from aiohttp import hdrs
from aiohttp.web_request import CIMultiDictProxy
from aiohttp.web_request import Request as HttpRequest
from aiohttp.web_response import Response as HttpResponse

from trpc_sdk._inner import trpc_pb2
from trpc_sdk.abc import BaseClientProtocolParser
from trpc_sdk.abc import BaseCodec
from trpc_sdk.abc import BaseServerProtocolParser
from trpc_sdk.abc import PluginInitPos
from trpc_sdk.abc import PluginType
from trpc_sdk.abc import TrpcContext
from trpc_sdk.abc import TrpcMessage
from trpc_sdk.abc import get_message
from trpc_sdk.exceptions import ClientInvokeError
from trpc_sdk.exceptions import TrpcErrorCode
from trpc_sdk.exceptions import TrpcErrorType
from trpc_sdk.exceptions import TrpcException
from trpc_sdk.plugin import register_plugin
from trpc_sdk.serialization import SerializationType
from ._constants import HTTP
from ._constants import TRANS_INFO_BASE64_KEY
from ._context import pop_http_ok_codes
from ._http_request import HttpClientReqHeader
from ._http_response import HttpClientRspHeader
from ._types import HTTP_TRPC_HEADER
from ._types import find_compress_type
from ._types import find_http_status
from ._types import find_serialization_type
from ._types import get_trpc_error_code


@register_plugin(PluginType.SERVER_PARSER, HTTP, init_pos=PluginInitPos.WORKER)
class ServerHttpParser(BaseServerProtocolParser):
    """HTTP server frame parser.

    HTTP framing is handled by aiohttp, so this parser is a no-op
    plugin registration that occupies the ``http`` parser slot.
    """


@register_plugin(PluginType.SERVER_CODEC, HTTP, init_pos=PluginInitPos.WORKER)
class ServerHttpCodec(BaseCodec):
    """Server codec: pack RPC results into an aiohttp response.

    ``encode_data`` / ``decode_data`` read the aiohttp request / response
    stored on the context's message.
    """

    def encode_data(self, context: TrpcContext, body: bytes) -> bytes:
        """Write status, Content-Type and tRPC headers onto the response.

        Does not wrap ``body``; aiohttp sends it as-is after
        :class:`~trpc_sdk.http.HttpMessageHandle` assigns it to
        ``response.body``.

        Args:
            context: Server request context whose message holds the aiohttp
                request and response as ``server_raw_req`` / ``server_raw_rsp``.
            body: Already-serialized response body.

        Returns:
            ``body`` unchanged.
        """
        message: TrpcMessage = get_message(context)
        request: HttpRequest = message.server_raw_req
        response: HttpResponse = message.server_raw_rsp

        content_type = response.headers.get(hdrs.CONTENT_TYPE, "")
        if content_type == "":
            req_content_type = request.headers.get(hdrs.CONTENT_TYPE, "")
            if req_content_type == "" or request.method == hdrs.METH_GET:
                response.headers[hdrs.CONTENT_TYPE] = "application/json"
            else:
                response.headers[hdrs.CONTENT_TYPE] = req_content_type

        err: TrpcException = message.server_rsp_err
        if err.code != TrpcErrorCode.TRPC_INVOKE_SUCCESS:
            response.headers[HTTP_TRPC_HEADER.trpc_error_message] = err.msg
            trpc_error_code = get_trpc_error_code(
                err.type,
                HTTP_TRPC_HEADER.trpc_user_func_error_code,
            )
            response.headers[trpc_error_code] = str(err.code)
            http_code = find_http_status(err.code, None)
            if http_code is None:
                if response.status in http.HTTPStatus.__members__.values():
                    http_code = http.HTTPStatus(response.status)
                elif err.code in http.HTTPStatus.__members__.values():
                    http_code = http.HTTPStatus(err.code)
                else:
                    # Non-standard HTTP codes still go out as 200; the
                    # tRPC error headers carry the real code.
                    http_code = http.HTTPStatus.OK
            response.set_status(http_code.value, http_code.phrase)

        self.encode_trpc_rsp_header(context, message, response.headers)

        return body

    @classmethod
    def encode_trpc_rsp_header(
        cls,
        context: TrpcContext,
        message: TrpcMessage,
        rsp_headers: CIMultiDictProxy,
    ) -> None:
        """Write tRPC call-type / request-id / trans_info onto ``rsp_headers``.

        Args:
            context: Server request context; supplies the
                ``TRANS_INFO_BASE64_KEY`` flag.
            message: Server-side tRPC message.
            rsp_headers: aiohttp response headers to mutate.
        """
        trans_info = message.server_meta_data or {}
        is_base64 = context.get_meta_data(TRANS_INFO_BASE64_KEY, False)
        if isinstance(message.server_rsp_head, trpc_pb2.ResponseProtocol):
            trpc_rsp: trpc_pb2.ResponseProtocol = message.server_rsp_head
            rsp_headers[HTTP_TRPC_HEADER.trpc_call_type] = str(trpc_rsp.call_type)
            rsp_headers[HTTP_TRPC_HEADER.trpc_request_id] = str(trpc_rsp.request_id)
            rsp_headers[HTTP_TRPC_HEADER.trpc_message_type] = str(trpc_rsp.message_type)
            rsp_headers[HTTP_TRPC_HEADER.trpc_version] = str(trpc_pb2.TRPC_PROTO_V1)
            trans_info.update(trpc_rsp.trans_info)

        for key, value in trans_info.items():
            if is_base64:
                if not isinstance(value, bytes):
                    value = str(value).encode()
                value = base64.b64encode(value)
            if isinstance(value, bytes):
                value = value.decode()
            trans_info[key] = value
        rsp_headers[HTTP_TRPC_HEADER.trpc_trans_info] = json.dumps(trans_info)

    async def decode_data(self, context: TrpcContext, _: bytes) -> bytes:
        """Read path, tRPC headers and body from the aiohttp request.

        GET uses the query string as the body and Form serialization;
        other methods ``read()`` the request body and pick serialization
        from Content-Type.

        Args:
            context: Server request context whose message holds the aiohttp
                request as ``server_raw_req``.
            _: Unused; HTTP body is read from that request.

        Returns:
            Request body bytes (query string for GET).
        """
        message: TrpcMessage = get_message(context)
        request: HttpRequest = message.server_raw_req

        url_path = request.path
        message.set_server_rpc_name(url_path)

        self.decode_trpc_req_header(context, message, request)
        trpc_req = message.server_req_head

        if request.method == hdrs.METH_GET:
            req_body = request.query_string.encode("utf-8")
            message.set_serialization_type(SerializationType.Form)
            trpc_req.content_type = SerializationType.Form
            return req_body

        req_body = await request.read()

        message.set_serialization_type(SerializationType.JSON)

        content_type = request.headers.get(hdrs.CONTENT_TYPE, "")
        serialization_type = find_serialization_type(content_type, SerializationType.JSON)
        message.set_serialization_type(serialization_type)
        trpc_req.content_type = serialization_type

        return req_body

    @classmethod
    def decode_trpc_req_header(
        cls,
        context: TrpcContext,
        message: TrpcMessage,
        request: HttpRequest,
    ) -> None:
        """Fill ``trpc_pb2.RequestProtocol`` and TrpcMessage identity from HTTP headers.

        Args:
            context: Server request context; supplies the
                ``TRANS_INFO_BASE64_KEY`` flag.
            message: Server-side tRPC message.
            request: Incoming aiohttp request.
        """
        trpc_req = trpc_pb2.RequestProtocol()
        headers: CIMultiDictProxy = request.headers

        trpc_version = headers.get(HTTP_TRPC_HEADER.trpc_version, trpc_req.version)
        trpc_req.version = int(trpc_version)

        trpc_call_type = headers.get(HTTP_TRPC_HEADER.trpc_call_type, trpc_req.call_type)
        trpc_req.call_type = int(trpc_call_type)

        trpc_message_type = headers.get(HTTP_TRPC_HEADER.trpc_message_type, trpc_req.message_type)
        trpc_req.message_type = int(trpc_message_type)

        trpc_request_id = headers.get(HTTP_TRPC_HEADER.trpc_request_id, trpc_req.request_id)
        trpc_req.request_id = int(trpc_request_id)
        message.set_request_id(trpc_req.request_id)

        trpc_timeout = headers.get(HTTP_TRPC_HEADER.trpc_timeout, trpc_req.timeout)
        trpc_req.timeout = int(trpc_timeout)
        message.set_timeout(trpc_req.timeout)

        trpc_caller_method = headers.get(HTTP_TRPC_HEADER.trpc_caller_method)
        if trpc_caller_method:
            message.set_caller_method(trpc_caller_method)

        trpc_caller = headers.get(HTTP_TRPC_HEADER.trpc_caller, None)
        if trpc_caller:
            trpc_req.caller = trpc_caller.encode("utf-8")
            message.set_caller_service_name(trpc_caller)

        trpc_req.func = request.path.encode("utf-8")
        path = request.path.split("/")
        callee = path[1] if len(path) > 1 else None

        trpc_callee = headers.get(HTTP_TRPC_HEADER.trpc_callee, callee)
        if trpc_callee:
            trpc_req.callee = trpc_callee.encode("utf-8")
            message.set_callee_service_name(trpc_callee)

        message.set_dyeing((trpc_req.message_type & trpc_pb2.TRPC_DYEING_MESSAGE) != 0)

        trpc_trans_info = headers.get(HTTP_TRPC_HEADER.trpc_trans_info, None)
        is_base64 = context.get_meta_data(TRANS_INFO_BASE64_KEY, False)
        if trpc_trans_info:
            trpc_trans_info: dict = json.loads(trpc_trans_info)
            if is_base64:
                for k, v in trpc_trans_info.items():
                    val = base64.b64decode(v)
                    try:
                        trpc_trans_info[k] = val.decode()
                    except Exception:
                        trpc_trans_info[k] = val
            message.server_meta_data.update(trpc_trans_info)
            for k, v in trpc_trans_info.items():
                if not isinstance(v, bytes):
                    v = str(v).encode()
                trpc_req.trans_info[k] = v

        message.set_server_req_head(trpc_req)


@register_plugin(PluginType.CLIENT_PARSER, HTTP, init_pos=PluginInitPos.WORKER)
class ClientHttpParser(BaseClientProtocolParser):
    """HTTP client frame parser.

    HTTP framing is handled by aiohttp, so this parser is a no-op
    plugin registration that occupies the ``http`` parser slot.
    """

    async def read_frame(self, connection: Any) -> bytes:
        """Never called: aiohttp already delivers whole HTTP responses.

        Args:
            connection: Unused.

        Raises:
            NotImplementedError: Always; the HTTP client transport reads
                the response through aiohttp instead of framing bytes.
        """
        raise NotImplementedError("HTTP frames are read by aiohttp, not by ClientHttpParser")


@register_plugin(PluginType.CLIENT_CODEC, HTTP, init_pos=PluginInitPos.WORKER)
class ClientHttpCodec(BaseCodec):
    """Client codec: fill method/caller defaults and decode HTTP errors.

    ``encode_data`` does not build an HTTP packet: it only creates a
    :class:`HttpClientReqHeader` when missing (GET for empty body, else
    POST) and a ``trpc.http.<script>.service`` caller name when the
    caller identity was never set. ``decode_data`` pops the accepted
    status codes from the context.
    """

    def encode_data(self, context: TrpcContext, body: bytes) -> bytes:
        """Fill default method and caller identity when the caller omitted them.

        Args:
            context: Client call context owning the outbound message.
            body: Already-serialized request body.

        Returns:
            ``body`` unchanged.
        """
        message: TrpcMessage = get_message(context)
        req_head = message.client_req_head

        if not req_head:
            method = hdrs.METH_GET if len(body) == 0 else hdrs.METH_POST
            req_head = HttpClientReqHeader(method)
            message.set_client_req_head(req_head)

        if not message.caller_service:
            caller_service = f"trpc.{HTTP}.{os.path.basename(sys.argv[0])}.service"
            message.set_caller_service_name(caller_service)

        return body

    def decode_data(self, context: TrpcContext, _: bytes) -> bytes:
        """Map HTTP status and tRPC error headers onto ``client_rsp_err``.

        Args:
            context: Client call context; the accepted status codes are
                popped from its meta.
            _: Unused; the body lives on ``client_rsp_head``.

        Returns:
            Response body bytes, or ``b''`` for a streamed response.

        Raises:
            ClientInvokeError: ``client_rsp_head`` is missing.
        """
        message: TrpcMessage = get_message(context)
        rsp_header: HttpClientRspHeader = message.client_rsp_head
        if not rsp_header:
            raise ClientInvokeError("no rsp header")

        headers: dict = rsp_header.headers
        encode = headers.get(hdrs.CONTENT_ENCODING, None)
        if encode:
            message.set_compress_type(find_compress_type(encode, 0))

        ret = int(headers.get(HTTP_TRPC_HEADER.trpc_framework_error_code, "0"))
        if ret != 0:
            message.set_client_rsp_err(
                TrpcException(
                    TrpcErrorType.CALLEE_FRAMEWORK,
                    ret,
                    headers.get(HTTP_TRPC_HEADER.trpc_error_message, "callee framework error"),
                ))

        func_ret = int(headers.get(HTTP_TRPC_HEADER.trpc_user_func_error_code, "0"))
        if func_ret != 0:
            message.set_client_rsp_err(
                TrpcException(
                    TrpcErrorType.BUSINESS,
                    func_ret,
                    headers.get(HTTP_TRPC_HEADER.trpc_error_message, "business error"),
                ))

        codec_ok = pop_http_ok_codes(context)
        codec_ok.add(http.HTTPStatus.OK)
        if rsp_header.status_code not in codec_ok:
            message.set_client_rsp_err(
                ClientInvokeError(f"http client codec StatusCode: {rsp_header.status_code}"))

        body = b""
        if not rsp_header.stream:
            body = rsp_header.body

        return body
