# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""tRPC-Python HTTP package: tRPC over aiohttp, plus a plain HTTP client.

Public symbols are re-exported here. Callers should ``from trpc_sdk.http
import ...``.

The package is split by responsibility:

- :mod:`~trpc_sdk.http._codec` — server/client HTTP codec and parsers
- :mod:`~trpc_sdk.http._http_transport` — server listen registration
- :mod:`~trpc_sdk.http._message_handle` — :class:`HttpMessageHandle`, the
  aiohttp-to-tRPC bridge
- :mod:`~trpc_sdk.http._client_transport` — aiohttp client round trip
- :mod:`~trpc_sdk.http._client` — :class:`Client` with naming and filters
- :mod:`~trpc_sdk.http._sse_client` — Server-Sent Events on top of Client
- :mod:`~trpc_sdk.http._sse_event` — :class:`SSEEvent` model
- :mod:`~trpc_sdk.http._sse_parser` — incremental event-stream decoder
- :mod:`~trpc_sdk.http._routes` — ``@route`` and static-file prefixes
- :mod:`~trpc_sdk.http._serialization_form` — form / query-string serializer
- :mod:`~trpc_sdk.http._context` — HTTP request/response accessors on context
- :mod:`~trpc_sdk.http._options` — HTTP client option assembly for invoke
- :mod:`~trpc_sdk.http._ctx_manager` — request context managers
- :mod:`~trpc_sdk.http._types`, :mod:`~trpc_sdk.http._constants` — header
  names, lookup tables, URL parser, protocol constants
"""

from __future__ import annotations

from ._client import Client
from ._client_transport import HttpClientTransport
from ._client_transport import register_http_session_params
from ._codec import ClientHttpCodec
from ._codec import ClientHttpParser
from ._codec import ServerHttpCodec
from ._codec import ServerHttpParser
from ._constants import DEFAULT_SESSION_NAME
from ._constants import HTTP
from ._constants import HTTP_ARG_JSON
from ._constants import HTTP_ARG_SSL
from ._constants import HTTP_ARG_STREAM_KEY
from ._constants import HTTP_ARGS_META_KEY
from ._constants import HTTP_REQ_KEY
from ._constants import HTTP_RSP_HEADER_KEY
from ._constants import HTTP_RSP_KEY
from ._constants import HTTP_RSP_OK_CODE_KEY
from ._constants import HTTPS
from ._constants import LOCAL_ADDR
from ._constants import TRANS_INFO_BASE64_KEY
from ._constants import USER_ROUTE_KEY
from ._context import get_http_header_in_client
from ._context import get_http_ok_codes
from ._context import get_http_request
from ._context import get_http_response
from ._context import get_http_rsp_header
from ._context import get_http_status_in_client
from ._context import pop_http_ok_codes
from ._context import pop_http_rsp_header
from ._context import set_http_ok_codes
from ._context import set_http_rsp_header
from ._ctx_manager import BaseRequestContextManager
from ._ctx_manager import RequestContextManager
from ._exceptions import InvalidURL
from ._http_request import HttpClientReqHeader
from ._http_response import HttpClientRspHeader
from ._http_response import TrpcHttpResponse
from ._http_response import create_rsp_object
from ._http_transport import HttpServerTransport
from ._http_transport import HttpServerTransportFactory
from ._message_handle import HttpDecodeCallback
from ._message_handle import HttpMessageHandle
from ._message_handle import register_user_http_decode_func
from ._options import get_options
from ._routes import HttpRoute
from ._routes import RegisteredRoutes
from ._routes import StaticFileRouters
from ._routes import register_static
from ._routes import route
from ._serialization_form import FormSerializer
from ._sse_client import SSEClient
from ._sse_client import SSEEventSource
from ._sse_event import SSEEvent
from ._types import HTTP_TRPC_HEADER
from ._types import HttpSession
from ._types import HttpTrpcHeader
from ._types import UrlParser
from ._types import find_compress_type
from ._types import find_content_encoding
from ._types import find_content_type
from ._types import find_http_status
from ._types import find_serialization_type
from ._types import get_trpc_error_code
from ._utils import check_url
from ._utils import find_from_dict
from ._utils import get_host
from ._utils import reverse_dict

__all__ = [
    # Codec
    "ServerHttpCodec",
    "ServerHttpParser",
    "ClientHttpCodec",
    "ClientHttpParser",
    # Server transport
    "HttpServerTransport",
    "HttpServerTransportFactory",
    "HttpMessageHandle",
    "HttpDecodeCallback",
    "register_user_http_decode_func",
    # Client transport
    "HttpClientTransport",
    "register_http_session_params",
    "HttpSession",
    # HTTP client
    "Client",
    "check_url",
    "get_host",
    "get_options",
    "BaseRequestContextManager",
    "RequestContextManager",
    "SSEClient",
    "SSEEvent",
    "SSEEventSource",
    # Routes
    "HttpRoute",
    "RegisteredRoutes",
    "StaticFileRouters",
    "route",
    "register_static",
    # Context helpers
    "get_http_request",
    "get_http_response",
    "get_http_header_in_client",
    "get_http_status_in_client",
    "set_http_rsp_header",
    "get_http_rsp_header",
    "pop_http_rsp_header",
    "set_http_ok_codes",
    "get_http_ok_codes",
    "pop_http_ok_codes",
    # Request / response objects
    "HttpClientReqHeader",
    "HttpClientRspHeader",
    "TrpcHttpResponse",
    "create_rsp_object",
    # Constants
    "HTTP",
    "HTTPS",
    "HTTP_REQ_KEY",
    "HTTP_RSP_KEY",
    "HTTP_ARG_JSON",
    "HTTP_ARG_SSL",
    "HTTP_ARG_STREAM_KEY",
    "HTTP_ARGS_META_KEY",
    "TRANS_INFO_BASE64_KEY",
    "USER_ROUTE_KEY",
    "LOCAL_ADDR",
    "HTTP_RSP_HEADER_KEY",
    "HTTP_RSP_OK_CODE_KEY",
    "DEFAULT_SESSION_NAME",
    # Types and lookups
    "HTTP_TRPC_HEADER",
    "HttpTrpcHeader",
    "UrlParser",
    "get_trpc_error_code",
    "find_serialization_type",
    "find_content_type",
    "find_compress_type",
    "find_content_encoding",
    "find_http_status",
    "reverse_dict",
    "find_from_dict",
    # Exceptions
    "InvalidURL",
    # Serialization
    "FormSerializer",
    "dict_to_protobuf",
]
