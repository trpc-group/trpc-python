# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Shared HTTP records: header names, URL parsing, session and lookups.

This module holds the data types that codec, client transport and
form serialisation all need:

- :class:`HttpTrpcHeader` / :data:`HTTP_TRPC_HEADER` — tRPC-over-HTTP
  header names
- the content-type / compress / status lookup tables and the
  ``find_*`` helpers bound to them
- :class:`UrlParser` — scheme-preserving URL parse used by the HTTP
  client
- :class:`HttpSession` — thread-local aiohttp session holder
- :data:`TYPE_CALLABLE_MAP` — protobuf field type to Python callable
"""

from __future__ import annotations

import http
import threading
from dataclasses import dataclass
from functools import partial
from typing import Dict
from typing import Optional
from urllib.parse import ParseResult
from urllib.parse import urlparse
from urllib.parse import urlunparse

import six
from aiohttp import ClientSession
from aiohttp import hdrs
from google.protobuf.descriptor import FieldDescriptor

from trpc_sdk.compressor import CompressType
from trpc_sdk.exceptions import TrpcErrorCode
from trpc_sdk.exceptions import TrpcErrorType
from trpc_sdk.serialization import SerializationType
from trpc_sdk.utils import strtobool
from ._exceptions import InvalidURL
from ._utils import find_from_dict
from ._utils import reverse_dict


class HttpSession(threading.local):
    """Thread-local holder for one aiohttp ``ClientSession``.

    aiohttp sessions are bound to a single event loop. Storing them on
    a :class:`threading.local` keeps workers that share a process but
    not a loop from borrowing each other's session.
    """

    def __init__(self) -> None:
        self.instance: Optional[ClientSession] = None


@dataclass
class HttpTrpcHeader:
    """tRPC-over-HTTP header names.

    Each field is the wire name written onto (or read from) an HTTP
    request or response. ``trpc-ret`` is the framework error code;
    ``trpc-func-ret`` is the user-function error code.
    """

    trpc_version: str = "trpc-version"
    trpc_call_type: str = "trpc-call-type"
    trpc_message_type: str = "trpc-message-type"
    trpc_request_id: str = "trpc-request-id"
    trpc_timeout: str = "trpc-timeout"
    trpc_caller: str = "trpc-caller"
    trpc_caller_method: str = "trpc-caller-method"
    trpc_callee: str = "trpc-callee"
    trpc_trans_info: str = "trpc-trans-info"
    trpc_env: str = "trpc-env"
    trpc_dyeing_key: str = "trpc-dyeing-key"
    # Error message from user code or the framework (for example validation).
    trpc_error_message: str = "trpc-error-msg"
    # Error code reported by the framework.
    trpc_framework_error_code: str = "trpc-ret"
    # Error code reported by the user function.
    trpc_user_func_error_code: str = "trpc-func-ret"
    # Connection is used to set whether connect mode is "Connection".
    connection: str = "Connection"


HTTP_TRPC_HEADER = HttpTrpcHeader()

_trpc_err_type_to_http_header: Dict[int, str] = {
    TrpcErrorType.FRAMEWORK: HTTP_TRPC_HEADER.trpc_framework_error_code,
    TrpcErrorType.BUSINESS: HTTP_TRPC_HEADER.trpc_user_func_error_code,
}

get_trpc_error_code = partial(find_from_dict, _trpc_err_type_to_http_header)

_content_type_to_serialization_type: Dict[str, SerializationType] = {
    "application/json": SerializationType.JSON,
    "application/protobuf": SerializationType.PB,
    "application/x-protobuf": SerializationType.PB,
    "application/pb": SerializationType.PB,
    "application/proto": SerializationType.PB,
    "application/jce": SerializationType.JCE,
    "application/flatbuffer": SerializationType.FlatBuffer,
    "application/octet-stream": SerializationType.Noop,
    "application/x-www-form-urlencoded": SerializationType.Form,
}
_serialization_type_to_content_type = reverse_dict(_content_type_to_serialization_type)

find_serialization_type = partial(find_from_dict, _content_type_to_serialization_type)
find_content_type = partial(find_from_dict, _serialization_type_to_content_type)

_content_encoding_to_compress_type: Dict[str, int] = {"gzip": CompressType.Gzip}
_compress_type_to_content_encoding = reverse_dict(_content_encoding_to_compress_type)

find_compress_type = partial(find_from_dict, _content_encoding_to_compress_type)
find_content_encoding = partial(find_from_dict, _compress_type_to_content_encoding)

_trpc_err_code_to_http_status: Dict[int, http.HTTPStatus] = {
    TrpcErrorCode.TRPC_INVOKE_SUCCESS: http.HTTPStatus.OK,
    TrpcErrorCode.TRPC_SERVER_DECODE_ERR: http.HTTPStatus.BAD_REQUEST,
    TrpcErrorCode.TRPC_SERVER_ENCODE_ERR: http.HTTPStatus.INTERNAL_SERVER_ERROR,
    TrpcErrorCode.TRPC_SERVER_NOFUNC_ERR: http.HTTPStatus.NOT_FOUND,
    TrpcErrorCode.TRPC_SERVER_NOSERVICE_ERR: http.HTTPStatus.NOT_FOUND,
    TrpcErrorCode.TRPC_SERVER_TIMEOUT_ERR: http.HTTPStatus.GATEWAY_TIMEOUT,
    TrpcErrorCode.TRPC_SERVER_OVERLOAD_ERR: http.HTTPStatus.TOO_MANY_REQUESTS,
    TrpcErrorCode.TRPC_SERVER_SYSTEM_ERR: http.HTTPStatus.INTERNAL_SERVER_ERROR,
    TrpcErrorCode.TRPC_SERVER_AUTH_ERR: http.HTTPStatus.NON_AUTHORITATIVE_INFORMATION,
    TrpcErrorCode.TRPC_SERVER_VALIDATE_ERR: http.HTTPStatus.BAD_REQUEST,
    TrpcErrorCode.TRPC_INVOKE_UNKNOWN_ERR: http.HTTPStatus.INTERNAL_SERVER_ERROR,
}

find_http_status = partial(find_from_dict, _trpc_err_code_to_http_status)

TYPE_CALLABLE_MAP = {
    FieldDescriptor.TYPE_DOUBLE: float,
    FieldDescriptor.TYPE_FLOAT: float,
    FieldDescriptor.TYPE_INT32: int,
    FieldDescriptor.TYPE_INT64: int if six.PY3 else six.integer_types[1],
    FieldDescriptor.TYPE_UINT32: int,
    FieldDescriptor.TYPE_UINT64: int if six.PY3 else six.integer_types[1],
    FieldDescriptor.TYPE_SINT32: int,
    FieldDescriptor.TYPE_SINT64: int if six.PY3 else six.integer_types[1],
    FieldDescriptor.TYPE_FIXED32: int,
    FieldDescriptor.TYPE_FIXED64: int if six.PY3 else six.integer_types[1],
    FieldDescriptor.TYPE_SFIXED32: int,
    FieldDescriptor.TYPE_SFIXED64: int if six.PY3 else six.integer_types[1],
    FieldDescriptor.TYPE_BOOL: strtobool,
    FieldDescriptor.TYPE_STRING: six.text_type,
    FieldDescriptor.TYPE_BYTES: six.binary_type,
    FieldDescriptor.TYPE_ENUM: int,
}


class UrlParser:
    """Parse a URL while preserving non-http schemes.

    ``urllib.parse.urlparse`` drops ``netloc`` for unknown schemes such
    as ``polaris://``. This parser rewrites those to ``http://`` just
    long enough to extract host/path/query, then restores the original
    scheme so :meth:`get_http_url` can later swap in a resolved
    ``ip:port``.
    """

    def __init__(self, url: str = "", result: Optional[ParseResult] = None) -> None:
        self.url = url
        self.result: Optional[ParseResult] = result

    def parse(self) -> ParseResult:
        """Parse ``self.url``, falling back to a custom scheme rewrite.

        Returns:
            The parsed result, also stored on ``self.result``.

        Raises:
            InvalidURL: The URL is missing ``://``, has more than one
                ``://``, or has an empty scheme.
        """
        result = urlparse(self.url)

        if not result.scheme or not result.netloc:
            if "://" not in self.url:
                raise InvalidURL(f"invalid url format: {self.url}, missing '://'")

            separator_count = self.url.count("://")
            if separator_count != 1:
                raise InvalidURL(f"invalid url format: {self.url}, multiple '://' found")

            parts = self.url.split("://", 1)
            original_scheme = parts[0]
            rest_of_url = parts[1]

            if not original_scheme:
                raise InvalidURL(f"invalid url format: {self.url}, empty scheme")

            temp_url = f"http://{rest_of_url}"
            result = urlparse(temp_url)
            result = result._replace(scheme=original_scheme)
        self.result = result
        return result

    def check_url(self) -> bool:
        """Return whether scheme and netloc are both non-empty.

        Returns:
            ``True`` when both fields are set.
        """
        return self.result.scheme != "" and self.result.netloc != ""

    def get_host(self) -> str:
        """Return the parsed netloc (host[:port]).

        Returns:
            The netloc string from ``self.result``.
        """
        return self.result.netloc

    def get_http_url(self, headers: dict, addr: Optional[str] = None) -> str:
        """Build an HTTP URL, optionally replacing netloc with ``addr``.

        When ``addr`` is omitted the original URL is returned and ``Host``
        is filled from the parsed netloc if missing. When ``addr`` is
        given, scheme is forced to ``http`` and netloc becomes ``addr``
        (the selector-resolved ``ip:port``).

        Args:
            headers: Request header dict; may be mutated to set ``Host``.
            addr: Optional ``ip:port`` that replaces the logical netloc.

        Returns:
            An HTTP URL string aiohttp can request.

        Raises:
            InvalidURL: :meth:`parse` has not been called yet.

        Example:
            >>> parser = UrlParser('polaris://trpc.a.b.c/a/b/c?a=1')
            >>> parser.parse()
            >>> parser.get_http_url({}, '10.10.10.10:1123')
            'http://10.10.10.10:1123/a/b/c?a=1'
        """
        if self.result is None:
            raise InvalidURL("URL not parsed yet, call parse() first")
        if not addr:
            if hdrs.HOST not in headers:
                headers[hdrs.HOST] = self.result.netloc
            return self.url or urlunparse(self.result)

        if hdrs.HOST not in headers:
            headers[hdrs.HOST] = addr

        http_result = self.result._replace(scheme="http", netloc=addr)
        return urlunparse(http_result)
