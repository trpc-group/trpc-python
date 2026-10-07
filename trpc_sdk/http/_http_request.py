# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""HTTP client request-header record.

:class:`HttpClientReqHeader` is the object stored on
``TrpcMessage.client_req_head``. The HTTP client fills it before invoke;
:class:`~trpc_sdk.http.HttpClientTransport` reads method, headers, stream
flag and the already-parsed URL from it.
"""

from __future__ import annotations

from typing import Optional
from urllib.parse import ParseResult

from aiohttp import hdrs

from ._constants import HTTP
from ._types import UrlParser


class HttpClientReqHeader:
    """HTTP request fields carried on a tRPC ``TrpcMessage``.

    ``url_parser`` is the source of truth for scheme and ``parsed_url``
    when present: :meth:`_full` copies them onto this object so the
    transport does not have to re-parse.
    """

    def __init__(
            self,
            method: str = hdrs.METH_GET,
            host: str = "",
            headers: Optional[dict] = None,
            stream: bool = False,
            scheme: str = HTTP,
            parsed_url: Optional[ParseResult] = None,
            url_parser: Optional[UrlParser] = None,
    ) -> None:
        self.method = method
        self.host = host  # not read by transport; Host is filled from parsed_url / selector addr
        self.headers = headers or {}
        self.stream = stream  # False: transport consumes the body; True: body stays a live ClientResponse
        self.scheme = scheme
        self.parsed_url = parsed_url  # copied from url_parser by _full when the caller omitted it
        self.url_parser = url_parser  # source of truth for scheme and parsed_url
        self._full()

    def _full(self) -> None:
        """Copy scheme and parsed URL from ``url_parser`` when set."""
        if not self.url_parser:
            return
        result = self.url_parser.result
        if not self.parsed_url:
            self.parsed_url = result
        if result.scheme:
            self.scheme = result.scheme

    def __str__(self) -> str:
        return f"""
            method: {self.method},
            host: {self.host},
            headers: {self.headers},
            stream: {self.stream},
            scheme: {self.scheme},
            parsed_url: {self.parsed_url}
        """
