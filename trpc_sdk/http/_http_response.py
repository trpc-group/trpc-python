# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""HTTP client response wrappers.

:class:`HttpClientRspHeader` is the raw status/headers/body the
transport stores on ``Message.client_rsp_head``. :class:`TrpcHttpResponse`
is the user-facing wrapper returned by :class:`~trpc_sdk.http.Client`.
"""

from __future__ import annotations

from typing import Optional
from typing import Type
from typing import Union

from aiohttp import ClientResponse
from aiohttp.web_request import CIMultiDictProxy
from google.protobuf.message import Message as PbMessage


class HttpClientRspHeader:
    """Raw HTTP response fields stored on ``Message.client_rsp_head``.

    When ``stream`` is ``True``, ``body`` is the live aiohttp
    ``ClientResponse`` (the transport has not read it). Otherwise
    ``body`` is already-consumed ``bytes``.
    """

    def __init__(
            self,
            code: int,
            headers: CIMultiDictProxy,
            body: Union[ClientResponse, bytes],
            stream: bool = False,
    ) -> None:
        self.status_code = code
        self.headers = headers  # aiohttp CIMultiDictProxy; lookups are case-insensitive
        self.body = body  # live ClientResponse when stream, else already-consumed bytes
        self.stream = stream

    def __str__(self) -> str:
        return f"""
            status_code: {self.status_code},
            headers: {self.headers},
            body: {repr(self.body)},
            stream: {self.stream},
        """


class TrpcHttpResponse:
    """User-facing HTTP response returned by :class:`~trpc_sdk.http.Client`.

    Parsing helpers (``json`` / ``pb`` / ``text`` / ``stream_data``)
    delegate to the wrapped :class:`HttpClientRspHeader`. :meth:`release`
    closes the underlying aiohttp response once, which the request
    context manager calls on ``__aexit__``.
    """

    def __init__(self, rsp: Optional[HttpClientRspHeader] = None) -> None:
        self.closed = False
        self.trpc_rsp = rsp

    @property
    def status(self):
        """HTTP status code from the wrapped response."""
        return self.trpc_rsp.status_code

    async def json(self):
        """Parse the body as JSON via aiohttp.

        Returns:
            The decoded JSON object.
        """
        return await self.trpc_rsp.body.json()

    async def pb(self, rsp: PbMessage):
        """Parse the body into ``rsp`` with ``ParseFromString``.

        Args:
            rsp: Protobuf message instance to fill.

        Returns:
            ``rsp`` after ``ParseFromString``.
        """
        content = await self.trpc_rsp.body.read()
        return rsp.ParseFromString(content)

    async def stream_data(self):
        """Yield raw chunks from the streamed response body.

        Yields:
            Chunks from ``body.content.readany()`` until the stream ends.
        """
        while True:
            chunk = await self.trpc_rsp.body.content.readany()
            if not chunk:
                break
            yield chunk

    async def stream_pb(self, rsp_cls: Type[PbMessage]):
        """Yield one protobuf message per streamed chunk.

        Args:
            rsp_cls: Protobuf message class used to decode each chunk.

        Yields:
            Instances of ``rsp_cls`` parsed from each chunk.
        """
        while True:
            chunk = await self.trpc_rsp.body.content.readany()
            if not chunk:
                break
            rsp = rsp_cls()
            rsp.ParseFromString(chunk)
            yield rsp

    async def text(self):
        """Decode the body as text via aiohttp.

        Returns:
            The decoded text.
        """
        return await self.trpc_rsp.body.text()

    @property
    def header(self):
        """Response headers from the wrapped response."""
        return self.trpc_rsp.headers

    @property
    def content(self):
        """Raw aiohttp content stream of the wrapped body."""
        return self.trpc_rsp.body.content

    @property
    def body(self):
        """Decoded body when not streaming; the raw body otherwise.

        Returns:
            Decoded ``str`` when ``stream`` is ``False`` and ``body`` is
            ``bytes``; otherwise the wrapped body object.
        """
        if not self.trpc_rsp.stream:
            return self.trpc_rsp.body.decode()
        return self.trpc_rsp.body

    async def read(self):
        """Read the remaining body bytes via aiohttp.

        Returns:
            Remaining body as ``bytes``.
        """
        return await self.trpc_rsp.body.read()

    def release(self) -> None:
        """Close the underlying aiohttp response if still open."""
        if self.closed:
            return
        self.trpc_rsp.body.close()
        self.closed = True


def create_rsp_object():
    """Return the empty response placeholder passed to ``Client.invoke``.

    HTTP responses are filled by the transport onto
    ``Message.client_rsp_head``, so the invoke path only needs a
    no-op factory here.

    Returns:
        ``None``.
    """
    return None
