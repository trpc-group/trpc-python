# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Server-Sent Events client."""

from __future__ import annotations

from typing import Any
from typing import AsyncIterator

from ._client import Client
from ._constants import HTTP_ARG_STREAM_KEY
from ._http_response import TrpcHttpResponse
from ._sse_event import SSEEvent
from ._sse_parser import SSEDecoder

#  pylint: disable=too-many-arguments


class SSEEventSource:
    """Event stream view over one streaming :class:`TrpcHttpResponse`."""

    def __init__(self, response: TrpcHttpResponse, encoding: str = "utf-8") -> None:
        """Wrap a response opened in streaming mode.

        Args:
            response: HTTP response whose body is an event stream.
            encoding: Text encoding of the stream body.
        """
        self._response = response
        self._encoding = encoding

    def _ensure_event_stream(self) -> None:
        """Check the response ``Content-Type`` before reading the body.

        Raises:
            TypeError: The media type does not contain ``text/event-stream``.
        """
        content_type = self._response.trpc_rsp.headers.get("content-type", "").partition(";")[0]
        if "text/event-stream" not in content_type:
            raise TypeError("Expected response header Content-Type to contain 'text/event-stream', "
                            f"got {content_type!r}")

    @property
    def response(self) -> TrpcHttpResponse:
        """The underlying streaming HTTP response."""
        return self._response

    async def aiter_sse(self) -> AsyncIterator[SSEEvent]:
        """Yield decoded events, skipping those with empty ``data``.

        Yields:
            Each dispatched :class:`SSEEvent` whose ``data`` is non-empty.

        Raises:
            TypeError: ``Content-Type`` is not ``text/event-stream``.
        """
        self._ensure_event_stream()
        decoder = SSEDecoder()
        async for line in self.aiter_lines(decoder):
            line = line.rstrip("\n")
            event = decoder.feed_line(line)
            if not event or not event.data:
                continue
            yield event

    async def _iter_event_blocks(self) -> AsyncIterator[bytes]:
        """Regroup raw body chunks into blocks that end on a blank line.

        Bytes accumulate until they end with ``\\r\\r``, ``\\n\\n`` or
        ``\\r\\n\\r\\n``; whatever remains at end of body is yielded last.
        ``str`` chunks are encoded with the stream encoding first.

        Yields:
            Byte blocks, line terminators included.
        """
        block = b""
        async for chunk in self._response.stream_data():
            for line in chunk.splitlines(True):
                if isinstance(line, str):
                    line = line.encode(self._encoding)
                block += line
                if block.endswith((b"\r\r", b"\n\n", b"\r\n\r\n")):
                    yield block
                    block = b""
        if block:
            yield block

    async def aiter_lines(self, decoder: SSEDecoder) -> AsyncIterator[str]:
        """Yield complete text lines from the body.

        Args:
            decoder: Decoder whose line buffer carries partial lines
                across blocks and is flushed at end of body.

        Yields:
            Lines without their terminators.
        """
        async for text in self._iter_event_blocks():
            if isinstance(text, bytes):
                text = text.decode(self._encoding)
            for line in decoder.split_lines(text):
                yield line
        for line in decoder.flush_lines():
            yield line


class SSEClient:
    """SSE client that issues streaming requests through tRPC's HTTP client."""

    def __init__(self, name: str = "", encoding: str = "utf-8") -> None:
        """Create the underlying HTTP client.

        Args:
            name: Name passed to :class:`Client` for config lookup.
            encoding: Text encoding of event stream bodies.
        """
        self.http_client = Client(name)
        self._encoding = encoding

    async def sse_iter_events(self,
                              ctx,
                              method,
                              url,
                              options=None,
                              *,
                              data: Any = None,
                              headers: Any = None,
                              **kwargs: Any):
        """Open a streaming request and yield its event source.

        The response stays open while the caller iterates the yielded
        :class:`SSEEventSource` and is closed when this generator resumes.

        Args:
            ctx: tRPC context.
            method: HTTP method.
            url: Target URL.
            options: Request options.
            data: Request body.
            headers: Request headers.
            kwargs: Extra request arguments; the stream flag is forced on.

        Yields:
            One :class:`SSEEventSource` bound to the open response.
        """
        if not kwargs:
            kwargs = {}
        kwargs[HTTP_ARG_STREAM_KEY] = True
        async with self.http_client.request(ctx, method, url, options, data=data, headers=headers,
                                            **kwargs) as response:
            yield SSEEventSource(response, self._encoding)

    async def request_events(self,
                             ctx,
                             method,
                             url,
                             options=None,
                             *,
                             data: Any = None,
                             headers: Any = None,
                             **kwargs: Any) -> AsyncIterator[SSEEvent]:
        """Send a streaming request and yield its parsed events.

        Args:
            ctx: tRPC context.
            method: HTTP method.
            url: Target URL.
            options: Request options.
            data: Request body.
            headers: Request headers.
            kwargs: Extra request arguments; the stream flag is forced on.

        Yields:
            Each :class:`SSEEvent` with non-empty ``data``.

        Example:
            async for event in client.request_events(ctx, "GET", "http://example.com/events"):
                print(event.data)
        """
        if not kwargs:
            kwargs = {}
        kwargs[HTTP_ARG_STREAM_KEY] = True
        async for event_source in self.sse_iter_events(ctx, method, url, options, data=data, headers=headers, **kwargs):
            async for event in event_source.aiter_sse():
                yield event
