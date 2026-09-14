# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""
The SSE parsing code is adapted from the open source library:
https://github.com/florimondmanca/httpx-sse/blob/master/src/httpx_sse/_decoders.py
https://github.com/florimondmanca/httpx-sse/blob/master/src/httpx_sse/_api.py
"""

from __future__ import annotations

import json
from typing import Any
from typing import AsyncIterator
from typing import List
from typing import Optional

from ._client import Client
from ._constants import HTTP_ARG_STREAM_KEY
from ._http_response import TrpcHttpResponse

_NEWLINE_CHARS = "\n\r\x0b\x0c\x1c\x1d\x1e\x85\u2028\u2029"

#  pylint: disable=too-many-arguments


class SSEEvent:
    """Representation of an event from the event stream."""

    def __init__(
        self,
        event: Optional[str] = None,
        data: Optional[str] = None,
        id: Optional[str] = None,  # pylint: disable=redefined-builtin
        retry: Optional[int] = None,
    ) -> None:
        """Initialize an SSE event with optional fields.
        
        Args:
            event: Event type (defaults to "message")
            data: Event data payload
            id: Event ID 
            retry: Reconnection time in milliseconds
        """
        if not event:
            event = "message"

        if data is None:
            data = ""

        if id is None:
            id = ""

        self._event = event
        self._data = data
        self._id = id
        self._retry = retry

    @property
    def event(self) -> str:
        """Get the event type.
        
        Returns:
            The event type string
        """
        return self._event

    @property
    def data(self) -> str:
        """Get the event data.
        
        Returns:
            The event data string
        """
        return self._data

    @property
    def id(self) -> str:
        """Get the event ID.
        
        Returns:
            The event ID string
        """
        return self._id

    @property
    def retry(self) -> Optional[int]:
        """Get the retry interval.
        
        Returns:
            The retry time in milliseconds or None
        """
        return self._retry

    def json(self) -> Any:
        """Parse event data as JSON.
        
        Returns:
            Parsed JSON data from the event payload
            
        Raises:
            json.JSONDecodeError: If data is not valid JSON
        """
        return json.loads(self.data)

    def __repr__(self) -> str:
        """String representation of the SSE event.
        
        Returns:
            Formatted string showing all non-default fields
        """
        pieces = [f"event={self.event!r}"]
        if self.data != "":
            pieces.append(f"data={self.data!r}")
        if self.id != "":
            pieces.append(f"id={self.id!r}")
        if self.retry is not None:
            pieces.append(f"retry={self.retry!r}")
        return f"SSEEvent({', '.join(pieces)})"


class _SSEParser:
    """SSE protocol parser implementation.
    
    Implements the Server-Sent Events specification
    """

    def __init__(self) -> None:
        """Initialize the parser with empty state."""
        self._event = ""
        self._data: List[str] = []
        self._last_event_id = ""
        self._retry: Optional[int] = None
        self.buffer: List[str] = []
        self.trailing_cr: bool = False

    def decode(self, line: str) -> Optional[SSEEvent]:
        """Parse a single line of SSE input.
        
        Args:
            line: A single line from the SSE stream
            
        Returns:
            SSEEvent if a complete event was parsed, None otherwise
            
        Note:
            Follows the SSE spec for event stream interpretation
        """
        if not line:
            if (not self._event and not self._data and not self._last_event_id and self._retry is None):
                return None

            sse = SSEEvent(
                event=self._event,
                data="\n".join(self._data),
                id=self._last_event_id,
                retry=self._retry,
            )

            # Reset state except last_event_id as per spec
            self._event = ""
            self._data = []
            self._retry = None

            return sse

        if line.startswith(":"):
            return None

        fieldname, _, value = line.partition(":")

        if value.startswith(" "):
            value = value[1:]

        if fieldname == "event":
            self._event = value
        elif fieldname == "data":
            self._data.append(value)
        elif fieldname == "id":
            if "\0" in value:
                pass
            else:
                self._last_event_id = value
        elif fieldname == "retry":
            try:
                self._retry = int(value)
            except (TypeError, ValueError):
                pass
        else:
            pass  # Ignore unknown fields

        return None

    def decode_line(self, text: str) -> List[str]:
        """Split input text into lines for parsing.
        
        Args:
            text: Raw input text from the stream
            
        Returns:
            List of properly delimited lines
            
        Note:
            Handles various newline conventions and buffering
        """

        # Handle trailing CR from previous chunk
        if self.trailing_cr:
            text = "\r" + text
            self.trailing_cr = False
        if text.endswith("\r"):
            self.trailing_cr = True
            text = text[:-1]

        if not text:
            return []

        trailing_newline = text[-1] in _NEWLINE_CHARS
        lines = text.splitlines()

        if len(lines) == 1 and not trailing_newline:
            # Buffer incomplete line
            self.buffer.append(lines[0])
            return []

        if self.buffer:
            # Prepend buffered data to first line
            lines = ["".join(self.buffer) + lines[0]] + lines[1:]
            self.buffer = []

        if not trailing_newline:
            # Buffer incomplete final line
            self.buffer = [lines.pop()]

        return lines

    def flush_line(self) -> List[str]:
        """Flush any buffered lines.
        
        Returns:
            Remaining buffered lines
            
        Note:
            Should be called at end of stream to process final data
        """
        if not self.buffer and not self.trailing_cr:
            return []

        lines = ["".join(self.buffer)]
        self.buffer = []
        self.trailing_cr = False
        return lines


class SSEEventSource:
    """Represents a source of SSE events from an HTTP response."""

    def __init__(self, response: TrpcHttpResponse, encoding: str = "utf-8") -> None:
        """Initialize with an HTTP response.

        Args:
            response: HTTP response object with event stream
        """
        self._response = response
        self._encoding = encoding

    def _check_content_type(self) -> None:
        """Verify response has correct Content-Type header.
        
        Raises:
            TypeError: If Content-Type is not text/event-stream
        """
        content_type = self._response.trpc_rsp.headers.get("content-type", "").partition(";")[0]
        if "text/event-stream" not in content_type:
            raise TypeError("Expected response header Content-Type to contain 'text/event-stream', "
                            f"got {content_type!r}")

    @property
    def response(self) -> TrpcHttpResponse:
        """Get the underlying HTTP response.
        
        Returns:
            The HTTP response object
        """
        return self._response

    async def aiter_sse(self) -> AsyncIterator[SSEEvent]:
        """Asynchronously iterate through parsed SSE events.
        
        Returns:
            Async iterator yielding SSEEvent objects
            
        Raises:
            TypeError: If Content-Type validation fails
        """
        self._check_content_type()
        decoder = _SSEParser()
        async for line in self.aiter_lines(decoder):
            line = line.rstrip("\n")
            sse = decoder.decode(line)
            if not sse or not sse.data:
                continue
            yield sse

    async def _read_complete_line(self) -> AsyncIterator[bytes]:
        """
        Read a complete line of data from the stream.
        
        Args:
            stream: The input stream to read from
            
        Returns:
            bytes: A complete line of text read from the stream,
                 with line ending characters removed
                 
        Raises:
            IOError: If an I/O error occurs while reading
            EOFError: If end of stream is reached before reading a complete line
        """
        data = b""
        async for chunk in self._response.stream_data():
            for line in chunk.splitlines(True):
                if isinstance(line, str):
                    line = line.encode(self._encoding)
                data += line
                if data.endswith((b"\r\r", b"\n\n", b"\r\n\r\n")):
                    yield data
                    data = b""
        if data:
            yield data

    async def aiter_lines(self, decoder: _SSEParser) -> AsyncIterator[str]:
        """Asynchronously iterate through raw lines from the stream.
        
        Args:
            decoder: SSE parser instance
            
        Returns:
            Async iterator yielding individual lines
        """
        async for text in self._read_complete_line():
            if isinstance(text, bytes):
                text = text.decode(self._encoding)
            for line in decoder.decode_line(text):
                yield line
        for line in decoder.flush_line():
            yield line


class SSEClient:
    """SSE client implementation using tRPC's HTTP client."""

    def __init__(self, name: str = "", encoding: str = "utf-8") -> None:
        """Initialize the SSE client.
        
        Args:
            name: Optional name for the underlying HTTP client
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
        """Get an async iterator of SSE event sources.
        
        Args:
            ctx: TRPC context
            method: HTTP method
            url: Target URL
            options: Request options
            data: Request body data
            headers: Request headers
            kwargs: Additional arguments
            
        Returns:
            Async iterator yielding SSEEventSource objects
            
        Note:
            Automatically sets streaming flag in kwargs
        """
        if not kwargs:
            kwargs = {}
        kwargs[HTTP_ARG_STREAM_KEY] = True
        async with self.http_client.request(ctx, method, url, options, data=data, headers=headers, **kwargs) as rsp:
            yield SSEEventSource(rsp, self._encoding)

    async def request_events(self,
                             ctx,
                             method,
                             url,
                             options=None,
                             *,
                             data: Any = None,
                             headers: Any = None,
                             **kwargs: Any) -> AsyncIterator[SSEEvent]:
        """Convenience method to directly iterate through parsed events.
        
        Args:
            ctx: TRPC context
            method: HTTP method
            url: Target URL
            options: Request options
            data: Request body data
            headers: Request headers
            kwargs: Additional arguments
            
        Returns:
            Async iterator yielding parsed SSEEvent objects
            
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
