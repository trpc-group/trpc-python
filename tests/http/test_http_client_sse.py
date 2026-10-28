# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.
"""Test cases for HTTP SSE (Server-Sent Events) client functionality"""

from unittest.mock import MagicMock

import pytest

from trpc_sdk.http import SSEEvent
from trpc_sdk.http import SSEEventSource
from trpc_sdk.http._sse_parser import SSEDecoder

# pylint: disable=protected-access
# pylint: disable=invalid-name


def test_sse_event_initialization():
    """Test SSEEvent initialization with all parameters
    
    Verifies:
    - All fields are correctly set during initialization
    """
    event = SSEEvent(event="test", data="data", id="123", retry=1000)
    assert event.event == "test"
    assert event.data == "data"
    assert event.id == "123"
    assert event.retry == 1000


def test_sse_event_default_values():
    """Test SSEEvent initialization with default values
    
    Verifies:
    - Default values are correctly set when no args provided
    """
    event = SSEEvent()
    assert event.event == "message"  # Default event type
    assert event.data == ""  # Empty data by default
    assert event.id == ""  # Empty id by default
    assert event.retry is None  # None retry by default


def test_sse_event_json():
    """Test SSEEvent json parsing
    
    Verifies:
    - Correctly parses JSON data payload
    """
    event = SSEEvent(data='{"key": "value"}')
    assert event.json() == {"key": "value"}


def test_sse_event_repr():
    """Test SSEEvent string representation
    
    Verifies:
    - __repr__ shows all fields correctly
    """
    event = SSEEvent(event="test", data="data", id="1", retry=1000)
    assert repr(event) == "SSEEvent(event='test', data='data', id='1', retry=1000)"


def test_sse_parser_decode_empty_line():
    """Test SSE parser with empty line
    
    Verifies:
    - Returns None for empty lines (event separator)
    """
    parser = SSEDecoder()
    assert parser.feed_line("") is None


def test_sse_parser_decode_comment_line():
    """Test SSE parser with comment line
    
    Verifies:
    - Returns None for comment lines
    """
    parser = SSEDecoder()
    assert parser.feed_line(":comment") is None


def test_sse_parser_decode_event_field():
    """Test SSE parser event field parsing
    
    Verifies:
    - Correctly extracts event type
    """
    parser = SSEDecoder()
    parser.feed_line("event:test")
    assert parser._event_type == "test"


def test_sse_parser_decode_multiple_data():
    """Test SSE parser with multiple data fields
    
    Verifies:
    - Correctly combines multiple data lines
    """
    parser = SSEDecoder()
    parser.feed_line("data:line1")
    parser.feed_line("data:line2")
    assert parser._data_lines == ["line1", "line2"]


def test_sse_parser_decode_complete_event():
    """Test SSE parser with complete event
    
    Verifies:
    - Correctly parses all SSE fields
    - Returns complete event object
    """
    parser = SSEDecoder()
    parser.feed_line("event:test")
    parser.feed_line("data:test data")
    parser.feed_line("id:123")
    parser.feed_line("retry:1000")
    event = parser.feed_line("")
    assert event.event == "test"
    assert event.data == "test data"
    assert event.id == "123"
    assert event.retry == 1000


@pytest.mark.asyncio
async def test_sse_event_source_aiter_sse():
    """Test async iteration over SSE events
    
    Verifies:
    - Correctly iterates through mock event stream
    - Properly parses event data
    """
    mock_response = MagicMock()
    mock_response.trpc_rsp.headers = {"content-type": "text/event-stream"}

    async def _chunks():
        yield "event:test\ndata:test data\n\n"
        yield b"event:test\ndata:test data\n\n"

    mock_response.stream_data.return_value = _chunks()

    event_source = SSEEventSource(mock_response)
    async for event in event_source.aiter_sse():
        assert event.event == "test"
        assert event.data == "test data"


def test_sse_event_source_check_content_type_fail():
    """Test content type validation failure
    
    Verifies:
    - Raises TypeError for invalid content types
    """
    mock_response = MagicMock()
    mock_response.trpc_rsp.headers = {"content-type": "application/json"}
    source = SSEEventSource(mock_response)
    with pytest.raises(TypeError):
        source._ensure_event_stream()
