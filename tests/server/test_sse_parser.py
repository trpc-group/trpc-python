"""Focused tests for the incremental SSE parser."""
from trpc_sdk.http.sse_client import _SSEParser


def test_id_containing_null_is_ignored():
    parser = _SSEParser()
    parser.decode("id: good")
    parser.decode("id: bad\0id")
    parser.decode("data: payload")

    event = parser.decode("")

    assert event.id == "good"
    assert event.data == "payload"


def test_illegal_retry_is_ignored():
    parser = _SSEParser()
    parser.decode("retry: not-a-number")
    parser.decode("data: payload")

    event = parser.decode("")

    assert event.retry is None


def test_cross_chunk_carriage_return_and_incomplete_lines():
    parser = _SSEParser()

    assert parser.decode_line("data: one\r") == []
    assert parser.decode_line("\ndata: tw") == ["data: one"]
    assert parser.decode_line("o\n\n") == ["data: two", ""]


def test_flush_line_clears_incomplete_data_and_trailing_cr():
    parser = _SSEParser()
    assert parser.decode_line("data: final") == []
    assert parser.flush_line() == ["data: final"]
    assert parser.flush_line() == []

    assert parser.decode_line("\r") == []
    assert parser.flush_line() == [""]
