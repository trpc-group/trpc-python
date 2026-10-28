# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.
"""Focused tests for the incremental SSE parser."""

from trpc_sdk.http._sse_parser import SSEDecoder


def test_id_containing_null_is_ignored():
    parser = SSEDecoder()
    parser.feed_line("id: good")
    parser.feed_line("id: bad\0id")
    parser.feed_line("data: payload")

    event = parser.feed_line("")

    assert event.id == "good"
    assert event.data == "payload"


def test_illegal_retry_is_ignored():
    parser = SSEDecoder()
    parser.feed_line("retry: not-a-number")
    parser.feed_line("data: payload")

    event = parser.feed_line("")

    assert event.retry is None
    assert event.data == "payload"


def test_cross_chunk_carriage_return_and_incomplete_lines():
    parser = SSEDecoder()

    assert parser.split_lines("data: one\r") == []
    assert parser.split_lines("\ndata: tw") == ["data: one"]
    assert parser.split_lines("o\n\n") == ["data: two", ""]


def test_flush_line_clears_incomplete_data_and_trailing_cr():
    parser = SSEDecoder()
    assert parser.split_lines("data: final") == []
    assert parser.flush_lines() == ["data: final"]
    assert parser.flush_lines() == []

    assert parser.split_lines("\r") == []
    assert parser.flush_lines() == [""]
