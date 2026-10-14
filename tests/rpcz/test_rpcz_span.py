# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

from trpc_sdk.rpcz.span import Span
from trpc_sdk.rpcz.span import SpanType


def test_span_to_dict_server_filters_client_fields_and_timeout():
    span = Span(
        span_id=1,
        span_type=SpanType.SERVER,
        timeout_ms=123,
        client_pre_rpc_invoke_us=11,
        server_pre_sched_recv_msg_us=22,
    )
    data = span.to_dict()
    assert "timeout_ms" not in data
    assert "client_pre_rpc_invoke_us" not in data
    assert "server_pre_sched_recv_msg_us" in data


def test_span_to_dict_client_filters_server_fields_and_timeout():
    span = Span(
        span_id=2,
        span_type=SpanType.CLIENT,
        timeout_ms=123,
        server_pre_sched_recv_msg_us=11,
        client_pre_rpc_invoke_us=22,
    )
    data = span.to_dict()
    assert "timeout_ms" not in data
    assert "server_pre_sched_recv_msg_us" not in data
    assert "client_pre_rpc_invoke_us" in data


def test_span_from_dict_recursively_builds_sub_spans():
    payload = {
        "span_id": 10,
        "span_type": "server",
        "sub_spans": [
            {"span_id": 11, "span_type": "client", "full_method_name": "m1"},
            {"span_id": 12, "span_type": "client", "full_method_name": "m2"},
        ],
    }
    span = Span.from_dict(payload)
    assert len(span.sub_spans) == 2
    assert all(isinstance(item, Span) for item in span.sub_spans)
    assert span.sub_spans[0].span_type == SpanType.CLIENT


def test_span_to_dict_tolerates_dict_type_sub_span():
    span = Span(span_id=1, span_type=SpanType.SERVER)
    # historical/compat mixed payload
    span.sub_spans = [{"span_id": 2, "span_type": "client"}]
    data = span.to_dict()
    assert isinstance(data["sub_spans"], list)
    assert data["sub_spans"][0]["span_type"] == "client"
