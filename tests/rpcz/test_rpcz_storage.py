# -*- coding: utf-8 -*-
from types import SimpleNamespace

from trpc_sdk.rpcz.span import Span
from trpc_sdk.rpcz.span import SpanType
from trpc_sdk.rpcz.storage import RPCZ_SERVER_REQ_SIZE_CTX_KEY
from trpc_sdk.rpcz.storage import RPCZ_SERVER_RSP_SIZE_CTX_KEY
from trpc_sdk.rpcz.storage import RPCZ_SERVER_SPAN_CTX_KEY
from trpc_sdk.rpcz.storage import pop_all_spans
from trpc_sdk.rpcz.storage import pop_sub_spans
from trpc_sdk.rpcz.storage import prepare_server_span_for_dispatch
from trpc_sdk.rpcz.storage import submit_server_span_on_dispatcher_post_io
from trpc_sdk.rpcz.storage import submit_server_span_on_executor_post_io
from trpc_sdk.rpcz.storage import submit_span
from trpc_sdk.rpcz.storage import submit_sub_span


class _Ctx:
    def __init__(self):
        self.meta_data = {}
        self.server_post_sched_recv_msg_us = 101
        self.server_pre_sched_send_msg_us = 102
        self.server_post_sched_send_msg_us = 103
        self.server_pre_io_send_msg_us = 104
        self.server_post_io_send_msg_us = 105

    def with_meta_data(self, key, value):
        self.meta_data[key] = value

    def get_meta_data(self, key, default=None):
        return self.meta_data.get(key, default)


def _reset_storage_state():
    # module-level containers are used by design; reset for test isolation
    from trpc_sdk.rpcz import storage as storage_mod
    storage_mod._spans.clear()      # pylint: disable=protected-access
    storage_mod._sub_spans.clear()  # pylint: disable=protected-access


def test_submit_and_pop_spans():
    _reset_storage_state()
    s1 = Span(span_id=1, span_type=SpanType.SERVER)
    s2 = Span(span_id=2, span_type=SpanType.CLIENT)
    submit_span(s1)
    submit_span(s2)
    all_spans = pop_all_spans()
    assert {s.span_id for s in all_spans} == {1, 2}
    assert pop_all_spans() == []


def test_submit_server_span_on_executor_post_io_merge_size_and_subspans(monkeypatch):
    _reset_storage_state()
    monkeypatch.setattr(
        "trpc_sdk.rpcz.storage.config",
        SimpleNamespace(
            global_config_obj=SimpleNamespace(
                global_config=SimpleNamespace(
                    rpcz=SimpleNamespace(sample_minimum_timecost=0),
                ),
            ),
        ),
    )

    server_span = Span(
        span_id=100,
        span_type=SpanType.SERVER,
        server_pre_sched_recv_msg_us=10,
        server_post_io_send_msg_us=200,
    )
    child_span = Span(span_id=200, span_type=SpanType.CLIENT)
    submit_sub_span(server_span.span_id, child_span)

    ctx = _Ctx()
    ctx.with_meta_data(RPCZ_SERVER_SPAN_CTX_KEY, server_span)
    ctx.with_meta_data(RPCZ_SERVER_REQ_SIZE_CTX_KEY, 1234)
    ctx.with_meta_data(RPCZ_SERVER_RSP_SIZE_CTX_KEY, 5678)

    out = submit_server_span_on_executor_post_io(ctx)
    assert out is not None
    assert out.request_size == 1234
    assert out.response_size == 5678
    assert len(out.sub_spans) == 1
    assert out.sub_spans[0].span_id == 200
    assert pop_sub_spans(server_span.span_id) == []


def test_prepare_server_span_for_dispatch_merge_size_and_subspans():
    _reset_storage_state()
    server_span = Span(
        span_id=300,
        span_type=SpanType.SERVER,
    )
    submit_sub_span(server_span.span_id, Span(span_id=301, span_type=SpanType.CLIENT))

    ctx = _Ctx()
    ctx.with_meta_data(RPCZ_SERVER_SPAN_CTX_KEY, server_span)
    ctx.with_meta_data(RPCZ_SERVER_REQ_SIZE_CTX_KEY, 66)
    ctx.with_meta_data(RPCZ_SERVER_RSP_SIZE_CTX_KEY, 77)

    out = prepare_server_span_for_dispatch(ctx)
    assert out is not None
    assert out.request_size == 66
    assert out.response_size == 77
    assert len(out.sub_spans) == 1
    assert out.sub_spans[0].span_id == 301


def test_submit_server_span_on_dispatcher_post_io_minimum_cost_filter(monkeypatch):
    _reset_storage_state()
    monkeypatch.setattr(
        "trpc_sdk.rpcz.storage.config",
        SimpleNamespace(
            global_config_obj=SimpleNamespace(
                global_config=SimpleNamespace(
                    rpcz=SimpleNamespace(sample_minimum_timecost=1000),
                ),
            ),
        ),
    )
    span = Span(
        span_id=400,
        span_type=SpanType.SERVER,
        server_pre_sched_recv_msg_us=100,
    )
    out = submit_server_span_on_dispatcher_post_io(
        span=span,
        post_sched_send_us=120,
        pre_io_send_us=121,
        post_io_send_us=122,
    )
    assert out is None
