# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from trpc_sdk.rpcz.filter.client_filter import RpczClientFilter, RPCZ_CLIENT_SPAN_CTX_KEY
from trpc_sdk.rpcz.storage import RPCZ_CLIENT_REQ_SIZE_CTX_KEY, RPCZ_CLIENT_RSP_SIZE_CTX_KEY, RPCZ_SERVER_SPAN_CTX_KEY
from trpc_sdk.rpcz.span import Span, SpanType
from trpc_sdk.rpcz.filter import client_filter as filter_mod


class _Ctx:
    def __init__(self):
        self.meta_data = {}
        self.timeout = 1000
        self.client_post_sched_send_msg_us = 111
        self.client_pre_io_send_msg_us = 112
        self.client_post_io_send_msg_us = 113
        self.client_post_sched_recv_msg_us = 114

    def with_meta_data(self, key, value):
        self.meta_data[key] = value

    def get_meta_data(self, key, default=None):
        return self.meta_data.get(key, default)


def _reset_storage_state():
    from trpc_sdk.rpcz import storage as storage_mod
    storage_mod._spans.clear()      # pylint: disable=protected-access
    storage_mod._sub_spans.clear()  # pylint: disable=protected-access


def test_finalize_client_span_fill_timestamps_and_sizes():
    ctx = _Ctx()
    ctx.with_meta_data(RPCZ_CLIENT_REQ_SIZE_CTX_KEY, 321)
    ctx.with_meta_data(RPCZ_CLIENT_RSP_SIZE_CTX_KEY, 654)
    span = Span(span_id=10, span_type=SpanType.CLIENT)

    RpczClientFilter._finalize_client_span(ctx, span, rsp=None, err=None)  # pylint: disable=protected-access

    assert span.request_size == 321
    assert span.response_size == 654
    assert span.client_post_sched_send_msg_us == 111
    assert span.client_pre_io_send_msg_us == 112
    assert span.client_post_io_send_msg_us == 113
    assert span.client_post_sched_recv_msg_us == 114
    assert span.error_code == 0


def test_submit_span_route_writes_to_sub_spans(monkeypatch):
    _reset_storage_state()
    monkeypatch.setattr(
        "trpc_sdk.rpcz.filter.client_filter.config",
        SimpleNamespace(
            global_config_obj=SimpleNamespace(
                global_config=SimpleNamespace(
                    rpcz=SimpleNamespace(sample_minimum_timecost=0),
                ),
            ),
        ),
    )
    from trpc_sdk.rpcz import storage as storage_mod

    ctx = _Ctx()
    ctx.with_meta_data(RPCZ_SERVER_SPAN_CTX_KEY, Span(span_id=999, span_type=SpanType.SERVER))
    span = Span(span_id=10, span_type=SpanType.CLIENT, client_pre_rpc_invoke_us=1, client_post_rpc_invoke_us=100)

    RpczClientFilter()._submit_span(ctx, span)  # pylint: disable=protected-access
    assert 999 in storage_mod._sub_spans  # pylint: disable=protected-access
    assert len(storage_mod._sub_spans[999]) == 1  # pylint: disable=protected-access


def test_submit_span_non_route_writes_to_spans(monkeypatch):
    _reset_storage_state()
    monkeypatch.setattr(
        "trpc_sdk.rpcz.filter.client_filter.config",
        SimpleNamespace(
            global_config_obj=SimpleNamespace(
                global_config=SimpleNamespace(
                    rpcz=SimpleNamespace(sample_minimum_timecost=0),
                ),
            ),
        ),
    )
    from trpc_sdk.rpcz import storage as storage_mod

    ctx = _Ctx()
    span = Span(span_id=11, span_type=SpanType.CLIENT, client_pre_rpc_invoke_us=1, client_post_rpc_invoke_us=100)

    RpczClientFilter()._submit_span(ctx, span)  # pylint: disable=protected-access
    assert 11 in storage_mod._spans  # pylint: disable=protected-access


@pytest.mark.asyncio
async def test_run_route_always_sampled(monkeypatch):
    _reset_storage_state()
    monkeypatch.setattr(
        "trpc_sdk.rpcz.filter.client_filter.get_message",
        lambda _: SimpleNamespace(
            callee_service_name="svc.a",
            client_rpc_name="/pkg.Cli/Call",
            remote_addr="127.0.0.1:9999",
        ),
    )
    monkeypatch.setattr(
        "trpc_sdk.rpcz.filter.client_filter.config",
        SimpleNamespace(
            global_config_obj=SimpleNamespace(
                global_config=SimpleNamespace(
                    rpcz=SimpleNamespace(
                        sample_minimum_timecost=0,
                        service_whitelist=[],
                        sample_rate=1000,
                        lower_water_level=0,
                        high_water_level=1,
                    ),
                ),
            ),
        ),
    )
    ctx = _Ctx()
    ctx.with_meta_data(RPCZ_SERVER_SPAN_CTX_KEY, Span(span_id=888, span_type=SpanType.SERVER))
    ctx.with_meta_data(RPCZ_CLIENT_REQ_SIZE_CTX_KEY, 12)
    ctx.with_meta_data(RPCZ_CLIENT_RSP_SIZE_CTX_KEY, 34)
    flt = RpczClientFilter()

    async def _handle():
        return "rsp", None

    rsp, err = await flt.run(ctx, req={"a": 1}, handle=_handle)
    assert err is None
    assert rsp == "rsp"
    out = ctx.get_meta_data(RPCZ_CLIENT_SPAN_CTX_KEY)
    assert out is not None
    assert out.request_size == 12
    assert out.response_size == 34


class _SimpleCtx:
    def __init__(self):
        self.meta_data = {}
        self.timeout = 10

    def with_meta_data(self, key, value):
        self.meta_data[key] = value

    def get_meta_data(self, key, default=None):
        return self.meta_data.get(key, default)


def _set_config(monkeypatch, **overrides):
    values = {
        "service_whitelist": [],
        "sample_rate": 1000,
        "lower_water_level": 0,
        "high_water_level": 1,
        "sample_minimum_timecost": 0,
    }
    values.update(overrides)
    rpcz = SimpleNamespace(**values)
    monkeypatch.setattr(
        filter_mod.config,
        "global_config_obj",
        SimpleNamespace(global_config=SimpleNamespace(rpcz=rpcz)),
    )


def test_service_whitelist_and_empty_callee(monkeypatch):
    _set_config(monkeypatch, service_whitelist=["allowed"])

    assert RpczClientFilter._is_service_allowed("") is False
    assert RpczClientFilter._is_service_allowed("denied") is False
    assert RpczClientFilter._is_service_allowed("allowed") is True


@pytest.mark.asyncio
async def test_intercepted_service_is_not_sampled(monkeypatch):
    _set_config(monkeypatch, service_whitelist=["allowed"])
    monkeypatch.setattr(
        filter_mod,
        "get_message",
        lambda _: SimpleNamespace(callee_service_name="denied"),
    )
    ctx = _SimpleCtx()

    async def handle():
        return "response", None

    assert await RpczClientFilter().run(ctx, "request", handle) == ("response", None)
    assert ctx.get_meta_data(RPCZ_CLIENT_SPAN_CTX_KEY) is None


def test_finalize_non_trpc_error_and_response_string_size(monkeypatch):
    monkeypatch.setattr(filter_mod, "now_us", lambda: 20)
    ctx = _SimpleCtx()
    span = Span(span_id=1, span_type=SpanType.CLIENT)

    RpczClientFilter._finalize_client_span(ctx, span, "hello", ValueError("bad"))

    assert span.error_code == -1
    assert span.response_size == len("hello")


def test_below_minimum_cost_discards_span(monkeypatch):
    _set_config(monkeypatch, sample_minimum_timecost=2)
    submit = MagicMock()
    monkeypatch.setattr(filter_mod, "submit_span", submit)
    span = Span(
        span_id=2,
        span_type=SpanType.CLIENT,
        client_pre_rpc_invoke_us=1_000,
        client_post_rpc_invoke_us=2_999,
    )

    RpczClientFilter()._submit_span(_SimpleCtx(), span)

    submit.assert_not_called()
