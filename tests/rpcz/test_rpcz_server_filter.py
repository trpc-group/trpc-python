# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

from types import SimpleNamespace

import pytest

from trpc_sdk.rpcz import RPCZ_SERVER_SPAN_CTX_KEY
from trpc_sdk.rpcz import RpczServerFilter
from trpc_sdk.rpcz import SpanIdGenerator
from trpc_sdk.rpcz import SpanType
from trpc_sdk.rpcz.filter import _server_filter as filter_mod


class _Ctx:
    def __init__(self):
        self.meta_data = {}
        self.recv_time = 1.5

    def with_meta_data(self, key, value):
        self.meta_data[key] = value

    def get_meta_data(self, key, default=None):
        return self.meta_data.get(key, default)


def test_build_server_span_uses_message_fields(monkeypatch):
    ctx = _Ctx()
    monkeypatch.setattr(
        filter_mod, "get_message",
        lambda _: SimpleNamespace(
            server_rpc_name="/pkg.Service/Method",
            caller_service_name="caller",
            remote_addr="127.0.0.1:1234",
            callee_service_name="callee",
        ),
    )
    flt = RpczServerFilter()
    span = flt._build_server_span(ctx, 7)  # pylint: disable=protected-access
    assert span.span_id == 7
    assert span.span_type == SpanType.SERVER
    assert span.full_method_name == "/pkg.Service/Method"
    assert span.remote_name == "caller"
    assert span.remote_endpoint == "127.0.0.1:1234"
    assert span.server_pre_sched_recv_msg_us == 1500


def _set_config(monkeypatch, **rpcz_values):
    rpcz = SimpleNamespace(**rpcz_values)
    monkeypatch.setattr(filter_mod.TrpcConfig, "global_config", staticmethod(lambda: SimpleNamespace(rpcz=rpcz)))


def test_should_sample_respects_whitelist(monkeypatch):
    ctx = _Ctx()
    monkeypatch.setattr(
        filter_mod, "get_message",
        lambda _: SimpleNamespace(callee_service_name="svc.a"),
    )
    _set_config(
        monkeypatch,
        service_whitelist=["svc.b"],
        sample_rate=1,
        lower_water_level=0,
        high_water_level=100,
    )
    flt = RpczServerFilter()
    assert flt._should_sample(ctx, 1) is False  # pylint: disable=protected-access


@pytest.mark.asyncio
async def test_run_sampled_writes_span(monkeypatch):
    ctx = _Ctx()
    monkeypatch.setattr(
        filter_mod, "get_message",
        lambda _: SimpleNamespace(
            server_rpc_name="/pkg.Service/Method",
            caller_service_name="caller",
            remote_addr="127.0.0.1:1234",
            callee_service_name="svc.a",
        ),
    )
    _set_config(
        monkeypatch,
        service_whitelist=[],
        sample_rate=1,
        lower_water_level=0,
        high_water_level=1000,
    )
    flt = RpczServerFilter()
    flt._id_generator = SpanIdGenerator()  # pylint: disable=protected-access

    async def _handle():
        return "rsp", None

    rsp, err = await flt.run(ctx, {"k": "v"}, _handle)
    assert err is None
    assert rsp == "rsp"
    span = ctx.get_meta_data(RPCZ_SERVER_SPAN_CTX_KEY)
    assert span is not None
    assert span.span_type == SpanType.SERVER
    assert span.server_pre_rpc_invoke_us > 0
    assert span.server_post_rpc_invoke_us > 0
