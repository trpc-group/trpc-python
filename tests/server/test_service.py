# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.
"""Tests for the unary budget path of :class:`trpc_sdk.server.ServiceAdapter` and its registrar wiring."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from trpc_sdk.abc import TrpcContext
from trpc_sdk.abc import get_message
from trpc_sdk.config import TrpcConfig
from trpc_sdk.exceptions import ServerFuncNotFoundError
from trpc_sdk.exceptions import ServerTimeoutError
from trpc_sdk.server import _registry
from trpc_sdk.server._cython import ServiceAdapter
from trpc_sdk.server._option import ServiceOption
from trpc_sdk.server._types import QueueWaitPolicy
from trpc_sdk.utils import now_ms


class _FakeCodec:
    """Codec that leaves the message as-is and records nothing on the wire."""

    async def decode_data(self, ctx, req_buf):
        return b""

    def encode_data(self, ctx, body):
        return b""


@pytest.fixture
def check_timeout(monkeypatch):
    monkeypatch.setattr(TrpcConfig.server_config(), "check_timeout", True)


def _make_service(queue_wait_timeout: int = 0, policy: int = int(QueueWaitPolicy.DEFAULT)) -> ServiceAdapter:
    opt = ServiceOption()
    opt.codec = _FakeCodec()
    opt.queue_wait_timeout = queue_wait_timeout
    opt.queue_wait_policy = policy
    return ServiceAdapter(opt)


def _make_ctx(timeout_ms: int, waited_ms: int) -> TrpcContext:
    ctx = TrpcContext()
    ctx.timeout = timeout_ms
    ctx.recv_time = float(now_ms() - waited_ms)
    get_message(ctx)
    return ctx


@pytest.mark.parametrize("policy", [int(QueueWaitPolicy.DEFAULT), int(QueueWaitPolicy.SLIDING_WINDOW)])
async def test_unlimited_budget_skips_queue_wait_check(check_timeout, policy) -> None:
    # No service timeout, no request timeout, queue_wait_timeout=0: the
    # queue-wait limit must be "none", so the request reaches handler lookup.
    service = _make_service(policy=policy)
    ctx = _make_ctx(timeout_ms=0, waited_ms=0)

    await service.handle(ctx, b"")

    assert isinstance(get_message(ctx).server_rsp_err, ServerFuncNotFoundError)


async def test_queue_wait_falls_back_to_handler_budget(check_timeout) -> None:
    service = _make_service()
    ctx = _make_ctx(timeout_ms=100, waited_ms=500)

    await service.handle(ctx, b"")

    assert isinstance(get_message(ctx).server_rsp_err, ServerTimeoutError)


async def test_queue_wait_within_handler_budget_passes(check_timeout) -> None:
    service = _make_service()
    ctx = _make_ctx(timeout_ms=5000, waited_ms=0)

    await service.handle(ctx, b"")

    assert isinstance(get_message(ctx).server_rsp_err, ServerFuncNotFoundError)


async def test_missing_stream_handle_raises_value_error() -> None:
    service = _make_service()
    with pytest.raises(ValueError, match="stream_handle is None"):
        await service._handle_stream(TrpcContext(), b"", None)


def test_context_timeout_accessors() -> None:
    ctx = TrpcContext()
    ctx.timeout = 1500
    assert ctx.get_timeout_ms() == 1500
    assert ctx.get_timeout_us() == 1_500_000
    assert ctx.get_timeout_sec() == pytest.approx(1.5)


def test_start_service_registry_only_for_configured_registry(monkeypatch) -> None:
    started = []
    monkeypatch.setattr(_registry.ServiceRegistrar, "start", lambda self: started.append(self.opt))

    with_registry = _make_service()
    with_registry.opt.registry = object()
    without_registry = _make_service()
    without_registry.opt.registry = None
    server = SimpleNamespace(services={"a": with_registry, "b": without_registry})

    _registry.start_service_registry(server)

    assert started == [with_registry.opt]
    assert isinstance(with_registry.registrar, _registry.ServiceRegistrar)
    assert without_registry.registrar is None


def test_close_stops_registrar() -> None:
    stopped = []
    service = _make_service()
    service.close()
    service.registrar = SimpleNamespace(stop=lambda: stopped.append(True))
    service.close()
    assert stopped == [True]
