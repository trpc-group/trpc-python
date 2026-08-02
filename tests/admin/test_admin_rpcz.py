# -*- coding: utf-8 -*-
import asyncio
import json
import time
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

import trpc_sdk.admin.server as server_mod
import trpc_sdk.config as trpc_config_package
from trpc_sdk.admin.server import Configer
from trpc_sdk.admin.server import handler_rpcz
from trpc_sdk.admin.server import init as admin_init
from trpc_sdk.admin.server import make_response
from trpc_sdk.config import config as trpc_config_core
from trpc_sdk.rpcz.span import Span
from trpc_sdk.rpcz.span import SpanType
from trpc_sdk.utils import get_or_create_event_loop
# 勿用 "trpc_sdk.config.config.global_config_obj" 字符串路径：pytest 解析时易与包名/子模块混淆。


class _FakeWs:
    def __init__(self, closed=False, send_err=None):
        self.closed = closed
        self.send_err = send_err
        self.sent = []

    async def send_str(self, msg):
        if self.send_err:
            raise self.send_err
        self.sent.append(msg)


class _FakeRequest:
    def __init__(self, query):
        self.query = query


def _response_json(resp):
    return json.loads(resp.text)


def _make_server_span(span_id, first_log_us):
    return Span(
        span_id=span_id,
        span_type=SpanType.SERVER,
        server_pre_sched_recv_msg_us=first_log_us,
        server_post_io_send_msg_us=first_log_us + 10,
    )


def test_remove_client_handles_existing_and_missing_client():
    cfg = Configer()
    ws1 = _FakeWs()
    ws2 = _FakeWs()
    cfg.add_client(ws1)
    cfg.add_client(ws2)

    cfg.remove_client(ws1)
    assert ws1 not in cfg.clients
    assert ws2 in cfg.clients

    # remove missing client should not raise
    cfg.remove_client(ws1)
    assert cfg.clients == [ws2]


@pytest.mark.asyncio
async def test_rpcz_returns_none_without_healthy_clients():
    cfg = Configer()
    out = await cfg.rpcz(payload={"type": "collect"})
    assert out is None


@pytest.mark.asyncio
async def test_rpcz_send_failure_cleans_pending():
    cfg = Configer()
    bad_ws = _FakeWs(send_err=RuntimeError("send failed"))
    cfg.clients = [bad_ws]

    out = await cfg.rpcz(payload={"type": "collect"}, timeout=0.05, child_index=0)
    assert out is None
    assert cfg.rpcz_pending == {}


@pytest.mark.asyncio
async def test_rpcz_timeout_cleans_pending():
    cfg = Configer()
    ws = _FakeWs()
    cfg.clients = [ws]

    out = await cfg.rpcz(payload={"type": "collect"}, timeout=0.01, child_index=0)
    assert out is None
    assert cfg.rpcz_pending == {}
    assert len(ws.sent) == 1


def test_decode_span_exception_branches(monkeypatch):
    cfg = Configer()

    monkeypatch.setattr(
        "trpc_sdk.admin.server.Span.from_dict",
        lambda _: (_ for _ in ()).throw(ValueError("bad dict")),
    )
    assert cfg._decode_span({"span_id": 1}) is None  # pylint: disable=protected-access

    assert cfg._decode_span("{not-json") is None  # pylint: disable=protected-access
    assert cfg._decode_span(12345) is None  # pylint: disable=protected-access


def test_on_subprocess_rpcz_appends_and_sorts():
    cfg = Configer()
    payload1 = {"request_id": "r1", "child_index": 0, "spans": [_make_server_span(1, 100)]}
    payload2 = {"request_id": "r2", "child_index": 1, "spans": [_make_server_span(2, 300)]}

    cfg.on_subprocess_rpcz("r1", payload1)
    cfg.on_subprocess_rpcz("r2", payload2)

    assert len(cfg.rpcz_sorted_spans) == 2
    assert cfg.rpcz_sorted_spans[0].server_pre_sched_recv_msg_us == 300


def test_on_subprocess_rpcz_empty_payload_skip():
    cfg = Configer()
    cfg.on_subprocess_rpcz("r-empty", {"child_index": 0, "spans": []})
    assert cfg.rpcz_sorted_spans == []


def test_on_subprocess_rpcz_decode_and_rename_failures(monkeypatch):
    cfg = Configer()
    pending = get_or_create_event_loop().create_future()
    cfg.rpcz_pending["r-decode"] = pending
    payload = {"child_index": 0, "spans": ["not-json", _make_server_span(2, 200)]}

    # force rename failure for valid span to cover rename exception branch
    monkeypatch.setattr(
        cfg,
        "_span_rename_id",
        lambda _span, _child_index: (_ for _ in ()).throw(RuntimeError("rename fail")),
    )
    cfg.on_subprocess_rpcz("r-decode", payload)

    assert cfg.rpcz_sorted_spans == []
    assert pending.done()
    assert pending.result() == payload


def test_on_subprocess_rpcz_top_level_exception(monkeypatch):
    cfg = Configer()
    pending = get_or_create_event_loop().create_future()
    cfg.rpcz_pending["r-ex"] = pending

    monkeypatch.setattr(cfg, "_decode_span", lambda _raw: (_ for _ in ()).throw(RuntimeError("boom")))
    payload = {"child_index": 0, "spans": ["xxx"]}
    cfg.on_subprocess_rpcz("r-ex", payload)

    assert pending.done()
    assert pending.result() == payload


@pytest.mark.asyncio
async def test_handler_rpcz_invalid_span_id_returns_error(monkeypatch):
    old_configer = server_mod.configer
    try:
        server_mod.configer = Configer()
        req = _FakeRequest({"span_id": "abc"})
        resp = await handler_rpcz(req)
        out = _response_json(resp)
        assert out["errorcode"] == -1
        assert "span_id must be integer" in out["message"]
    finally:
        server_mod.configer = old_configer


@pytest.mark.asyncio
async def test_handler_rpcz_span_id_not_found(monkeypatch):
    old_configer = server_mod.configer
    try:
        server_mod.configer = Configer()
        req = _FakeRequest({"span_id": "999"})
        resp = await handler_rpcz(req)
        out = _response_json(resp)
        assert out["errorcode"] == -1
        assert out["message"] == "span_id not found"
    finally:
        server_mod.configer = old_configer


@pytest.mark.asyncio
async def test_handler_rpcz_span_id_detail_success(monkeypatch):
    old_configer = server_mod.configer
    try:
        cfg = Configer()
        span = _make_server_span(123, 1000)
        cfg.rpcz_sorted_spans = [span]
        server_mod.configer = cfg

        req = _FakeRequest({"span_id": "123"})
        resp = await handler_rpcz(req)
        out = _response_json(resp)
        assert out["errorcode"] == 0
        assert out["span"]["span_id"] == 123
    finally:
        server_mod.configer = old_configer


@pytest.mark.asyncio
async def test_handler_rpcz_brief_fallback_when_config_read_failed(monkeypatch):
    old_configer = server_mod.configer
    try:
        cfg = Configer()
        cfg.rpcz_sorted_spans = [_make_server_span(i, 1000 + i) for i in range(20)]
        server_mod.configer = cfg

        monkeypatch.setattr(trpc_config_core, "global_config_obj", None, raising=False)
        monkeypatch.setattr(trpc_config_package, "global_config_obj", None, raising=False)
        req = _FakeRequest({})
        resp = await handler_rpcz(req)
        out = _response_json(resp)
        assert out["errorcode"] == 0
        assert len(out["spans"]) == 10
    finally:
        server_mod.configer = old_configer


def _rpcz_cfg(server_filter=None, services=None, client_filter=None, backends=None):
    services = services or []
    backends = backends or []
    return SimpleNamespace(
        server=SimpleNamespace(filter=server_filter or [], service=services),
        client=SimpleNamespace(filter=client_filter or [], service=backends),
    )


def test_is_rpcz_filter_enabled_false_when_no_rpcz():
    assert server_mod.is_rpcz_filter_enabled_in_config(_rpcz_cfg()) is False


def test_is_rpcz_filter_enabled_server_level():
    assert server_mod.is_rpcz_filter_enabled_in_config(_rpcz_cfg(server_filter=["rpcz"])) is True


def test_is_rpcz_filter_enabled_service_level():
    svc = SimpleNamespace(filter=["other", "rpcz"])
    assert server_mod.is_rpcz_filter_enabled_in_config(_rpcz_cfg(services=[svc])) is True


def test_is_rpcz_filter_enabled_client_level():
    assert server_mod.is_rpcz_filter_enabled_in_config(_rpcz_cfg(client_filter=["rpcz"])) is True


def test_is_rpcz_filter_enabled_backend_level():
    backend = SimpleNamespace(filter=["rpcz"])
    assert server_mod.is_rpcz_filter_enabled_in_config(_rpcz_cfg(backends=[backend])) is True


def test_is_rpcz_filter_enabled_config_inspect_error_returns_false():
    class _BadCfg:
        @property
        def server(self):
            raise RuntimeError("boom")

    assert server_mod.is_rpcz_filter_enabled_in_config(_BadCfg()) is False


def test_init_enable_rpcz_collector_true_starts_thread_and_startup_hook(monkeypatch):
    """enable_rpcz_collector 为 True 时应启动采集线程并注册 loop 绑定回调。"""
    monkeypatch.delattr(admin_init, "_rpcz_collector_started", raising=False)
    monkeypatch.setattr(server_mod, "is_rpcz_filter_enabled_in_config", lambda: False)
    app_base = admin_init()
    n_base = len(app_base.on_startup)

    monkeypatch.setattr(server_mod, "is_rpcz_filter_enabled_in_config", lambda: True)
    monkeypatch.delattr(admin_init, "_rpcz_collector_started", raising=False)

    mock_thread_cls = MagicMock()
    mock_thread_inst = MagicMock()
    mock_thread_cls.return_value = mock_thread_inst
    monkeypatch.setattr(server_mod.threading, "Thread", mock_thread_cls)

    app = admin_init()

    mock_thread_cls.assert_called_once()
    call_kw = mock_thread_cls.call_args.kwargs
    assert call_kw["name"] == "rpcz_collector"
    assert call_kw["daemon"] is True
    assert call_kw["target"].__name__ == "rpcz_collector"
    mock_thread_inst.start.assert_called_once()
    assert getattr(admin_init, "_rpcz_collector_started") is True
    assert len(app.on_startup) == n_base + 1


@pytest.mark.asyncio
async def test_init_enable_rpcz_collector_bind_running_loop_sets_configer_loop(monkeypatch):
    monkeypatch.delattr(admin_init, "_rpcz_collector_started", raising=False)
    monkeypatch.setattr(server_mod, "is_rpcz_filter_enabled_in_config", lambda: True)
    monkeypatch.setattr(
        server_mod.threading,
        "Thread",
        lambda *a, **k: MagicMock(start=MagicMock()),
    )

    app = admin_init()
    binders = [cb for cb in app.on_startup if getattr(cb, "__name__", None) == "_bind_running_loop"]
    assert len(binders) == 1
    loop = asyncio.get_running_loop()
    old_loop = server_mod.configer.loop
    try:
        await binders[0](None)
        assert server_mod.configer.loop is loop
    finally:
        server_mod.configer.loop = old_loop


def test_init_enable_rpcz_collector_second_init_does_not_start_second_thread(monkeypatch):
    """_rpcz_collector_started 已置位后再次 init 不应再创建采集线程。"""
    monkeypatch.delattr(admin_init, "_rpcz_collector_started", raising=False)
    monkeypatch.setattr(server_mod, "is_rpcz_filter_enabled_in_config", lambda: True)

    mock_thread_cls = MagicMock()
    mock_thread_cls.return_value = MagicMock()
    monkeypatch.setattr(server_mod.threading, "Thread", mock_thread_cls)

    admin_init()
    mock_thread_cls.assert_called_once()
    mock_thread_cls.reset_mock()

    admin_init()
    mock_thread_cls.assert_not_called()


def test_evict_expired_spans_removes_old_spans():
    cfg = Configer()
    now_us = int(time.time() * 1_000_000)
    old_span = _make_server_span(1, now_us - 120_000_000)
    recent_span = _make_server_span(2, now_us - 10_000_000)
    cfg.rpcz_sorted_spans = [recent_span, old_span]

    cfg.evict_expired_spans(cache_expire_interval_s=60)

    assert len(cfg.rpcz_sorted_spans) == 1
    assert cfg.rpcz_sorted_spans[0].span_id == 2


def test_init_enable_rpcz_collector_via_global_config_obj(monkeypatch):
    """通过 global_config_obj 判定为启用时，同样应走采集线程启动分支。"""
    monkeypatch.delattr(admin_init, "_rpcz_collector_started", raising=False)
    empty = _rpcz_cfg()
    monkeypatch.setattr(trpc_config_core, "global_config_obj", empty)
    monkeypatch.setattr(trpc_config_package, "global_config_obj", empty)
    n_base = len(admin_init().on_startup)

    enabled = _rpcz_cfg(server_filter=["rpcz"])
    monkeypatch.setattr(trpc_config_core, "global_config_obj", enabled)
    monkeypatch.setattr(trpc_config_package, "global_config_obj", enabled)
    monkeypatch.delattr(admin_init, "_rpcz_collector_started", raising=False)

    mock_thread_cls = MagicMock()
    mock_thread_cls.return_value = MagicMock()
    monkeypatch.setattr(server_mod.threading, "Thread", mock_thread_cls)

    assert server_mod.is_rpcz_filter_enabled_in_config() is True
    app = admin_init()
    mock_thread_cls.assert_called_once()
    assert len(app.on_startup) == n_base + 1
