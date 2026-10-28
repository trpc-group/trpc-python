# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.
"""Resolution of the thread- and process-model fields of ServerOption."""

from __future__ import annotations

from types import SimpleNamespace
from typing import List

import pytest

from trpc_sdk.config import GlobalConfig
from trpc_sdk.config import ProcessModel
from trpc_sdk.config import ServerConfig
from trpc_sdk.config._parser import TrpcConfigParser
from trpc_sdk.server import ServerOption
from trpc_sdk.server import _option_resolver
from trpc_sdk.server._option_resolver import apply_process_model
from trpc_sdk.server._option_resolver import resolve_server_option


@pytest.fixture
def warnings(monkeypatch) -> List[str]:
    recorded: List[str] = []
    fake_logger = SimpleNamespace(
        warning=lambda msg, *args: recorded.append(msg % args if args else msg),
        info=lambda msg, *args: None,
    )
    monkeypatch.setattr(_option_resolver, "logger", fake_logger)
    monkeypatch.setattr(_option_resolver, "default_worker_num", lambda: 4)
    monkeypatch.setattr(_option_resolver.os, "cpu_count", lambda: 4)
    monkeypatch.setattr(_option_resolver.sys, "platform", "linux")
    return recorded


def _raw_option(mode: str = "merge", **fields) -> ServerOption:
    """Build a ServerOption holding unresolved values, as if filled by hand."""
    option = ServerOption()
    option.mode = mode
    for name, value in fields.items():
        assert hasattr(option, name), name
        setattr(option, name, value)
    return option


def _apply(mode: str = "merge", services=(), **fields) -> ServerOption:
    option = _raw_option(mode, **fields)
    resolve_server_option(option, list(services))
    return option


# Thread model.

def test_defaults_resolve_to_single_thread_without_warnings(warnings):
    opt = _apply()
    assert opt.mode == "merge"
    assert opt.io_handle_type == "merge"
    assert opt.io_thread_num == 1
    assert opt.handle_thread_num == 0
    assert opt.handler_max_concurrency == 0
    assert opt.handler_queue_size == 1024
    assert opt.tcp_reuse_port is False
    assert opt.ready_timeout == 300000
    assert warnings == []


def test_empty_mode_counts_as_merge(warnings):
    assert _apply(mode="").io_thread_num == 1
    assert warnings == []


def test_handle_threads_select_the_separate_thread_model(warnings):
    opt = _apply(io_thread_num=2, handle_thread_num=8)
    assert (opt.io_handle_type, opt.io_thread_num, opt.handle_thread_num) == ("separate", 2, 8)
    assert warnings == []


@pytest.mark.parametrize("value", ["separate", "seperate"])
def test_explicit_separate_agreeing_with_handle_threads_is_kept(warnings, value):
    opt = _apply(io_handle_type=value, handle_thread_num=2)
    assert (opt.io_handle_type, opt.handle_thread_num) == ("separate", 2)
    assert warnings == []


def test_explicit_merge_without_handle_threads_is_kept(warnings):
    assert _apply(io_handle_type="merge", io_thread_num=2).io_handle_type == "merge"
    assert warnings == []


@pytest.mark.parametrize("fields", [
    {"io_handle_type": "separate"},
    {"io_handle_type": "merge", "handle_thread_num": 2},
])
def test_explicit_io_handle_type_contradicting_handle_threads_fails(warnings, fields):
    with pytest.raises(ValueError, match="disagrees"):
        _apply(**fields)
    assert len(warnings) == 1


def test_unknown_io_handle_type_fails(warnings):
    with pytest.raises(ValueError, match="colocated"):
        _apply(io_handle_type="colocated")
    assert len(warnings) == 1


def test_negative_handle_thread_num_warns_and_stays_merge(warnings):
    opt = _apply(handle_thread_num=-1)
    assert (opt.io_handle_type, opt.handle_thread_num) == ("merge", 0)
    assert len(warnings) == 1 and "negative" in warnings[0]


def test_oversized_handle_thread_num_warns_but_is_kept(warnings):
    assert _apply(handle_thread_num=17).handle_thread_num == 17
    assert len(warnings) == 1 and "likely too many" in warnings[0]


def test_merge_with_several_io_threads_is_kept(warnings):
    assert _apply(io_thread_num=4).io_thread_num == 4
    assert warnings == []


def test_oversized_io_thread_num_warns_but_is_kept(warnings):
    assert _apply(io_thread_num=17).io_thread_num == 17
    assert len(warnings) == 1 and "likely too many" in warnings[0]


def test_negative_io_thread_num_warns(warnings):
    assert _apply(io_thread_num=-2).io_thread_num == 1
    assert len(warnings) == 1 and "negative" in warnings[0]


def test_negative_max_concurrency_becomes_unlimited(warnings):
    assert _apply(handler_max_concurrency=-1).handler_max_concurrency == 0
    assert len(warnings) == 1 and "negative" in warnings[0]


def test_negative_queue_size_becomes_default(warnings):
    opt = _apply(handle_thread_num=2, handler_max_concurrency=10, handler_queue_size=-5)
    assert opt.handler_queue_size == 1024
    assert len(warnings) == 1 and "negative" in warnings[0]


def test_limit_with_zero_queue_warns_requests_are_dropped(warnings):
    opt = _apply(handle_thread_num=2, handler_max_concurrency=10, handler_queue_size=0)
    assert (opt.handler_max_concurrency, opt.handler_queue_size) == (10, 0)
    assert len(warnings) == 1 and "dropped" in warnings[0]


def test_limit_without_handle_threads_warns_it_is_ignored(warnings):
    opt = _apply(handler_max_concurrency=10)
    assert opt.handler_max_concurrency == 10
    assert len(warnings) == 1 and "only enforced" in warnings[0]


def test_queue_size_without_limit_warns_it_has_no_effect(warnings):
    opt = _apply(handler_queue_size=50)
    assert opt.handler_queue_size == 50
    assert len(warnings) == 1 and "no effect" in warnings[0]


def test_limit_queue_and_reuse_port_are_kept(warnings):
    opt = _apply(handle_thread_num=2, handler_max_concurrency=8, handler_queue_size=16, tcp_reuse_port=True)
    assert (opt.handler_max_concurrency, opt.handler_queue_size) == (8, 16)
    assert opt.tcp_reuse_port is True
    assert warnings == []


def test_ready_timeout_is_only_validated_when_something_waits(warnings):
    assert _apply(ready_timeout=-5).ready_timeout == 300000
    assert warnings == []
    assert _apply(io_thread_num=2, ready_timeout=-5).ready_timeout == 300000
    assert len(warnings) == 1 and "negative" in warnings[0]


@pytest.mark.parametrize("mode", ["separate", "peer"])
@pytest.mark.parametrize("fields", [{"handle_thread_num": 2}, {"io_handle_type": "separate"}, {"io_thread_num": 2}])
def test_threads_outside_merge_mode_fail_startup(warnings, mode, fields):
    with pytest.raises(ValueError):
        _apply(mode=mode, worker_num=2, **fields)
    assert len(warnings) == 1


def test_peer_mode_negative_ready_timeout_falls_back(warnings):
    assert _apply(mode="peer", worker_num=2, ready_timeout=-5).ready_timeout == 300000
    assert len(warnings) == 1 and "negative" in warnings[0]


def test_process_fields_under_server_are_not_parsed():
    removed = ("worker_thread_num", "mode", "worker_num", "io_handle_type", "io_thread_num", "handle_thread_num")
    cfg = ServerConfig()
    TrpcConfigParser.populate_from_dict({**{name: 2 for name in removed}, "ready_timeout": 0}, cfg)
    assert not any(hasattr(cfg, name) for name in removed)
    assert cfg.ready_timeout == 0


# Process model.

def test_seperate_spelling_resolves_to_separate(warnings):
    opt = _apply(mode="seperate", worker_num=2)
    assert (opt.mode, opt.worker_num) == ("separate", 2)
    assert warnings == []


def test_merge_ignores_worker_num(warnings):
    assert _apply(worker_num=3).worker_num == 3
    assert len(warnings) == 1 and "only read in separate and peer mode" in warnings[0]


@pytest.mark.parametrize("mode", ["separate", "peer"])
def test_unset_worker_num_becomes_cpu_count(warnings, mode):
    assert _apply(mode=mode).worker_num == 4
    assert warnings == []


@pytest.mark.parametrize("mode", ["separate", "peer"])
def test_negative_worker_num_warns_and_uses_cpu_count(warnings, mode):
    assert _apply(mode=mode, worker_num=-1).worker_num == 4
    assert len(warnings) == 1 and "negative" in warnings[0]


def test_worker_num_uses_the_fallback_when_cpu_count_is_unknown(monkeypatch, warnings):
    from trpc_sdk.config import _types

    monkeypatch.setattr(_option_resolver, "default_worker_num", _types.default_worker_num)
    monkeypatch.setattr(_types, "get_max_procs", lambda: 0)
    assert _apply(mode="separate").worker_num == _types.DEFAULT_WORKER_NUM_FALLBACK


@pytest.mark.parametrize("mode", ["separate", "peer"])
def test_too_many_workers_warns_but_is_kept(warnings, mode):
    assert _apply(mode=mode, worker_num=9).worker_num == 9
    assert len(warnings) == 1 and "too many" in warnings[0]


@pytest.mark.parametrize("mode", ["separate", "peer"])
def test_explicit_worker_num_is_kept_silently(warnings, mode):
    assert _apply(mode=mode, worker_num=8).worker_num == 8
    assert warnings == []


def test_handler_limits_are_ignored_in_separate_mode(warnings):
    _apply(mode="separate", handler_max_concurrency=8)
    assert any("not supported in separate mode" in w for w in warnings)


@pytest.mark.parametrize("network,protocol,warned", [
    ("tcp", "trpc", False),
    ("udp", "trpc", True),
    ("tcp", "http", True),
])
def test_macos_warns_about_self_bound_services(monkeypatch, warnings, network, protocol, warned):
    monkeypatch.setattr(_option_resolver.sys, "platform", "darwin")
    service = SimpleNamespace(name="svc", network=network, protocol=protocol)
    _apply(mode="separate", services=[service], worker_num=2)
    assert bool(warnings) is warned


def test_macos_with_one_rpc_task_does_not_warn(monkeypatch, warnings):
    monkeypatch.setattr(_option_resolver.sys, "platform", "darwin")
    _apply(mode="separate", services=[SimpleNamespace(name="svc", network="udp", protocol="trpc")], worker_num=1)
    assert warnings == []


def test_peer_single_worker_warns(warnings):
    assert _apply(mode="peer", worker_num=1).worker_num == 1
    assert len(warnings) == 1 and "single worker" in warnings[0]


def test_peer_on_macos_warns_about_balancing(monkeypatch, warnings):
    monkeypatch.setattr(_option_resolver.sys, "platform", "darwin")
    _apply(mode="peer", worker_num=2)
    assert len(warnings) == 1 and "macOS" in warnings[0]


def test_peer_ignores_reuse_port_silently(warnings):
    _apply(mode="peer", worker_num=2, tcp_reuse_port=True)
    assert warnings == []


# global.processmodel.

def _apply_model(option_fields=None, **model_fields) -> ServerOption:
    """Apply a ``global.processmodel`` block onto a ServerOption, then resolve."""
    option = _raw_option(**(option_fields or {}))
    apply_process_model(option, ProcessModel(**model_fields))
    resolve_server_option(option, [])
    return option


def test_processmodel_parses_under_global():
    cfg = GlobalConfig()
    TrpcConfigParser.populate_from_dict(
        {"processmodel": {"mode": "multi_process", "io_handle_type": "peer", "handle_num": 3, "task_num": 2}}, cfg)
    assert (cfg.processmodel.mode, cfg.processmodel.io_handle_type) == ("multi_process", "peer")
    assert (cfg.processmodel.handle_num, cfg.processmodel.task_num) == (3, 2)


def test_empty_processmodel_leaves_a_hand_built_option_alone(warnings):
    opt = _apply_model({"mode": "peer", "worker_num": 2})
    assert (opt.mode, opt.worker_num, opt.task_num) == ("peer", 2, 1)
    assert warnings == []


@pytest.mark.parametrize("io_num,handle_num,threads", [(2, 2, 2), (3, 0, 3), (0, 3, 3), (0, 0, 1)])
def test_single_process_merge_uses_one_thread_count(warnings, io_num, handle_num, threads):
    opt = _apply_model(mode="single_process", io_handle_type="merge", io_num=io_num, handle_num=handle_num)
    assert (opt.mode, opt.io_handle_type) == ("merge", "merge")
    assert (opt.io_thread_num, opt.handle_thread_num) == (threads, 0)
    assert warnings == []


def test_single_process_merge_with_unequal_counts_uses_handle_num(warnings):
    opt = _apply_model(mode="single_process", io_num=2, handle_num=3)
    assert (opt.io_handle_type, opt.io_thread_num, opt.handle_thread_num) == ("merge", 3, 0)
    assert len(warnings) == 1 and "must be equal" in warnings[0]


def test_single_process_separate_maps_to_handle_threads(warnings):
    opt = _apply_model(mode="single_process", io_handle_type="seperate", io_num=2, handle_num=4)
    assert (opt.mode, opt.io_handle_type, opt.io_thread_num, opt.handle_thread_num) == ("merge", "separate", 2, 4)
    assert warnings == []


def test_single_process_separate_needs_handle_num(warnings):
    with pytest.raises(ValueError, match="handle_num > 0"):
        _apply_model(mode="single_process", io_handle_type="separate", io_num=2)


def test_multi_process_defaults_to_separate(warnings):
    opt = _apply_model(mode="multi_process", handle_num=3)
    assert (opt.mode, opt.worker_num, opt.io_thread_num, opt.handle_thread_num) == ("separate", 3, 1, 0)
    assert warnings == []


def test_multi_process_separate_forces_one_io_process(warnings):
    opt = _apply_model(mode="multi_process", io_handle_type="separate", io_num=2, handle_num=2)
    assert (opt.mode, opt.io_thread_num) == ("separate", 1)
    assert len(warnings) == 1 and "single I/O process" in warnings[0]


@pytest.mark.parametrize("io_handle_type", ["merge", "peer"])
def test_multi_process_merge_is_peer(warnings, io_handle_type):
    opt = _apply_model(mode="multi_process", io_handle_type=io_handle_type, handle_num=2)
    assert (opt.mode, opt.worker_num) == ("peer", 2)
    assert warnings == []


def test_multi_process_peer_ignores_io_num(warnings):
    _apply_model(mode="multi_process", io_handle_type="merge", io_num=2, handle_num=2)
    assert len(warnings) == 1 and "no effect" in warnings[0]


def test_multi_process_zero_handle_num_means_cpu_count(warnings):
    assert _apply_model(mode="multi_process", io_handle_type="peer").worker_num == 4


@pytest.mark.parametrize("fields,match", [
    ({"mode": "multi"}, "processmodel.mode"),
    ({"mode": "single_process", "io_handle_type": "peer"}, "io_handle_type"),
    ({"mode": "multi_process", "io_handle_type": "x"}, "io_handle_type"),
])
def test_processmodel_rejects_unknown_names(warnings, fields, match):
    with pytest.raises(ValueError, match=match):
        _apply_model(**fields)


def test_processmodel_replaces_a_hand_built_option(warnings):
    opt = _apply_model({"mode": "peer", "worker_num": 8}, mode="single_process", io_num=2, handle_num=2)
    assert (opt.mode, opt.worker_num, opt.io_thread_num) == ("merge", 0, 2)
    assert warnings == []


@pytest.mark.parametrize("value,expected,warned", [(0, 1, False), (3, 3, False), (-1, 1, True)])
def test_processmodel_task_num(warnings, value, expected, warned):
    assert _apply_model(task_num=value).task_num == expected
    assert bool(warnings) is warned
