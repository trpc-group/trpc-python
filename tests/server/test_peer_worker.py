# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.
"""Peer mode without real processes: worker construction, startup order, exit hooks."""

from __future__ import annotations

import asyncio
import multiprocessing
import signal
import time
from types import SimpleNamespace

import pytest

from trpc_sdk.runtime import HandlerThreadInfo
from trpc_sdk.runtime import MergeWorker
from trpc_sdk.runtime import PeerMainWorker
from trpc_sdk.runtime import ResourceKind
from trpc_sdk.runtime import _constants
from trpc_sdk.runtime import _merge_worker
from trpc_sdk.runtime import _modes
from trpc_sdk.runtime import current_thread_model
from trpc_sdk.runtime._common import _shutdown
from trpc_sdk.runtime._common import _readiness
from trpc_sdk.runtime._process import _peer_worker
from trpc_sdk.transport import TrpcServerTransport
from trpc_sdk.utils import TimerScope

PeerWorker = _peer_worker.PeerWorker
PeerWorkerProcess = _peer_worker.PeerWorkerProcess


@pytest.fixture
def engine(engine_factory):
    return engine_factory([("tcp", "trpc")], worker_num=3, close_wait_time=2000)


@pytest.fixture
def loops():
    created = []

    def _new() -> asyncio.AbstractEventLoop:
        created.append(asyncio.new_event_loop())
        return created[-1]

    yield _new
    for loop in created:
        if not loop.is_closed():
            loop.close()


@pytest.fixture
def child(engine):
    return PeerWorkerProcess(engine, 1, 3, multiprocessing.RawArray("b", 3))


@pytest.fixture
def main(engine, monkeypatch, loops):
    worker = PeerMainWorker(engine)
    worker.loop = loops()
    worker._create_ready_flags(worker.worker_num)
    worker._worker_children = [SimpleNamespace(name="worker:1"), SimpleNamespace(name="worker:2")]
    return worker


def _run_until_exit(process, *later) -> int:
    """Run ``process.loop`` with ``(delay, callback)`` pairs scheduled; return the exit status."""
    for delay, callback in later:
        process.loop.call_later(delay, callback)
    with pytest.raises(SystemExit) as exc:
        process.loop.run_forever()
    return exc.value.code


def _record_logs(monkeypatch, level: str):
    lines = []
    monkeypatch.setattr(_peer_worker.logger, level, lambda fmt, *args: lines.append(fmt % args if args else fmt))
    return lines


def test_peer_worker_forces_reuse_port_and_a_single_loop(engine):
    worker = PeerWorker(engine, 2, 3, "worker:2")

    assert engine.opts.tcp_reuse_port is False
    assert worker.tcp_reuse_port is True
    assert worker.thread_num == 1
    assert worker.worker_id == 2


def test_main_loop_identity_is_the_worker_index(engine):
    info = PeerWorker(engine, 2, 3, "worker:2")._main_loop_thread_info()

    assert (info.index, info.count) == (2, 3)


def test_merge_worker_main_loop_identity_is_unchanged(engine):
    info = MergeWorker(engine)._main_loop_thread_info()

    assert (info.index, info.count) == (0, 1)


def test_udp_is_bound_with_reuse_port_and_kept(monkeypatch, engine_factory, loops):
    calls = []
    endpoint = object()

    def _listen(self):
        model = current_thread_model()
        calls.append((model.reuse_port, list(model.io.reactors) == [model.acceptor]))
        model.keep_resource(model.acceptor, endpoint, ResourceKind.ENDPOINT)

    monkeypatch.setattr(TrpcServerTransport, "listen", _listen)
    worker = PeerWorker(engine_factory([("udp", "trpc")]), 1, 3, "worker:1")
    worker.loop = loops()
    worker._install_thread_model()

    worker._listen_on(worker.transports[0])

    model = worker.thread_model
    assert calls == [(True, True)]
    assert model.reuse_port is True
    assert model.pop_resources(model.acceptor, ResourceKind.ENDPOINT) == [endpoint]


def test_graceful_exit_releases_everything_once(monkeypatch, engine, loops):
    events = []

    async def _stop_hooks(info):
        events.append(("on_stop", info.index))

    async def _cleanup():
        events.append("runner")

    monkeypatch.setattr(_merge_worker, "run_stop_hooks", _stop_hooks)
    worker = PeerWorker(engine, 1, 3, "worker:1")
    worker.loop = loops()
    worker.timer = SimpleNamespace(stop=lambda: events.append("timer"))
    worker._main_thread_info = HandlerThreadInfo(index=1, count=3, loop=worker.loop)
    worker._install_thread_model()
    model = worker.thread_model
    model.keep_resource(model.acceptor, SimpleNamespace(close=lambda: events.append("udp")), ResourceKind.ENDPOINT)
    model.keep_resource(model.acceptor, SimpleNamespace(cleanup=_cleanup), ResourceKind.RUNNER)

    worker._on_graceful_exit()
    worker._on_graceful_exit()

    assert events == ["timer", ("on_stop", 1), "runner", "udp"]


def test_child_is_controlled_and_has_no_callbacks(child):
    assert child._controlled
    assert child._stop_signal() == signal.SIGUSR1
    assert child.drain_seconds == pytest.approx(2.0)
    assert child.name == "worker:1"
    assert not child.ready_callbacks
    assert not child.exit_callbacks
    assert not child.post_drain_callbacks
    assert not child.master_exit_callbacks


def test_child_first_sigusr1_drains_and_the_next_cuts_it_short(monkeypatch, child, loops):
    monkeypatch.setattr(_shutdown.os, "getppid", lambda: child._parent_pid)
    monkeypatch.setattr(_shutdown, "run_worker_signal_handle", lambda: None)
    stopped = []
    monkeypatch.setattr(PeerWorkerProcess, "_stop_accepting", lambda self: stopped.append(True))
    child.loop = loops()
    began = time.monotonic()

    code = _run_until_exit(child, (0, child._on_stop_signal), (0.05, child._on_stop_signal))

    assert code == 0
    assert stopped == [True]
    assert time.monotonic() - began < 1.5


def test_child_sigusr1_after_the_parent_died_exits_now(monkeypatch, child):
    calls = []
    monkeypatch.setattr(_shutdown.os, "getppid", lambda: child._parent_pid + 1)
    monkeypatch.setattr(PeerWorkerProcess, "_arm_exit_killer", lambda self: calls.append("killer"))
    monkeypatch.setattr(PeerWorkerProcess, "_cut_now", lambda self: calls.append("cut"))

    child._on_stop_signal()

    assert calls == ["killer", "cut"]


def _stub_child_startup(monkeypatch, child, order):
    for name in ("_restore_task_mesh_after_fork", "_close_unowned_task_pipes"):
        monkeypatch.setattr(child, name, lambda: None)
    monkeypatch.setattr(child, "_connect_tasks", lambda: order.append("connect"))
    monkeypatch.setattr(child, "start_worker_process", lambda: order.append("bind"))
    monkeypatch.setattr(child, "_start_timers", lambda: order.append("timers"))


def test_child_startup_binds_after_on_start_and_sets_its_flag_last(monkeypatch, child):
    order = []
    _stub_child_startup(monkeypatch, child, order)

    child._prepare_to_serve()

    assert order == ["connect", "bind", "timers"]
    assert list(child._ready_flags) == [0, 1, 0]


def test_child_told_to_stop_during_startup_does_not_bind(monkeypatch, child):
    order = []
    _stub_child_startup(monkeypatch, child, order)
    child._early_exit = True

    child._prepare_to_serve()

    assert order == ["connect"]
    assert child._ready_flags[1] == 0


def test_child_startup_failure_goes_through_fail_startup(monkeypatch, child):
    failures = []
    _stub_child_startup(monkeypatch, child, [])

    def _boom():
        raise RuntimeError("bind failed")

    monkeypatch.setattr(child, "start_worker_process", _boom)
    monkeypatch.setattr(child, "_fail_startup", lambda what, err: failures.append((what, str(err))))

    child._prepare_to_serve()

    assert failures == [("startup failed", "bind failed")]
    assert child._ready_flags[1] == 0


def test_task_is_rejected_before_the_fork(monkeypatch, main):
    monkeypatch.setattr(_peer_worker, "get_registered_tasks", lambda: [SimpleNamespace(name="infer")])

    with pytest.raises(ValueError, match="does not support Task"):
        main._check_before_fork()


def test_check_before_fork_warns(monkeypatch, main):
    warnings = _record_logs(monkeypatch, "warning")
    monkeypatch.setattr(_peer_worker, "get_registered_tasks", lambda: [])
    monkeypatch.setattr(_readiness, "registered_timer_tasks",
                        lambda: [SimpleNamespace(cls=type("Tick", (), {}), name="", scope=TimerScope.PROCESS)])
    monkeypatch.setattr(_peer_worker, "has_custom_scheduler", lambda: True)

    main._check_before_fork()

    assert any("3 peer workers" in line for line in warnings)
    assert any("register_dispatcher_worker_func" in line for line in warnings)
    assert warnings[-1] == _constants.PEER_STARTUP_WARNING % 3


def test_io_wait_count_is_ignored(monkeypatch, main):
    warnings = _record_logs(monkeypatch, "warning")
    main.io_wait_count = 3

    main._wait_until_tasks_ready()

    assert main.io_wait_count == 0
    assert len(warnings) == 1 and "io_wait_count=3" in warnings[0]


def test_registration_runs_before_workers_bind(monkeypatch, main):
    """Worker 0 runs ready callbacks before it binds and waits for the others."""
    order = []
    for name in ("_restore_task_mesh_after_fork", "_close_unowned_task_pipes", "_connect_tasks", "_start_timers"):
        monkeypatch.setattr(main, name, lambda: None)
    monkeypatch.setattr(MergeWorker, "start_worker_process", lambda self: order.append("bind"))
    monkeypatch.setattr(main, "_wait_children_ready",
                        lambda records: order.append(("wait", main._ready_flags[0], len(records))) or 0.0)
    main.ready_callbacks.append(lambda: order.append("register"))

    main._prepare_to_serve()

    assert order == ["register", "bind", ("wait", 1, 2)]


def test_exit_hooks_signal_and_wait_for_the_other_workers(monkeypatch, main):
    signalled = []
    waited = []

    async def _wait_children(records, timeout):
        waited.append((records, timeout))

    monkeypatch.setattr(main, "_signal", lambda records: signalled.append(records))
    monkeypatch.setattr(main, "_wait_children", _wait_children)

    main._stop_accepting()
    main._on_drain_cut()
    main.loop.run_until_complete(main._wait_handlers())

    assert signalled == [main._worker_children, main._worker_children]
    assert waited == [(main._worker_children, _constants.CHILD_EXIT_GRACE_SECONDS)]


def _record_shutdown(monkeypatch, main, order):
    async def _wait_children(records, timeout):
        order.append("wait_children")

    monkeypatch.setattr(main, "_signal", lambda records: order.append("signal"))
    monkeypatch.setattr(main, "_wait_children", _wait_children)
    monkeypatch.setattr(_shutdown, "run_worker_signal_handle", lambda: order.append("worker_handle"))
    main.exit_callbacks.append(lambda: order.append("deregister"))
    main.post_drain_callbacks.append(lambda: order.append("post_drain"))
    main.master_exit_callbacks.append(lambda: order.append("master_handle"))


def test_shutdown_order(monkeypatch, main):
    order = []
    _record_shutdown(monkeypatch, main, order)

    code = _run_until_exit(main, (0, main._handle_sigterm))

    assert code == 0
    assert order == ["deregister", "signal", "post_drain", "wait_children", "master_handle", "worker_handle"]


def test_second_sigterm_cuts_the_drain_and_still_runs_post_drain(monkeypatch, main):
    order = []
    _record_shutdown(monkeypatch, main, order)
    main.drain_seconds = 30
    began = time.monotonic()

    code = _run_until_exit(main, (0, main._handle_sigterm), (0.05, main._handle_sigterm))

    assert code == 0
    assert time.monotonic() - began < 5
    assert order.count("signal") == 2
    assert "post_drain" in order


def test_peer_mode_is_registered_and_not_replaced_by_the_thread_model(engine_factory):
    engine = engine_factory([("tcp", "trpc")], handle_thread_num=2)

    assert _modes.resolve_worker_cls("peer") is PeerMainWorker
    assert type(_modes.create_worker("peer", engine)) is PeerMainWorker
