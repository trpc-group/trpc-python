# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.
"""Separate mode without a control pipe: SIGUSR1 shutdown and the shared ready flags."""

from __future__ import annotations

import asyncio
import signal
import time

import pytest

from trpc_sdk.runtime import RpcTaskProcess
from trpc_sdk.runtime import SeparateWorker
from trpc_sdk.runtime._common import _shutdown
from trpc_sdk.runtime._common import _readiness
from trpc_sdk.runtime._process import _rpc_task


@pytest.fixture
def worker(engine_factory):
    engine = engine_factory([("tcp", "trpc")], worker_num=2)
    engine.opts.close_wait_time = 2000
    io_worker = SeparateWorker(engine)
    io_worker._create_rpc_task_pipes()
    yield io_worker
    for pipes in io_worker.rpc_task_pipes:
        pipes.close()
    if io_worker.loop is not None and not io_worker.loop.is_closed():
        io_worker.loop.close()


@pytest.fixture
def rpc_task(worker):
    task = RpcTaskProcess(0, worker.rpc_task_pipes, worker._ready_flags, worker.server_engine)
    yield task
    if task.loop is not None and not task.loop.is_closed():
        task.loop.remove_signal_handler(signal.SIGUSR1)
        task.loop.close()


def _run_until_exit(process, *later) -> int:
    """Run ``process.loop`` with ``(delay, callback)`` pairs scheduled; return the exit status."""
    for delay, callback in later:
        process.loop.call_later(delay, callback)
    with pytest.raises(SystemExit) as exc:
        process.loop.run_forever()
    return exc.value.code


def test_rpc_task_is_controlled_and_drains_close_wait_time(rpc_task):
    assert rpc_task._controlled
    assert rpc_task._stop_signal() == signal.SIGUSR1
    assert rpc_task.drain_seconds == pytest.approx(2.0)


def test_first_sigusr1_drains_and_the_next_cuts_it_short(monkeypatch, rpc_task):
    monkeypatch.setattr(_shutdown.os, "getppid", lambda: rpc_task._parent_pid)
    monkeypatch.setattr(_shutdown, "run_worker_signal_handle", lambda: None)
    stopped = []
    monkeypatch.setattr(RpcTaskProcess, "_stop_accepting", lambda self: stopped.append(True))
    rpc_task.loop = asyncio.new_event_loop()
    began = time.monotonic()

    code = _run_until_exit(rpc_task, (0, rpc_task._on_stop_signal), (0.05, rpc_task._on_stop_signal))

    assert code == 0
    assert stopped == [True]
    assert time.monotonic() - began < 1.5


def test_sigusr1_after_the_parent_died_exits_now(monkeypatch, rpc_task):
    calls = []
    monkeypatch.setattr(_shutdown.os, "getppid", lambda: rpc_task._parent_pid + 1)
    monkeypatch.setattr(RpcTaskProcess, "_arm_exit_killer", lambda self: calls.append("killer"))
    monkeypatch.setattr(RpcTaskProcess, "_cut_now", lambda self: calls.append("cut"))

    rpc_task._on_stop_signal()

    assert calls == ["killer", "cut"]


def test_sigusr1_during_startup_is_only_recorded(rpc_task):
    rpc_task._record_early_exit(signal.SIGUSR1, None)

    assert rpc_task._early_exit
    assert not rpc_task._exiting


def test_recorded_sigusr1_is_acted_on_once_the_loop_handler_is_installed(monkeypatch, rpc_task):
    handlers = {}
    cuts = []
    monkeypatch.setattr(RpcTaskProcess, "_cut_now", lambda self: cuts.append(True))
    fake_loop = type("_Loop", (), {"add_signal_handler": lambda self, sig, cb: handlers.setdefault(sig, cb)})()
    rpc_task._early_exit = True
    rpc_task.loop = fake_loop

    rpc_task._register_signal_handlers()
    rpc_task.loop = None

    assert handlers[signal.SIGUSR1] == rpc_task._on_stop_signal
    assert cuts == [True]


def test_early_sigusr1_exits_without_waiting_out_close_wait_time(monkeypatch, rpc_task):
    monkeypatch.setattr(_shutdown, "run_worker_signal_handle", lambda: None)
    rpc_task.loop = asyncio.new_event_loop()
    rpc_task._early_exit = True
    began = time.monotonic()

    # ``_serve`` installs the handlers before the loop turns, so the template
    # runs to completion right here and raises ``SystemExit``.
    with pytest.raises(SystemExit) as exc:
        rpc_task._register_signal_handlers()

    assert exc.value.code == 0
    assert time.monotonic() - began < 1.0


def test_rpc_task_runs_worker_handle_before_on_stop_and_only_once(monkeypatch, rpc_task):
    order = []

    async def _on_stop(info):
        order.append("on_stop")

    monkeypatch.setattr(_shutdown, "run_worker_signal_handle", lambda: order.append("worker_handle"))
    monkeypatch.setattr(_rpc_task, "run_stop_hooks", _on_stop)
    rpc_task.loop = asyncio.new_event_loop()
    rpc_task.drain_seconds = 0
    rpc_task._thread_info = object()

    code = _run_until_exit(rpc_task, (0, rpc_task._handle_sigterm))
    rpc_task._on_graceful_exit()
    rpc_task._on_graceful_exit()

    assert code == 0
    assert order == ["worker_handle", "on_stop"]


def test_second_sigterm_cuts_the_drain_resignals_rpc_tasks_and_still_runs_post_drain(monkeypatch, worker):
    signalled = []
    posted = []
    monkeypatch.setattr(SeparateWorker, "_signal", lambda self, records: signalled.append(records))
    worker.drain_seconds = 30
    worker.post_drain_callbacks.append(lambda: posted.append(True))
    worker.loop = asyncio.new_event_loop()
    began = time.monotonic()

    code = _run_until_exit(worker, (0, worker._handle_sigterm), (0.05, worker._handle_sigterm))

    assert code == 0
    assert time.monotonic() - began < 5
    assert signalled == [worker._rpc_records, worker._rpc_records]
    assert posted == [True]


def test_ready_flags_complete_the_startup_wait(worker):
    worker.loop = asyncio.new_event_loop()
    worker._ready_flags[0] = 1
    worker._ready_flags[1] = 1

    worker._wait_children_ready([])

    assert worker._not_ready() == []


def test_missing_ready_flag_times_out_and_names_the_rpc_task(monkeypatch, worker):
    errors = []
    monkeypatch.setattr(_readiness.logger, "error", lambda fmt, *args: errors.append(args))
    monkeypatch.setattr(worker, "_stop_children", lambda: None)
    worker.loop = asyncio.new_event_loop()
    worker.ready_timeout = 0.2
    worker._ready_flags[0] = 1

    with pytest.raises(SystemExit) as exc:
        worker._wait_children_ready([])

    assert exc.value.code == 1
    assert errors and errors[0][0] == [1]


def test_signal_sends_each_live_child_its_own_stop_signal(monkeypatch, worker):
    sent = []
    monkeypatch.setattr(_shutdown.os, "kill", lambda pid, sig: sent.append((pid, sig)))

    def _record(pid, stop):
        process = type("_P", (), {"pid": pid, "_stop_signal": lambda self: stop})()
        return type("_Rec", (), {"process": process})()

    controlled = _record(11, signal.SIGUSR1)
    top_level = _record(12, signal.SIGTERM)
    dead = _record(13, signal.SIGUSR1)
    monkeypatch.setattr(worker, "_child_alive", lambda record: record is not dead)

    worker._signal([controlled, top_level, dead])

    assert sent == [(11, signal.SIGUSR1), (12, signal.SIGTERM)]
