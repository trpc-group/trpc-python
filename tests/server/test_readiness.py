# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.
"""The startup wait shared by separate and peer mode."""

from __future__ import annotations

import asyncio
import os
from types import SimpleNamespace

import pytest

from trpc_sdk.runtime._common import _readiness
from trpc_sdk.utils import TimerScope


class _Host(_readiness.ChildReadiness):
    """Smallest host the mixin needs: ``loop``, ``name``, ``ready_timeout``, ``_exit_now``."""

    def __init__(self, count: int, ready_timeout: float) -> None:
        self.loop = asyncio.new_event_loop()
        self.name = "host"
        self.ready_timeout = ready_timeout
        self.exit_codes = []
        self._create_ready_flags(count)

    def _exit_now(self, exit_code: int) -> None:
        self.exit_codes.append(exit_code)
        raise SystemExit(exit_code)


@pytest.fixture
def host():
    created = []

    def _make(count: int = 2, ready_timeout: float = 5.0) -> _Host:
        created.append(_Host(count, ready_timeout))
        return created[-1]

    yield _make
    for item in created:
        item.loop.close()


def test_dead_child_aborts_startup_with_status_1_and_names_it(monkeypatch, host):
    errors = []
    monkeypatch.setattr(_readiness.logger, "error", lambda fmt, *args: errors.append(fmt % args))
    worker = host()
    read_fd, write_fd = os.pipe()
    record = SimpleNamespace(name="worker:1", process=SimpleNamespace(sentinel=read_fd, exitcode=1))
    os.close(write_fd)
    try:
        with pytest.raises(SystemExit) as exc:
            worker._wait_children_ready([record])
    finally:
        os.close(read_fd)

    assert exc.value.code == 1
    assert any("worker:1" in line for line in errors)


def test_sigterm_during_startup_exits_with_status_0(host):
    worker = host()
    worker.loop.call_soon(worker._on_sigterm_during_startup)

    with pytest.raises(SystemExit) as exc:
        worker._wait_children_ready([])

    assert exc.value.code == 0
    assert worker._startup_waiter is None


def test_zero_ready_timeout_waits_without_a_deadline(host):
    worker = host(ready_timeout=0)
    worker._ready_flags[0] = 1
    worker._ready_flags[1] = 1

    elapsed = worker._wait_children_ready([])

    assert worker.exit_codes == []
    assert elapsed >= 0


def test_not_ready_lists_unset_slots(host):
    worker = host(count=3)
    worker._ready_flags[1] = 1

    assert worker._not_ready() == [0, 2]


def test_warn_unnamed_timers_names_copies_and_unit(monkeypatch):
    warnings = []
    monkeypatch.setattr(_readiness.logger, "warning", lambda fmt, *args: warnings.append(fmt % args))
    timers = [
        SimpleNamespace(cls=type("Tick", (), {}), name="", scope=TimerScope.PROCESS),
        SimpleNamespace(cls=object, name="worker:0", scope=TimerScope.PROCESS),
        SimpleNamespace(cls=type("PerLoop", (), {}), name="", scope=TimerScope.THREAD),
    ]
    monkeypatch.setattr(_readiness, "registered_timer_tasks", lambda: timers)

    _readiness.warn_unnamed_timers("worker:0", 3, "peer workers")
    _readiness.warn_unnamed_timers("worker:0", 1, "peer workers")

    assert len(warnings) == 1
    assert "3 peer workers" in warnings[0] and "Tick" in warnings[0]
    assert "PerLoop" not in warnings[0]
