# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

from __future__ import annotations

import asyncio
import threading

import pytest

from trpc_sdk.utils import TimerExecutor
from trpc_sdk.utils import TimerScope
from trpc_sdk.utils import TimerTask
from trpc_sdk.utils import TimerTaskInfo
from trpc_sdk.utils import TimerType
from trpc_sdk.utils import register_timer_task
from trpc_sdk.utils import _timer


@pytest.fixture(autouse=True)
def registry():
    """Give each test an empty timer registry and restore the real one afterwards."""
    saved = list(_timer._TIMER_TASKS_INFO)
    _timer._TIMER_TASKS_INFO.clear()
    yield _timer._TIMER_TASKS_INFO
    _timer._TIMER_TASKS_INFO[:] = saved


class _Counting(TimerTask):
    runs = 0

    async def handle(self):
        type(self).runs += 1


def _register(**kwargs) -> type:
    cls = type("Tick", (_Counting,), {"runs": 0})
    register_timer_task(**kwargs)(cls)
    return cls


@pytest.mark.parametrize("info, thread_index, accepted", [
    (TimerTaskInfo(), None, True),
    (TimerTaskInfo(), 0, False),
    (TimerTaskInfo(name="worker:0"), None, True),
    (TimerTaskInfo(name="worker:1"), None, False),
    (TimerTaskInfo(scope=TimerScope.THREAD), None, False),
    (TimerTaskInfo(scope=TimerScope.THREAD), 0, True),
    (TimerTaskInfo(scope=TimerScope.THREAD), 3, True),
    (TimerTaskInfo(scope=TimerScope.THREAD, thread=1), 1, True),
    (TimerTaskInfo(scope=TimerScope.THREAD, thread=1), 0, False),
    (TimerTaskInfo(scope=TimerScope.THREAD, name="worker:1", thread=0), 0, False),
])
def test_accepts_matches_scope_name_and_thread(info, thread_index, accepted):
    executor = TimerExecutor(loop=None, name="worker:0", thread_index=thread_index)

    assert executor._accepts(info) is accepted


def test_factories_set_the_scope():
    loop = object()

    assert TimerExecutor.for_process(loop, "worker:0").thread_index is None
    assert TimerExecutor.for_thread(loop, "worker:0", 2).thread_index == 2


def test_register_rejects_thread_for_process_scope(registry):
    with pytest.raises(ValueError):
        register_timer_task(thread=0)
    assert registry == []


def test_register_records_scope_and_thread(registry):
    cls = _register(scope=TimerScope.THREAD, thread=1)

    assert registry == [TimerTaskInfo(cls=cls, scope=TimerScope.THREAD, thread=1)]


def test_only_accepted_registrations_are_instantiated():
    process_cls = _register()
    thread_cls = _register(scope=TimerScope.THREAD)
    loop = asyncio.new_event_loop()
    try:
        executor = TimerExecutor.for_thread(loop, "worker:0", 0)
        executor.add_register_task()
        executor.add_register_task()

        assert [type(task) for task in executor._tasks] == [thread_cls]
        assert executor._tasks[0].scope is TimerScope.THREAD
        assert executor._tasks[0].loop is loop
        assert process_cls not in {type(task) for task in executor._tasks}
    finally:
        loop.close()


def test_run_twice_schedules_once():
    _register(interval=60)
    loop = asyncio.new_event_loop()
    try:
        executor = TimerExecutor.for_process(loop, "worker:0")
        executor.run()
        executor.run()

        assert len(executor._tasks) == 1
        assert len(executor._timers) == 1
        executor.stop()
    finally:
        loop.close()


def test_stop_cancels_a_once_task_not_fired_yet():
    cls = _register(type=TimerType.ONCE, interval=0.05)
    loop = asyncio.new_event_loop()
    try:
        executor = TimerExecutor.for_process(loop, "worker:0")
        executor.run()
        assert len(executor._handles) == 1

        executor.stop()
        loop.run_until_complete(asyncio.sleep(0.15))

        assert cls.runs == 0
        assert executor._handles == []
    finally:
        loop.close()


def test_once_task_fires_and_forgets_its_handle():
    cls = _register(type=TimerType.ONCE, interval=0.01)
    loop = asyncio.new_event_loop()
    try:
        executor = TimerExecutor.for_process(loop, "worker:0")
        executor.run()
        loop.run_until_complete(asyncio.sleep(0.1))

        assert cls.runs == 1
        assert executor._handles == []
    finally:
        loop.close()


def test_stop_from_another_thread_runs_on_the_owner_loop():
    stopped_on = []

    class _Recorded(_Counting):
        def stop(self):
            stopped_on.append(threading.get_ident())
            super().stop()

    register_timer_task(interval=60)(_Recorded)
    loop = asyncio.new_event_loop()
    started = threading.Event()
    owner = {}

    def _serve():
        asyncio.set_event_loop(loop)
        owner["tid"] = threading.get_ident()
        loop.call_soon(started.set)
        loop.run_forever()

    thread = threading.Thread(target=_serve)
    thread.start()
    executor = TimerExecutor.for_process(loop, "worker:0")
    try:
        assert started.wait(5)
        asyncio.run_coroutine_threadsafe(asyncio.sleep(0), loop).result(5)
        loop.call_soon_threadsafe(executor.run)
        asyncio.run_coroutine_threadsafe(asyncio.sleep(0), loop).result(5)

        executor.stop()
        asyncio.run_coroutine_threadsafe(asyncio.sleep(0), loop).result(5)

        # ``_Recorded.start`` calls ``stop`` once on the owner thread too.
        assert stopped_on and set(stopped_on) == {owner["tid"]}
        assert executor._timers == []
    finally:
        loop.call_soon_threadsafe(loop.stop)
        thread.join(5)
        loop.close()


def test_stop_in_a_forked_copy_only_forgets_the_handles():
    cls = _register(type=TimerType.ONCE, interval=0.05)
    loop = asyncio.new_event_loop()
    try:
        executor = TimerExecutor.for_process(loop, "worker:0")
        executor.run()
        executor._owner_pid = -1

        executor.stop()
        assert executor._handles == []
        loop.run_until_complete(asyncio.sleep(0.15))

        assert cls.runs == 1
    finally:
        loop.close()


def test_stop_before_run_is_a_no_op():
    TimerExecutor.for_process(None, "worker:0").stop()


def test_format_cron_spec_is_unchanged(monkeypatch):
    monkeypatch.setattr("importlib.metadata.version", lambda name: "1.8.0")

    assert TimerExecutor.format_cron_spec("*/5 * * * *") == "*/5 * * * *"
    assert TimerExecutor.format_cron_spec("1 2 3 4 5 6") == "2 3 4 5 6 1"
