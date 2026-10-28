# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Which loops a serving process starts its process and thread timer executors on, and that they stop."""

from __future__ import annotations

import asyncio

import pytest

from trpc_sdk.runtime import MergeWorker
from trpc_sdk.utils import TimerScope
from trpc_sdk.utils import TimerTask
from trpc_sdk.utils import TimerType
from trpc_sdk.utils import register_timer_task
from trpc_sdk.utils import _timer


class _Idle(TimerTask):

    async def handle(self):
        pass


@pytest.fixture(autouse=True)
def timers():
    """Register one ONCE timer per scope, far enough out that it stays pending."""
    saved = list(_timer._TIMER_TASKS_INFO)
    _timer._TIMER_TASKS_INFO.clear()
    register_timer_task(type=TimerType.ONCE, interval=60)(_Idle)
    register_timer_task(type=TimerType.ONCE, interval=60, scope=TimerScope.THREAD)(_Idle)
    yield
    _timer._TIMER_TASKS_INFO[:] = saved


def _pending(executor) -> int:
    return len(executor._handles)


def test_single_loop_merge_worker_runs_both_scopes_on_the_main_loop_and_stops_them(engine_factory):
    worker = MergeWorker(engine_factory([("tcp", "trpc")]))
    worker.loop = asyncio.new_event_loop()
    try:
        worker._install_thread_model()
        worker._start_timers()
        process_timer = worker.timer
        thread_timers = list(worker.thread_timers)

        assert process_timer.loop is worker.loop and process_timer.thread_index is None
        assert [(t.loop, t.thread_index) for t in thread_timers] == [(worker.loop, 0)]
        assert _pending(process_timer) == 1 and _pending(thread_timers[0]) == 1

        worker._on_graceful_exit()

        assert worker.timer is None and worker.thread_timers == []
        assert _pending(process_timer) == 0 and _pending(thread_timers[0]) == 0
    finally:
        worker.loop.close()


def test_threaded_merge_worker_runs_one_thread_timer_per_serving_loop(engine_factory):
    worker = MergeWorker(engine_factory([("tcp", "trpc")], io_thread_num=2))
    worker.loop = asyncio.new_event_loop()
    try:
        worker._install_thread_model()
        worker._start_loop_threads()
        worker._start_timers()
        loops = [thread.loop for thread in worker.loop_threads]
        process_timer = worker.timer
        thread_timers = list(worker.thread_timers)

        assert process_timer.loop is loops[0]
        assert [(t.loop, t.thread_index) for t in thread_timers] == [(loops[0], 0), (loops[1], 1)]

        worker._on_graceful_exit()

        assert worker.timer is None and worker.thread_timers == []
        assert [_pending(t) for t in (process_timer, *thread_timers)] == [0, 0, 0]
    finally:
        worker._stop_serving_threads()
        worker.loop.close()
