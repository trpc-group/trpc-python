# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

import asyncio

import pytest

from trpc_sdk.abc import TimerHook
from trpc_sdk.utils import TimerExecutor
from trpc_sdk.utils import TimerScope
from trpc_sdk.utils import TimerTask
from trpc_sdk.utils import TimerType
from trpc_sdk.utils import register_timer_task
from trpc_sdk.utils._timer import _TIMER_TASKS_INFO


@pytest.fixture(autouse=True)
def _restore_timer_registry():
    saved = list(_TIMER_TASKS_INFO)
    yield
    _TIMER_TASKS_INFO.clear()
    _TIMER_TASKS_INFO.extend(saved)


@pytest.mark.asyncio
async def test_abc_class():
    task = TimerTask()
    assert task.interval == 1
    assert task.type == TimerType.REPEAT_WITH_SECOND
    assert task.name == ""
    with pytest.raises(NotImplementedError):
        await task.handle()
    with pytest.raises(TypeError):
        hook = TimerHook()
        hook.before()
        hook.after()
        hook.signal_handle()


@pytest.mark.asyncio
async def test_timer_executor():
    counts = {"repeat": 0, "once": 0, "other": 0, "cron": 0}

    @register_timer_task(name="test", type=TimerType.REPEAT_WITH_SECOND, interval=1)
    class RepeatTask(TimerTask):

        async def handle(self):
            counts["repeat"] += 1

    @register_timer_task(name="test", type=TimerType.ONCE, interval=1)
    class OnceTask(TimerTask):

        async def handle(self):
            counts["once"] += 1

    @register_timer_task(name="test2", type=TimerType.REPEAT_WITH_SECOND, interval=1)
    class OtherTask(TimerTask):

        async def handle(self):
            counts["other"] += 1

    @register_timer_task(name="test", type=TimerType.REPEAT_WITH_CRON, spec="*/2 * * * * *")
    class CronTask(TimerTask):

        async def handle(self):
            counts["cron"] += 1

    loop = asyncio.get_running_loop()
    executor = TimerExecutor(loop=loop, name="test")
    executor.run()
    await asyncio.sleep(5)
    assert counts["repeat"] > 1
    assert counts["cron"] > 1
    assert counts["once"] == 1
    assert counts["other"] == 0
    executor.stop()


@register_timer_task(name="mock_test", type=TimerType.ONCE, interval=1)
class MockTimerTask1(TimerTask):
    run_data: int = 0

    def __init__(self) -> None:
        super().__init__()

    @classmethod
    def add(cls):
        cls.run_data += 1

    async def handle(self):
        self.add()


@pytest.mark.asyncio
async def test_register_timer_task():
    MockTimerTask1.run_data = 0
    loop = asyncio.get_running_loop()
    executor = TimerExecutor(loop=loop, name="mock_test")
    executor.run()
    await asyncio.sleep(1.5)
    assert MockTimerTask1.run_data == 1
    executor.stop()


class MockTimerHook(TimerHook):

    def __init__(self) -> None:
        self.before_run = 0
        self.after_run = 0
        self.signal_run = 0

    def before(self):
        self.before_run += 1

    def after(self):
        self.after_run += 1

    def signal_handle(self):
        self.signal_run += 1


def test_timer_hook_callbacks():
    """TimerHook is the lifecycle contract that replaced TimerProcess."""
    hook = MockTimerHook()
    hook.before()
    hook.signal_handle()
    hook.after()
    assert hook.before_run == 1
    assert hook.signal_run == 1
    assert hook.after_run == 1


def test_format_cron_spec_non_six_parts():
    assert TimerExecutor.format_cron_spec("*/5 * * * *") == "*/5 * * * *"
    assert TimerExecutor.format_cron_spec("") == ""
    assert TimerExecutor.format_cron_spec("  ") == "  "


def test_format_cron_spec_aiocron_ge2(monkeypatch):
    monkeypatch.setattr(
        "importlib.metadata.version",
        lambda name: "2.1.0" if name == "aiocron" else "0",
    )
    assert TimerExecutor.format_cron_spec("1 2 3 4 5 6") == "1 2 3 4 5 6"


def test_format_cron_spec_aiocron_lt2(monkeypatch):
    monkeypatch.setattr(
        "importlib.metadata.version",
        lambda name: "1.8.0" if name == "aiocron" else "0",
    )
    assert TimerExecutor.format_cron_spec("1 2 3 4 5 6") == "2 3 4 5 6 1"


def test_format_cron_spec_version_error(monkeypatch):
    def boom(_name):
        raise RuntimeError("no metadata")

    monkeypatch.setattr("importlib.metadata.version", boom)
    assert TimerExecutor.format_cron_spec("1 2 3 4 5 6") == "2 3 4 5 6 1"


def test_register_timer_task_rejects_thread_on_process_scope():
    with pytest.raises(ValueError):
        register_timer_task(name="timer:0", scope=TimerScope.PROCESS, thread=0)
