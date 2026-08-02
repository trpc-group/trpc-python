# -*- coding: utf-8 -*-
import asyncio
import multiprocessing
import os
import signal
from functools import partial

import pytest

from trpc_sdk.utils import *
from trpc_sdk.utils import timer as timer_mod
from trpc_sdk.utils.timer import (
    TimerExecutor,
    TimerProcess,
    TimerProcessInfo,
    get_register_timer_process,
    register_timer_process,
)

# Shared counters for timer-process tests (require fork start method).
_hook_before_run = multiprocessing.Value('i', 0)
_hook_after_run = multiprocessing.Value('i', 0)
_hook_signal_run = multiprocessing.Value('i', 0)
_mock_timer_task2_run_data = multiprocessing.Value('i', 0)


# 测试定时器
@pytest.mark.asyncio
async def test_abc_class():
    # 测试抽象类
    task = TimerTask()
    assert task.interval == 1
    assert task.type == TimerType.REPAET_WITH_SECOND
    assert task.name == ""
    with pytest.raises(NotImplementedError):
        await task.handle()
    with pytest.raises(TypeError):
        hook = TimerHook()
        hook.before()
        hook.after()
        hook.signal_handle()


class MockTimerTask(TimerTask):

    def __init__(self) -> None:
        super().__init__()
        self.run_data = 0

    async def handle(self):
        self.run_data += 1


@pytest.mark.asyncio
async def test_timer_executor():
    task = MockTimerTask()
    task.name = "test"

    task1 = MockTimerTask()
    task1.name = "test"
    task1.type = TimerType.ONCE

    task2 = MockTimerTask()
    # test2 和 test 不同名称，这个是不会被执行
    task2.name = "test2"

    task3 = MockTimerTask()
    task3.name = "test"
    task3.interval = 1
    task3.type = TimerType.REPAET_WITH_CRON
    task3.spec = "*/2 * * * * *"

    loop = asyncio.get_event_loop()
    executor = TimerExecutor(loop=loop, name="test", tasks=[task, task1, task2, task3])
    executor.run()
    await asyncio.sleep(5)
    # 运行多次
    assert task.run_data > 1
    assert task3.run_data > 1
    # 只运行一次
    assert task1.run_data == 1
    assert task2.run_data == 0
    executor.stop()


@register_timer_task(name="mock_test", type=TimerType.ONCE)
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
    loop = asyncio.get_event_loop()
    executor = TimerExecutor(loop=loop, name="mock_test")
    executor.run()
    await asyncio.sleep(1)
    assert MockTimerTask1.run_data == 1
    executor.stop()


class MockTimerHook(TimerHook):

    @classmethod
    def add_before_run(cls):
        _hook_before_run.value += 1

    @classmethod
    def add_after_run(cls):
        _hook_after_run.value += 1

    @classmethod
    def add_signal_handle(cls):
        _hook_signal_run.value += 1

    def before(self):
        self.add_before_run()
        print("\nMockTimerHook before\n")

    def after(self):
        self.add_after_run()
        print("\nMockTimerHook after\n")

    def signal_handle(self):
        self.add_signal_handle()
        print("\nMockTimerHook signal_handle\n")


@register_timer_task(name="timer:0", type=TimerType.ONCE)
class MockTimerTask2(TimerTask):

    @classmethod
    def add(cls):
        _mock_timer_task2_run_data.value += 1

    async def handle(self):
        self.add()


@pytest.mark.asyncio
async def test_register_timer_process():
    """Test register"""
    _hook_before_run.value = 0
    _hook_after_run.value = 0
    _hook_signal_run.value = 0
    _mock_timer_task2_run_data.value = 0

    try:
        multiprocessing.set_start_method('fork', force=True)
    except RuntimeError:
        pass

    register_timer_process(name="timer:0", hook=MockTimerHook())
    with pytest.raises(ValueError):
        register_timer_process(name="timer:0", hook=MockTimerHook())
    maps = get_register_timer_process()
    assert len(maps) == 1

    timer_info = maps["timer:0"]
    spawn_function = partial(timer_info.timer_handle, timer_info)
    timer_process = spawn_function()
    timer_process.start()
    await asyncio.sleep(2)
    if timer_process.is_alive():
        os.kill(timer_process.pid, signal.SIGTERM)
    timer_process.join(timeout=5)
    assert _mock_timer_task2_run_data.value == 1
    await asyncio.sleep(1)
    assert _hook_before_run.value == 1
    assert _hook_after_run.value == 1
    assert _hook_signal_run.value == 1

# ---- merged from test_utils_timer_extra.py ----


@pytest.fixture(autouse=True)
def _restore_timer_globals():
    old_map = dict(timer_mod._TIMER_PROCESS_INFO_MAP)
    old_pid = TimerProcess._TimerProcess__pid
    yield
    timer_mod._TIMER_PROCESS_INFO_MAP.clear()
    timer_mod._TIMER_PROCESS_INFO_MAP.update(old_map)
    TimerProcess._TimerProcess__pid = old_pid


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
    # Falls back to aiocron<2 reorder.
    assert TimerExecutor.format_cron_spec("1 2 3 4 5 6") == "2 3 4 5 6 1"


def test_gen_pid_increments():
    TimerProcess._TimerProcess__pid = -1
    assert TimerProcess.gen_pid() == 0
    assert TimerProcess.gen_pid() == 1


def test_register_timer_process_empty_name_and_duplicate():
    TimerProcess._TimerProcess__pid = 10
    register_timer_process(name="")
    maps = get_register_timer_process()
    assert "timer:11" in maps

    with pytest.raises(ValueError):
        register_timer_process(name="timer:11")


def test_timer_process_info_pid():
    info = TimerProcessInfo()

    class FakeProc:
        pid = 12345

    info.process_instance = FakeProc()
    assert info.pid == 12345


def test_timer_process_assigns_default_name():
    TimerProcess._TimerProcess__pid = 20
    info = TimerProcessInfo()
    info.name = ""
    proc = TimerProcess(info)
    assert proc.name == "timer:21"
