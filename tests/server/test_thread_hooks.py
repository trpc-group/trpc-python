# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.
"""Per-serving-loop start/stop hooks."""

from __future__ import annotations

import asyncio
from typing import List

import pytest

from trpc_sdk.runtime import HandlerThreadInfo
from trpc_sdk.runtime import current_handler_thread
from trpc_sdk.runtime import register_handler_thread_hook
from trpc_sdk.runtime._threadmodel import _thread_hooks
from trpc_sdk.runtime._threadmodel._thread_hooks import freeze_handler_thread_hooks
from trpc_sdk.runtime._threadmodel._thread_hooks import run_start_hooks
from trpc_sdk.runtime._threadmodel._thread_hooks import run_stop_hooks
from trpc_sdk.runtime._threadmodel._thread_hooks import set_current_handler_thread


@pytest.fixture(autouse=True)
def fresh_registry(monkeypatch):
    monkeypatch.setattr(_thread_hooks, "_hooks", [])
    monkeypatch.setattr(_thread_hooks, "_frozen", False)


def _info() -> HandlerThreadInfo:
    return HandlerThreadInfo(index=0, count=1, loop=asyncio.get_running_loop())


async def test_start_in_order_stop_in_reverse_sync_and_async():
    calls: List[str] = []

    async def start_a(info):
        calls.append("start_a")
        info.state["a"] = 1

    def stop_a(info):
        calls.append("stop_a")

    def start_b(info):
        calls.append("start_b")

    async def stop_b(info):
        calls.append("stop_b")

    register_handler_thread_hook(start_a, stop_a)
    register_handler_thread_hook(start_b, stop_b)
    info = _info()
    await run_start_hooks(info)
    await run_stop_hooks(info)
    assert calls == ["start_a", "start_b", "stop_b", "stop_a"]
    assert info.state == {"a": 1}


async def test_hooks_may_be_one_sided():
    calls: List[str] = []
    register_handler_thread_hook(on_start=lambda info: calls.append("start"))
    register_handler_thread_hook(on_stop=lambda info: calls.append("stop"))
    info = _info()
    await run_start_hooks(info)
    await run_stop_hooks(info)
    assert calls == ["start", "stop"]


def test_register_after_serve_raises():
    freeze_handler_thread_hooks()
    with pytest.raises(RuntimeError):
        register_handler_thread_hook(on_start=lambda info: None)


async def test_failing_start_hook_propagates_and_stops_the_chain():
    calls: List[str] = []

    def boom(info):
        raise ValueError("no redis")

    register_handler_thread_hook(on_start=boom)
    register_handler_thread_hook(on_start=lambda info: calls.append("later"))
    with pytest.raises(ValueError):
        await run_start_hooks(_info())
    assert calls == []


async def test_failing_stop_hook_is_logged_and_others_still_run():
    calls: List[str] = []

    def boom(info):
        raise ValueError("close failed")

    register_handler_thread_hook(on_stop=lambda info: calls.append("first"))
    register_handler_thread_hook(on_stop=boom)
    await run_stop_hooks(_info())
    assert calls == ["first"]


async def test_current_handler_thread_is_thread_local():
    assert current_handler_thread() is None
    info = _info()
    set_current_handler_thread(info)
    try:
        assert current_handler_thread() is info
        assert await asyncio.to_thread(current_handler_thread) is None
    finally:
        set_current_handler_thread(None)
