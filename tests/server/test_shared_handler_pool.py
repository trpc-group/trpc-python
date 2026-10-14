# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.
"""SharedHandlerPool: stream frames are pinned by hash and bypass the limit."""

from __future__ import annotations

import asyncio
from typing import List

from trpc_sdk._inner import trpc_pb2
from trpc_sdk.runtime._threadmodel._handler_pool import HandleSlot
from trpc_sdk.runtime._threadmodel._handler_pool import SharedHandlerPool
from trpc_sdk.transport import SchedulerParams
from trpc_sdk.transport import get_next_worker
from trpc_sdk.transport import register_dispatcher_worker_func


async def _settle() -> None:
    for _ in range(5):
        await asyncio.sleep(0)


def _pool(slots: int = 3, max_concurrency: int = 0, queue_size: int = 0) -> SharedHandlerPool:
    loop = asyncio.get_running_loop()
    return SharedHandlerPool([HandleSlot(i, loop) for i in range(slots)], max_concurrency, queue_size)


async def test_stream_target_matches_the_dispatcher_rule_and_ignores_hooks():
    pool = _pool(3)
    pairs = [(s, t) for s in (1, 2, 7, 2**31 - 1) for t in (0, 1, 100, 101, 4096)]
    expected = [
        get_next_worker(SchedulerParams(worker_conn_num=3, remote_ip="", session_id=s, stream_id=t,
                                        frame_type=trpc_pb2.TRPC_STREAM_FRAME)) for s, t in pairs
    ]
    assert [pool.stream_target(s, t) for s, t in pairs] == expected
    assert [pool.stream_target(s, t) for s, t in pairs] == expected

    register_dispatcher_worker_func(lambda schedule: 0, is_scheduler=True)
    try:
        assert [pool.stream_target(s, t) for s, t in pairs] == expected
    finally:
        register_dispatcher_worker_func(None, is_scheduler=True)


async def test_submit_stream_bypasses_a_full_limit_and_queue():
    pool = _pool(2, max_concurrency=1, queue_size=0)
    gate = asyncio.Event()
    ran: List[str] = []

    async def blocker() -> None:
        await gate.wait()

    pool.submit(blocker)
    pool.submit(blocker)
    await _settle()
    # Both threads are at the limit and the queue holds nothing.
    assert [slot.in_flight for slot in pool.slots] == [1, 1]

    for name in ("init", "data", "feedback", "close"):

        async def frame(name=name) -> None:
            ran.append(name)

        pool.submit_stream(pool.stream_target(5, 101), frame)
    await _settle()
    assert ran == ["init", "data", "feedback", "close"]
    assert [slot.in_flight for slot in pool.slots] == [1, 1]
    assert pool.queued == 0
    gate.set()
    await _settle()
