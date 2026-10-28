# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.
"""Test stream flow control."""

import asyncio

import pytest

from trpc_sdk.stream import ReceiveControl
from trpc_sdk.stream import SendControl


@pytest.mark.asyncio
async def test_send_control_blocks_then_wakes():
    ctrl = SendControl(init_window=0)

    async def grant():
        await asyncio.sleep(0.01)
        ctrl.update_window(10)

    asyncio.get_running_loop().create_task(grant())
    await ctrl.acquire_window(3)
    assert ctrl.available_window == 7


@pytest.mark.asyncio
async def test_send_control_close_wakes_and_raises():
    ctrl = SendControl(init_window=0)

    async def closer():
        await asyncio.sleep(0.01)
        ctrl.close_control()

    asyncio.get_running_loop().create_task(closer())
    with pytest.raises(Exception, match="stream is already closed"):
        await ctrl.acquire_window(1)
    assert ctrl.is_closed is True


@pytest.mark.asyncio
async def test_receive_control_feedback_at_quarter():
    feedbacks = []

    async def send_feedback(increment):
        feedbacks.append(increment)

    ctrl = ReceiveControl(window_size=40, send_feedback=send_feedback)
    await ctrl.on_recv(5)
    assert feedbacks == []
    assert ctrl.get_remaining_window() == 35

    await ctrl.on_recv(5)
    assert feedbacks == [10]
    assert ctrl.consumed_since_feedback == 0
    assert ctrl.get_remaining_window() == 40


@pytest.mark.asyncio
async def test_receive_control_without_feedback():
    ctrl = ReceiveControl(window_size=8, send_feedback=None)
    await ctrl.on_recv(4)
    assert ctrl.consumed_since_feedback == 0
    assert ctrl.get_remaining_window() == 8
