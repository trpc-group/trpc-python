# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.
"""Test stream flow control."""

import asyncio

import pytest

from trpc_sdk.stream.flow_control import ReceiveControl, SendControl


@pytest.mark.asyncio
async def test_send_control_blocks_then_wakes():
    ctrl = SendControl(window=0)

    async def grant():
        await asyncio.sleep(0.01)
        ctrl.update_window(10)

    asyncio.get_event_loop().create_task(grant())
    await ctrl.get_window(3)
    assert ctrl.window == 7


@pytest.mark.asyncio
async def test_send_control_close_wakes_and_raises():
    ctrl = SendControl(window=0)

    async def closer():
        await asyncio.sleep(0.01)
        ctrl.close_control()

    asyncio.get_event_loop().create_task(closer())
    with pytest.raises(Exception, match="stream is already closed"):
        await ctrl.get_window(1)
    assert ctrl.is_closed is True


@pytest.mark.asyncio
async def test_receive_control_feedback_at_quarter():
    feedbacks = []

    async def feed_back(increment):
        feedbacks.append(increment)

    ctrl = ReceiveControl(buffer=40, feed_back=feed_back)
    await ctrl.on_recv(5)
    assert feedbacks == []
    assert ctrl.get_left() == 35

    await ctrl.on_recv(5)
    assert feedbacks == [10]
    assert ctrl.un_updated == 0
    assert ctrl.get_left() == 40


@pytest.mark.asyncio
async def test_receive_control_without_feedback():
    ctrl = ReceiveControl(buffer=8, feed_back=None)
    await ctrl.on_recv(4)
    assert ctrl.un_updated == 0
    assert ctrl.get_left() == 8
