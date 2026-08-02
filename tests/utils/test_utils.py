# -*- coding: utf-8 -*-

import asyncio

import pytest

from trpc_sdk.utils import *


@pytest.mark.asyncio
async def test_push_in_list():
    q = Queue()
    q.push("test", close=True)
    rsp = await q.pop()
    assert rsp == 'test'


@pytest.mark.asyncio
async def test_push_in_future():
    q = Queue()

    async def push():
        await asyncio.sleep(1)
        q.push("test", True)

    asyncio.create_task(push())
    rsp = await q.pop()
    assert rsp == 'test'

    try:
        await q.pop()
    except Exception as ex:  # pylint: disable=broad-except
        assert ex is not None

    q = Queue(max_count=0)
    q.push(None)


@pytest.mark.asyncio
async def test_push_with_existing_future():
    """Test push operation when future exists and is not done"""
    q = Queue()

    # Create a task that will wait for pop
    async def wait_for_pop():
        return await q.pop()

    # Start the pop operation (this creates a future)
    pop_task = asyncio.create_task(wait_for_pop())

    # Give event loop a chance to create the future
    await asyncio.sleep(0.01)

    # Push data while future exists and is not done
    q.push("test_data")

    # The pop should complete immediately with the pushed data
    result = await pop_task
    assert result == "test_data"

    # Verify future is cleared after push
    assert q.future is None
    q.push(None)
