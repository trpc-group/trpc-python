# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Synchronous wrapper for async tests that must share tRPC's event loop.

The connection pool is bound to the loop created by
:func:`trpc_sdk.utils.get_or_create_event_loop`. pytest-asyncio
starts a different loop, so tests that touch the pool cannot use
``@pytest.mark.asyncio``.
"""

from __future__ import annotations

from typing import Any
from typing import Awaitable
from typing import Callable

from ._loop_lib import get_or_create_event_loop


def run_async_test(async_func: Callable[[], Awaitable[Any]]) -> Callable[[], Any]:
    """Run an async test on the framework event loop.

    Args:
        async_func: Coroutine function with no arguments.

    Returns:
        A synchronous wrapper that pytest can collect. The wrapper is
        named ``run_async`` and does not preserve the original function
        name.

    Example:
        @run_async_test
        async def test_hello():
            assert 0 != 1
    """

    def run_async():
        loop = get_or_create_event_loop()
        loop.run_until_complete(async_func())

    return run_async
