# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.
"""Compatible functions"""

import sys
import asyncio

from typing import Optional
from typing import Type
from typing import TypeVar

T = TypeVar("T")

PY_310 = sys.version_info >= (3, 10)


async def wait_for_by_loop(*args, **kwargs):
    """Compatible wait_for from asyncio.
    Args:
        *args: Arguments for asyncio.wait_for.
        **kwargs: Keyword arguments for asyncio.wait_for.
    Returns:
        The result of asyncio.wait_for.
    """
    if PY_310:
        kwargs.pop("loop", None)

    return await asyncio.wait_for(*args, **kwargs)


async def sleep_by_loop(second: float, loop: Optional[asyncio.BaseEventLoop] = None):
    """Compatible wait_for from asyncio.
    Args:
        second: Seconds to sleep.
        loop: Loop to use.
    Returns:
        The result of asyncio.sleep.
    """
    if PY_310:
        await asyncio.sleep(second)
        return
    await asyncio.sleep(second, loop=loop)


def create_object_with_loop(cls: Type[T], *args, **kwargs) -> T:
    """Create object with loop.
    Args:
        cls: Class to create.
        *args: Arguments for cls.
        **kwargs: Keyword arguments for cls.
    Returns:
        The created object.
    """

    if PY_310:
        kwargs.pop("loop", None)

    return cls(*args, **kwargs)


if sys.version_info >= (3, 8):
    import asyncio.trsock
    TransportSocket = asyncio.trsock.TransportSocket
else:
    import socket
    TransportSocket = socket.socket
