# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.
"""Test event-loop selection helpers."""
import asyncio
import sys

import uvloop

from trpc_sdk.utils import _loop_lib as loop_lib
from trpc_sdk.utils import get_async_lib
from trpc_sdk.utils import get_or_create_event_loop


def test_uvloop():
    """Test uvloop selection."""
    lib = get_async_lib(True)
    loop = lib.new_event_loop()
    try:
        assert lib is uvloop
        assert isinstance(loop, uvloop.Loop)
        assert loop_lib.asynclib is uvloop
    finally:
        loop.close()

    lib = get_async_lib(False)
    assert lib is asyncio
    assert loop_lib.asynclib is asyncio
    loop = lib.new_event_loop()
    try:
        assert isinstance(loop, asyncio.AbstractEventLoop)
    finally:
        loop.close()
        asyncio.set_event_loop_policy(asyncio.DefaultEventLoopPolicy())


def test_uvloop_error(monkeypatch):
    """Test error import"""
    monkeypatch.setitem(sys.modules, "uvloop", None)
    lib = get_async_lib(True)
    assert lib is asyncio
    assert loop_lib.asynclib is asyncio


def test_get_or_create_event_loop():
    loop = get_or_create_event_loop()
    assert isinstance(loop, asyncio.AbstractEventLoop)
