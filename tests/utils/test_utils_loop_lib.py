# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.
"""Test log module"""
import asyncio
import os

from importlib import reload

import uvloop

from trpc_sdk.utils import loop_lib


def test_uvloop():
    """Test uvloop"""
    os.environ['TRPC_UVLOOP'] = '1'

    reload(loop_lib)
    loop = loop_lib.asynclib.new_event_loop()
    assert isinstance(loop, uvloop.Loop)
    assert loop_lib.use_uvloop is True
    os.environ['TRPC_UVLOOP'] = '0'
    loop_lib.use_uvloop = False
    reload(loop_lib)
    loop = loop_lib.asynclib.new_event_loop()
    assert loop_lib.use_uvloop is False
    assert isinstance(loop, asyncio.AbstractEventLoop)
    assert loop_lib.asynclib.new_event_loop == asyncio.new_event_loop


class _MockLoop:

    def __init__(self) -> None:
        raise ImportError


def test_uvloop_error(mocker):
    """Test error import"""
    os.environ['TRPC_UVLOOP'] = '1'
    mocker.patch("uvloop.EventLoopPolicy", _MockLoop)
    reload(loop_lib)
    assert loop_lib.use_uvloop is False
    os.environ['TRPC_UVLOOP'] = '0'
