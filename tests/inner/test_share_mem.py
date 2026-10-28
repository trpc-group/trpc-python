# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.
"""Test meme share"""
import multiprocessing

import pytest

from trpc_sdk.utils import RegisterStatus
from trpc_sdk.utils import get_register_status
from trpc_sdk.utils import inc_io_wait_count
from trpc_sdk.utils import register_enable
from trpc_sdk.utils import register_spinlock
from trpc_sdk.utils import remove_lock_file
from trpc_sdk.utils import set_register_status
from trpc_sdk.utils import wait_notify_in_io
from trpc_sdk.utils import warm_up


def test_get_register_status():
    """Test register"""
    warm_up()
    set_register_status(0)
    assert get_register_status() == 0
    remove_lock_file()


def test_set_register_status():
    """Test set register"""
    warm_up()
    set_register_status(0)
    assert get_register_status() == 0
    set_register_status(1)
    assert get_register_status() == 1
    remove_lock_file()


def test_register_enable():
    """Test register enable"""
    warm_up()
    set_register_status(0)
    register_enable()
    assert get_register_status() == RegisterStatus.register.value
    remove_lock_file()


@pytest.mark.asyncio
async def test_register_loop():
    """Test register loop"""
    warm_up()
    register_enable()
    await register_spinlock()
    remove_lock_file()


def _func(interval: int):
    if not interval:
        print("wait for dispatcher")
        wait_notify_in_io(3)
        print("wait for dispatcher end")
        return
    print("inc_io_wait_count")
    inc_io_wait_count()


def test_dispatcher_wait():
    """Test dispatcher wait"""
    multiprocessing.set_start_method("fork", force=True)
    with multiprocessing.Pool(5) as pool:
        pool.map(_func, [0, 1, 2, 3])
