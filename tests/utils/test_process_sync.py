# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.
"""Tests for the cross-process sync primitives in :mod:`trpc_sdk.utils._process_sync`."""

from __future__ import annotations

import asyncio
import multiprocessing
import os
import sys

import pytest

from trpc_sdk.utils import REGISTER_STATUE
from trpc_sdk.utils import TRPC_SHARE_MEM_LOCK
from trpc_sdk.utils import RegisterStatus
from trpc_sdk.utils import get_register_status
from trpc_sdk.utils import inc_io_wait_count
from trpc_sdk.utils import io_ready_reached
from trpc_sdk.utils import pause_heartbeat
from trpc_sdk.utils import register_enable
from trpc_sdk.utils import register_spinlock
from trpc_sdk.utils import remove_lock_file
from trpc_sdk.utils import resume_heartbeat
from trpc_sdk.utils import set_register_status
from trpc_sdk.utils import should_heartbeat
from trpc_sdk.utils import wait_notify_in_io
from trpc_sdk.utils import warm_up
from trpc_sdk.utils import _process_sync


def _current_count() -> int:
    return _process_sync._io_ready_cnt.value


@pytest.fixture
def lock_dir(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    REGISTER_STATUE.value = RegisterStatus.unregister.value
    yield tmp_path
    REGISTER_STATUE.value = RegisterStatus.unregister.value


def test_counter_reaches_threshold_after_notifications() -> None:
    start = _current_count()

    assert not io_ready_reached(start + 2)
    inc_io_wait_count()
    assert not io_ready_reached(start + 2)
    inc_io_wait_count()

    assert io_ready_reached(start + 2)
    wait_notify_in_io(start + 2)


@pytest.mark.skipif(sys.platform == "win32", reason="fork is not available on Windows")
def test_counter_notification_from_forked_child_is_visible() -> None:
    start = _current_count()
    child = multiprocessing.get_context("fork").Process(target=inc_io_wait_count)

    child.start()
    child.join(5)

    assert child.exitcode == 0
    assert io_ready_reached(start + 1)


def test_register_flag_round_trip(lock_dir) -> None:
    warm_up()
    assert os.path.exists(TRPC_SHARE_MEM_LOCK)
    assert get_register_status() == RegisterStatus.unregister.value

    assert register_enable()
    assert get_register_status() == RegisterStatus.register.value

    assert set_register_status(RegisterStatus.unregister.value)
    assert get_register_status() == RegisterStatus.unregister.value

    remove_lock_file()
    assert not os.path.exists(TRPC_SHARE_MEM_LOCK)
    remove_lock_file()


def test_register_spinlock_waits_until_enabled(lock_dir) -> None:
    with pytest.raises(asyncio.TimeoutError):
        asyncio.run(asyncio.wait_for(register_spinlock(), 0.1))

    register_enable()

    asyncio.run(asyncio.wait_for(register_spinlock(), 1))


@pytest.fixture
def heartbeat_reset():
    _process_sync._heartbeat_pause_count.value = 0
    yield
    _process_sync._heartbeat_pause_count.value = 0


def test_heartbeat_needs_one_resume_per_pause(heartbeat_reset) -> None:
    assert should_heartbeat()

    pause_heartbeat()
    pause_heartbeat()
    assert not should_heartbeat()

    resume_heartbeat()
    assert not should_heartbeat()
    resume_heartbeat()
    assert should_heartbeat()


def test_heartbeat_resume_without_pause_stays_at_zero(heartbeat_reset) -> None:
    resume_heartbeat()
    assert _process_sync._heartbeat_pause_count.value == 0

    pause_heartbeat()
    assert not should_heartbeat()


@pytest.mark.skipif(sys.platform == "win32", reason="fork is not available on Windows")
def test_heartbeat_resume_from_forked_child_is_visible(heartbeat_reset) -> None:
    pause_heartbeat()
    child = multiprocessing.get_context("fork").Process(target=resume_heartbeat)

    child.start()
    child.join(5)

    assert child.exitcode == 0
    assert should_heartbeat()
