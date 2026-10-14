# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.
"""send_to_task names the cause when called on a thread without task pipes."""

from __future__ import annotations

import threading

import pytest

from trpc_sdk.task._task import send_to_task
from trpc_sdk.task._types import TaskRegistry


@pytest.fixture
def two_children(monkeypatch):
    monkeypatch.setitem(TaskRegistry.nums, "infer", 2)
    yield


def _call_on_new_thread() -> BaseException:
    caught = []

    def run() -> None:
        try:
            send_to_task(None, "infer", b"")
        except BaseException as err:  # pylint: disable=broad-except
            caught.append(err)

    thread = threading.Thread(target=run)
    thread.start()
    thread.join()
    return caught[0]


def test_thread_without_task_conns_gets_a_clear_runtime_error(two_children):
    err = _call_on_new_thread()
    assert isinstance(err, RuntimeError)
    assert "serving loop" in str(err)


def test_missing_child_on_a_serving_thread_still_raises_key_error(two_children):
    TaskRegistry.conns["other"] = object()
    try:
        with pytest.raises(KeyError):
            send_to_task(None, "infer", b"")
    finally:
        del TaskRegistry.conns["other"]
