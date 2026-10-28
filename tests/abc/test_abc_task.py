# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.
"""Test task abstract module"""
from typing import List

from trpc_sdk.abc import TaskInfo


def handler(reqs: List) -> List:
    """Handler reqs"""
    return reqs


def test_task():
    """Test register task"""
    task = TaskInfo(name="", handler=handler)
    assert task.handler is not None
    assert not task.name
    assert not task.initializer
    assert not task.batch_size
    assert not task.thread_num
    assert not task.model_path
    assert not task.model_info
    assert task.use_c_task is False
    assert not task.c_task_driver_params
    assert not task.on_fork
    assert not task.pause_heartbeat
    assert task.task_num == 0

    task = TaskInfo(name="worker", handler=handler, task_num=1)
    assert task.use_c_task is False
    assert task.task_num == 1
    assert task.handler is handler
