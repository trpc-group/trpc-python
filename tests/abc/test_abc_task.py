# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.
"""Test task abstract module"""
from typing import List
from typing import NamedTuple

from trpc_sdk.abc.task import Task


def handler(reqs: List) -> List:
    """Handler reqs"""
    return reqs


class MockStreams(NamedTuple):
    """Mack stream"""
    client = "client"
    service = "service"


def test_task():
    """Test register task"""
    task = Task(handler=handler)
    assert task.handler is not None
    assert not task.name
    assert not task.initializer
    assert not task.batch_size
    assert not task.thread_num
    assert not task.model_path
    assert not task.model_info
    assert task.use_c_task is False
    assert not task.on_fork
    assert not task.pause_heartbeat
    assert not task.task_stream_handler
    assert task.task_num == 1

    task = Task(handler=handler, task_stream_handler=MockStreams())
    assert task.use_c_task is False
    assert task.task_stream_handler is not None
