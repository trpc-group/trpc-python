# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.
"""Python task process that replaced CTaskProcess and the C task driver."""

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from trpc_sdk.abc import ProcessType
from trpc_sdk.runtime import TaskInfo
from trpc_sdk.runtime import TaskProcess
from trpc_sdk.server import ServerOption as ServerOpts
from trpc_sdk.task import TASK_CONN
from trpc_sdk.task import TaskConsumer
from trpc_sdk.task import TaskWorkerConnection
from trpc_sdk.task import inner_encode_task_req
from trpc_sdk.utils import now_ms


def _handler(reqs):
    return ["ok-" + str(req) for req in reqs]


def _adapter():
    return SimpleNamespace(opts=ServerOpts())


def _task_info(**extra):
    info = TaskInfo(name="unit-task", task_handle=(None, _handler), task_pipe_conn=[])
    for key, value in extra.items():
        setattr(info, key, value)
    return info


def test_task_process_keeps_the_python_handler():
    """``use_c_task`` is stored, but the child is still a Python ``TaskProcess``."""
    info = _task_info(use_c_task=True, c_task_driver_params={"log_level": 4, "log_file": "./test.log"})
    process = TaskProcess(info, _adapter())

    assert process.__class__ is TaskProcess
    assert process.type == ProcessType.TASK
    assert process.task_handle is _handler
    assert process.task_initializer is None
    assert process.block is True
    assert process._controlled is False
    assert process.task_info.c_task_driver_params["log_level"] == 4
    assert process.task_info.use_c_task is True

    info.c_task_driver_params = {}
    again = TaskProcess(info, _adapter())
    assert again.task_info.c_task_driver_params == {}


def test_separate_mode_task_is_a_controlled_child():
    info = _task_info(separate_mode=True)
    process = TaskProcess(info, _adapter())
    assert process._controlled is True


def test_task_consumer_uses_the_registered_handler():
    consumer = TaskConsumer(_handler, wait_interval=100, batch_size=1)
    assert consumer.func is _handler
    assert consumer.batch_size == 1
    consumer.terminate()
    assert consumer._running is False


def test_task_worker_connection_records_the_pipe_and_ignores_garbage(monkeypatch):
    received = []
    monkeypatch.setattr(
        "trpc_sdk.task._connection.receive_pkg_from_worker",
        lambda index, package: received.append((index, package)),
    )
    conn = TaskWorkerConnection(app=None, index=0)
    transport = MagicMock()
    transport.is_closing.return_value = False
    conn.connection_made(transport)
    assert TASK_CONN[0] is transport

    frame = inner_encode_task_req(1, int(now_ms()), 1000, b"123")
    conn.data_received(frame)
    assert received == [(0, frame)]

    conn.data_received(b"123xxxxxxxxxxxxxxxxxx")
    assert received == [(0, frame)]

    conn.connection_lost(None)
    transport.close.assert_called()
    TASK_CONN.pop(0, None)
