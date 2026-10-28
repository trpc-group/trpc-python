# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.
"""Task request records and the worker-side name to connection table."""

from trpc_sdk.task import TaskRegistry
from trpc_sdk.task import TaskRequest
from trpc_sdk.task import TaskResponse
from trpc_sdk.task import TaskStatusCode
from trpc_sdk.task import get_task_conn
from trpc_sdk.task import register_task_conn
from trpc_sdk.task import register_task_num
from trpc_sdk.utils import now_ms


def test_task_request_timeout():
    fresh = TaskRequest(1, 1, now_ms(), 0, "req")
    assert fresh.is_timeout() is False

    expired = TaskRequest(1, 1, now_ms() - 5000, 1, "req")
    assert expired.is_timeout() is True


def test_task_response_status():
    req = TaskRequest(0, 0, 0, 0, "127.0.0.1")
    assert req.is_timeout() is False
    rsp = TaskResponse(req, "123")
    assert rsp.ret_code == TaskStatusCode.SUCCESS
    assert rsp.pkg_id == req.pkg_id
    rsp.set_status_code(TaskStatusCode.TIMEOUT)
    assert rsp.ret_code == TaskStatusCode.TIMEOUT


def test_register_task_num_round_robin():
    name = "task_util_rr"
    first = object()
    second = object()
    try:
        register_task_num(name, 2)
        register_task_conn(f"{name}:0", first)
        register_task_conn(f"{name}:1", second)
        picked = [get_task_conn(name) for _ in range(4)]
        assert set(picked) == {first, second}
        assert picked[0] is not picked[1]
        assert picked[0] is picked[2]
    finally:
        TaskRegistry.nums.pop(name, None)
        TaskRegistry.cur_select.pop(name, None)
        TaskRegistry.conns.pop(f"{name}:0", None)
        TaskRegistry.conns.pop(f"{name}:1", None)


def test_exact_connection_name_wins_over_round_robin():
    name = "task_util_exact"
    conn = object()
    try:
        register_task_conn(name, conn)
        assert get_task_conn(name) is conn
        assert get_task_conn("task_util_missing") is None
    finally:
        TaskRegistry.conns.pop(name, None)
