# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.
"""Worker-side send_to_task and the pipe that carries the reply back."""

import pytest

from trpc_sdk.abc import TrpcContext
from trpc_sdk.exceptions import TaskHandleError
from trpc_sdk.task import TaskConsumer
from trpc_sdk.task import TaskRegistry
from trpc_sdk.task import TaskStatusCode
from trpc_sdk.task import WorkerTaskConnection
from trpc_sdk.task import inner_decode_task_req
from trpc_sdk.task import inner_encode_task_rsp
from trpc_sdk.task import register_task_conn
from trpc_sdk.task import register_task_num
from trpc_sdk.task import send_to_task
from trpc_sdk.utils import now_ms


class _Conn:
    def __init__(self):
        self.buf = b""
        self.closed = False

    def write(self, buf):
        self.buf += buf

    def is_closing(self):
        return self.closed

    def close(self):
        self.closed = True


def _ctx():
    ctx = TrpcContext()
    ctx.recv_time = now_ms()
    ctx.timeout = 30000
    return ctx


def test_send_to_task_without_a_connection_fails():
    with pytest.raises(TaskHandleError, match="task conn cannot find"):
        send_to_task(_ctx(), "task_missing_name", "123")


def test_task_consumer_can_be_stopped_before_start():
    consumer = TaskConsumer(lambda reqs: reqs, wait_interval=1, batch_size=1, daemon=True)
    assert consumer.daemon is True
    consumer.terminate()
    assert consumer._running is False


@pytest.mark.asyncio
async def test_send_to_task_round_trip():
    name = "task_round_trip"
    conn = _Conn()
    register_task_conn(name, conn)
    try:
        future = send_to_task(_ctx(), name, "123")
        pkg_id, _recv, _timeout, obj = inner_decode_task_req(conn.buf)
        assert obj == "123"

        protocol = WorkerTaskConnection()
        protocol.connection_made(conn)
        protocol.data_received(inner_encode_task_rsp(pkg_id, TaskStatusCode.SUCCESS, "task handle ok-123"))
        assert await future == "task handle ok-123"
    finally:
        TaskRegistry.conns.pop(name, None)
        TaskRegistry.pkg_ids.pop(conn, None)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("code", "match"),
    [
        (TaskStatusCode.TIMEOUT, "task wait timeout"),
        (TaskStatusCode.QUEUE_FULL, "queue is full"),
        (TaskStatusCode.RSP_WRONG_TYPE, "non-list type"),
        (TaskStatusCode.RSP_WRONG_LENGTH, "wrong length"),
    ],
)
async def test_receive_maps_status_codes(code, match):
    name = f"task_status_{int(code)}"
    conn = _Conn()
    register_task_conn(name, conn)
    try:
        future = send_to_task(_ctx(), name, "123")
        pkg_id, _recv, _timeout, _obj = inner_decode_task_req(conn.buf)
        protocol = WorkerTaskConnection()
        protocol.connection_made(conn)
        protocol.data_received(inner_encode_task_rsp(pkg_id, code, None))
        with pytest.raises(TaskHandleError, match=match):
            await future
    finally:
        TaskRegistry.conns.pop(name, None)
        TaskRegistry.pkg_ids.pop(conn, None)


@pytest.mark.asyncio
async def test_handler_error_is_reraised():
    name = "task_handler_error"
    conn = _Conn()
    register_task_conn(name, conn)
    try:
        future = send_to_task(_ctx(), name, "123")
        pkg_id, _recv, _timeout, _obj = inner_decode_task_req(conn.buf)
        protocol = WorkerTaskConnection()
        protocol.connection_made(conn)
        protocol.data_received(inner_encode_task_rsp(pkg_id, TaskStatusCode.SUCCESS, TaskHandleError("test exception")))
        with pytest.raises(TaskHandleError, match="test exception"):
            await future
    finally:
        TaskRegistry.conns.pop(name, None)
        TaskRegistry.pkg_ids.pop(conn, None)


@pytest.mark.asyncio
async def test_task_num_round_robin_writes_a_child_pipe():
    name = "task_num_children"
    child = _Conn()
    register_task_num(name, 1)
    register_task_conn(f"{name}:0", child)
    try:
        future = send_to_task(_ctx(), name, "123")
        assert inner_decode_task_req(child.buf)[-1] == "123"
        future.cancel()
    finally:
        TaskRegistry.nums.pop(name, None)
        TaskRegistry.cur_select.pop(name, None)
        TaskRegistry.conns.pop(f"{name}:0", None)
        TaskRegistry.pkg_ids.pop(child, None)
        TaskRegistry.rsp_futures.clear()


def test_connection_lost_closes_the_pipe():
    protocol = WorkerTaskConnection()
    conn = _Conn()
    protocol.connection_made(conn)
    protocol.connection_lost(RuntimeError("gone"))
    assert conn.closed is True

    closed = _Conn()
    closed.closed = True
    protocol.connection_made(closed)
    protocol.connection_lost(None)
    assert closed.closed is True
