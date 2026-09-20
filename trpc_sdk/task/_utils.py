# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Worker-side helpers for talking to task child processes.

The worker never runs a task handler inline: it encodes the request, writes
it to the task's IPC pipe and parks a future until the child writes back.
This module owns that request/response pairing; the tables it reads and the
records it exchanges live in :mod:`trpc_sdk.task._types`.
"""

from asyncio import Future
from asyncio.transports import Transport
from typing import Optional

from trpc_sdk.abc import TrpcContext
from trpc_sdk.exceptions import TaskHandleError
from ._cython import inner_encode_task_req
from ._types import TaskRegistry
from ._types import TaskStatusCode


def register_task_conn(task_name: str, conn: Transport):
    """Bind a task name to the pipe that reaches its child process.

    Args:
        task_name: Task name, or ``"<name>:<index>"`` for a task with more
            than one child.
        conn: Transport wrapping the worker end of the task IPC pipe.
    """
    TaskRegistry.conns[task_name] = conn


def get_task_conn(task_name: str) -> Optional[Transport]:
    """Resolve the pipe to write the next request for a task to.

    Args:
        task_name: Task name as declared in the server config.

    Returns:
        The transport to write to, or ``None`` when nothing was registered
        under this name.

    Raises:
        KeyError: The name has a registered child count but the selected
            child never registered its connection.
    """
    # Exact names win over round-robin so that callers which registered a
    # connection themselves keep reaching it, and so that an already indexed
    # name is not expanded a second time.
    if task_name in TaskRegistry.conns:
        return TaskRegistry.conns[task_name]

    child_name = TaskRegistry.next_child_name(task_name)
    if child_name is None:
        return None
    return TaskRegistry.conns[child_name]


def register_task_num(task_name: str, task_num: int):
    """Record how many children were forked for a task.

    Args:
        task_name: Task name as declared in the server config.
        task_num: Number of children; any value above one turns lookups of
            ``task_name`` into round-robin over ``"<name>:<index>"``.
    """
    TaskRegistry.nums[task_name] = task_num


def get_task_pkg_id(conn: Transport):
    """Take the next request id for a task pipe.

    Ids are per connection and wrap at 32 bits to match the width of the
    field in the frame head.

    Args:
        conn: Transport the request will be written to.

    Returns:
        The request id to stamp on the outgoing frame.
    """
    if conn not in TaskRegistry.pkg_ids:
        TaskRegistry.pkg_ids[conn] = 1
    else:
        TaskRegistry.pkg_ids[conn] = (TaskRegistry.pkg_ids[conn] + 1) & 0xffffffff
    return TaskRegistry.pkg_ids[conn]


def send_to_task(ctx: TrpcContext, conn: Transport, req: object) -> Future:
    """Write a Python-protocol request to a task pipe.

    Args:
        ctx: Server context; supplies the receive time and deadline the task
            uses to drop work that expired while queued.
        conn: Transport reaching the task child.
        req: Request body, encoded by the task codec.

    Returns:
        Future resolved by :func:`receive_from_task` when the child answers.
    """
    pkg_id = get_task_pkg_id(conn)
    req_buf = inner_encode_task_req(pkg_id, ctx.recv_time, ctx.timeout, req)
    conn.write(req_buf)

    key = (conn, pkg_id)
    future = Future()
    TaskRegistry.rsp_futures[key] = future
    return future


def receive_from_task(conn: Transport, pkg_id: int, ret_code: int, rsp: object):
    """Resolve the caller future matching one decoded task response.

    Unknown ids are ignored rather than reported: the caller may already
    have timed out and dropped its future.

    Args:
        conn: Transport the response arrived on.
        pkg_id: Request id echoed by the child.
        ret_code: One of the :class:`TaskStatusCode` values.
        rsp: Decoded response body. A :class:`TaskHandleError` here means the
            user handler raised inside the task and the error was shipped
            back as the body, so it is re-raised as is.
    """
    key = (conn, pkg_id)
    if key in TaskRegistry.rsp_futures:
        future: Future = TaskRegistry.rsp_futures[key]
        if not future.done():
            if isinstance(rsp, TaskHandleError):
                future.set_exception(rsp)
            elif ret_code == TaskStatusCode.SUCCESS:
                future.set_result(rsp)
            elif ret_code == TaskStatusCode.TIMEOUT:
                future.set_exception(TaskHandleError("error: task wait timeout!"))
            elif ret_code == TaskStatusCode.QUEUE_FULL:
                future.set_exception(TaskHandleError("error: queue is full!"))
            elif ret_code == TaskStatusCode.RSP_WRONG_TYPE:
                future.set_exception(TaskHandleError("error: get non-list type from function return!"))
            elif ret_code == TaskStatusCode.RSP_WRONG_LENGTH:
                future.set_exception(TaskHandleError("error: get wrong length from function return!"))
            else:
                future.set_exception(TaskHandleError(f"error: unsupported ret code: {ret_code} {rsp}!"))
        del TaskRegistry.rsp_futures[key]
