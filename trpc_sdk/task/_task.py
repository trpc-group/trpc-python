# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Business-facing entry point for sending a request to a task process.

This wrapper resolves a task name to a connection; the encoding and the
response pairing itself live in :mod:`trpc_sdk.task._utils`.
"""

from asyncio import Future

from trpc_sdk.abc import TrpcContext
from trpc_sdk.exceptions import TaskHandleError

from ._utils import get_task_conn
from ._utils import send_to_task as send_task_to_conn


def send_to_task(ctx: TrpcContext, task_name: str, req: object) -> Future:
    """Send a request from business code to a task process.

    Args:
        ctx: Request context of the RPC being served.
        task_name: Task name as declared in the server config.
        req: Request body, encoded by the task codec.

    Returns:
        Future resolved with the task response.

    Raises:
        TaskHandleError: No task is registered under this name.
    """
    conn = get_task_conn(task_name)
    if conn is None:
        raise TaskHandleError("fatal error: task conn cannot find!")
    return send_task_to_conn(ctx, conn, req)
