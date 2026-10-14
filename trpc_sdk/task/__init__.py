# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""tRPC-Python task process module.

A task is a child process the server forks before its event loop starts, so a
slow or blocking handler never stalls a worker. Workers do not call the handler
inline: they encode the request, write it to the task's IPC pipe and park a
future until the child writes the answer back.

The package is split by responsibility:

- :mod:`~trpc_sdk.task._task` — business-facing ``send_to_task`` that
  resolves a task name to a connection
- :mod:`~trpc_sdk.task._utils` — worker-side registration tables and the
  request/response pairing on top of them
- :mod:`~trpc_sdk.task._types` — status codes, queued request/response records
  and the process-wide registries both sides share
- :mod:`~trpc_sdk.task._default_codec` — the process-wide codec install slot
  and the pickle implementation installed by default on import; the
  :class:`~trpc_sdk.abc.TaskCodec` contract lives in ``trpc_sdk.abc``
- :mod:`~trpc_sdk.task._consumer` — the batch consumer threads and the
  response pump running inside the task child
- :mod:`~trpc_sdk.task._connection` — the asyncio protocols on both ends of
  the worker <-> task pipe, and the ``TASK_CONN`` map the response pump
  writes through
- :mod:`~trpc_sdk.task._cython` — the native frame codec and request queue

Business code normally only needs :func:`send_to_task`. The registration and
frame-level helpers are re-exported here because the runtime package drives
the task children through them.
"""

from ._default_codec import DefaultTaskCodec
from ._default_codec import get_default_codec
from ._default_codec import set_default_codec
from ._cython import co_consume_batch_req
from ._cython import consume_batch_req
from ._cython import consume_batch_rsp
from ._cython import inner_decode_task_req
from ._cython import inner_decode_task_rsp
from ._cython import inner_encode_task_req
from ._cython import inner_encode_task_rsp
from ._cython import receive_pkg_from_worker
from ._cython import task_init
from ._connection import TASK_CONN
from ._connection import TaskWorkerConnection
from ._connection import WorkerTaskConnection
from ._consumer import TaskConsumer
from ._consumer import consume_response
from ._types import TaskRegistry
from ._types import TaskRequest
from ._types import TaskResponse
from ._types import TaskStatusCode
from ._utils import get_task_conn
from ._utils import get_task_pkg_id
from ._utils import receive_from_task
from ._utils import register_task_conn
from ._utils import register_task_num
from ._task import send_to_task

__all__ = [
    # Business-facing entry points
    "send_to_task",
    # Body codec
    "DefaultTaskCodec",
    "get_default_codec",
    "set_default_codec",
    # Shared types and registries
    "TaskRegistry",
    "TaskRequest",
    "TaskResponse",
    "TaskStatusCode",
    # Worker-side registration and response pairing
    "get_task_conn",
    "get_task_pkg_id",
    "receive_from_task",
    "register_task_conn",
    "register_task_num",
    # Worker <-> task pipe protocols
    "WorkerTaskConnection",
    "TaskWorkerConnection",
    "TASK_CONN",
    # Task child process internals
    "TaskConsumer",
    "consume_response",
    "task_init",
    "receive_pkg_from_worker",
    "consume_batch_req",
    "co_consume_batch_req",
    "consume_batch_rsp",
    # Native frame codec
    "inner_encode_task_req",
    "inner_decode_task_req",
    "inner_encode_task_rsp",
    "inner_decode_task_rsp",
]
