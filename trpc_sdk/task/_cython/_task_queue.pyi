# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Type hints for the request / response queues of a task process.

These functions are only meaningful inside a task child: ``task_init``,
``receive_pkg_from_worker`` and ``consume_batch_rsp`` run on its main loop,
while ``consume_batch_req`` runs on the
:class:`~trpc_sdk.task.TaskConsumer` thread.

Bodies are passed through the codec installed by
:func:`trpc_sdk.task.set_default_codec`, so worker and task child must have
installed the same one before the fork. The framing itself lives in
:mod:`trpc_sdk.task._cython._task_frame`.
"""

from __future__ import annotations

from typing import Any


def task_init(capability: int) -> None:
    """Size the bounded request queue of this task child.

    Called once before the consumer thread starts. Only the request queue is
    bounded; responses are unbounded because each one corresponds to a request
    already taken off the request queue.

    Args:
        capability: Maximum number of queued requests. Beyond it,
            :func:`receive_pkg_from_worker` answers ``QUEUE_FULL`` instead of
            queueing, which is what bounds memory under a slow handler.
    """
    ...


def receive_pkg_from_worker(task_conn_index: int, package: bytes) -> None:
    """Decode one request frame and queue it for the handler.

    Runs on the task process main loop, so the body is decoded here rather
    than on the consumer thread. A request that does not fit in the queue is
    answered ``QUEUE_FULL`` immediately and never reaches the handler.

    Args:
        task_conn_index: Index of the worker pipe the frame arrived on; it is
            kept on the request so the reply takes the same route back.
        package: One complete frame, head included.

    Raises:
        ServerDecodeError: The frame does not start with the inner magic
            number.
    """
    ...


def consume_batch_req(app: Any) -> int:
    """Run one batch through a synchronous handler.

    Requests that expired while queued are answered ``TIMEOUT`` and dropped
    before the handler sees them. A handler exception is not fatal: every
    request in the batch is answered with a
    :class:`~trpc_sdk.exceptions.TaskHandleError` body so no caller waits out
    its timeout.

    Args:
        app: The :class:`~trpc_sdk.task.TaskConsumer`; supplies ``func``
            and ``batch_size``.

    Returns:
        ``0`` when a batch was handled, ``-1`` when the queue was empty or the
        whole batch had expired. The caller backs off on ``-1``.
    """
    ...


async def co_consume_batch_req(app: Any) -> int:
    """Coroutine variant of :func:`consume_batch_req`.

    Used when the registered handler is a coroutine function. Same batching,
    same failure handling, same return codes.

    Args:
        app: The :class:`~trpc_sdk.task.TaskConsumer`; supplies ``func``
            and ``batch_size``.

    Returns:
        ``0`` when a batch was handled, ``-1`` when there was nothing to do.
    """
    ...


def consume_batch_rsp() -> int:
    """Encode and write every queued response back to its worker pipe.

    Runs on the task process main loop and drains the queue in one go, which
    is what keeps the codec off the handler thread.

    Returns:
        Number of responses written; ``0`` means the queue was empty.
    """
    ...
