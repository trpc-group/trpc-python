# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Type hints for the task IPC framing.

One protocol lives here: a worker and its task child exchange a fixed 26-byte
request head and a fixed 14-byte response head, each followed by the body the
installed task codec produced. Fields are in host byte order, since both
processes are the same build on the same machine.

Every body is passed through the codec installed by
:func:`trpc_sdk.task.set_default_codec`, so worker and task child must have
installed the same one before the fork.
"""

from __future__ import annotations

from typing import Optional
from typing import Tuple


def inner_encode_task_req(pkg_id: int, recv_time: int, timeout: int, data: object) -> bytes:
    """Pack a request: 26-byte head plus the encoded body.

    Args:
        pkg_id: Per-pipe request id the reply echoes back.
        recv_time: When the worker received the originating RPC.
        timeout: Deadline carried along so the task can drop work that expired
            while queued.
        data: Request body, encoded with the installed task codec.
    """
    ...


def inner_decode_task_req(data: bytes) -> Tuple[int, int, int, Optional[object]]:
    """Unpack a request frame.

    Args:
        data: One complete frame, head included.

    Returns:
        ``(pkg_id, recv_time, timeout, obj)``. ``obj`` is ``None`` for a
        head-only frame.

    Raises:
        ServerDecodeError: The frame does not start with the inner magic
            number.
    """
    ...


def inner_encode_task_rsp(pkg_id: int, ret_code: int, data: object) -> bytes:
    """Pack a response: 14-byte head plus the encoded body.

    Args:
        pkg_id: Request id copied off the request being answered.
        ret_code: One of the :class:`~trpc_sdk.task.TaskStatusCode` values.
        data: Response body, encoded with the installed task codec.
    """
    ...


def inner_decode_task_rsp(data: bytes) -> Tuple[int, int, Optional[object]]:
    """Unpack a response frame.

    Args:
        data: One complete frame, head included.

    Returns:
        ``(pkg_id, ret_code, obj)``. When the user handler raised, ``obj`` is
        the :class:`~trpc_sdk.exceptions.TaskHandleError` the task shipped as
        the body, and ``ret_code`` is still ``SUCCESS``.

    Raises:
        ServerDecodeError: The frame does not start with the inner magic
            number.
    """
    ...
