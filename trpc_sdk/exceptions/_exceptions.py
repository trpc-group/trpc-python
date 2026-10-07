# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Exceptions raised by the task process feature.

``TrpcErrorType`` and ``TrpcErrorCode`` stay in :mod:`trpc_sdk.exceptions`
because the codes are part of the cross-language tRPC error space; only the
exception class itself lives here.
"""

from __future__ import annotations

from ._trpc_exception import TrpcErrorCode
from ._trpc_exception import TrpcErrorType
from ._trpc_exception import TrpcException


class TaskHandleError(TrpcException):
    """Raised when a request routed to a task process fails.

    Covers both sides of the worker/task split:

    - Worker side, raised inline: no task connection registered for the
      requested name.
    - Task side, delivered through the response future: wait timeout, full
      request queue, malformed handler return value, or an exception thrown
      by the user handler. Those are encoded into the response body and
      re-raised on the worker's future by
      :func:`trpc_sdk.task.receive_from_task`.
    """

    def __init__(self, msg):
        """Initialize the task handling error.

        Args:
            msg: Description of the task processing failure. Accepts a
                non-str value (the task side passes the caught exception
                object); the base class stringifies it.
        """
        super().__init__(TrpcErrorType.FRAMEWORK, TrpcErrorCode.TRPC_TASK_HANDLE_ERR, msg)
