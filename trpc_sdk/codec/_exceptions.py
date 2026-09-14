# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Task-stream encode / decode errors raised by the inner worker codec.

These are :class:`~trpc_sdk.exceptions.TrpcException` subclasses with
``TrpcErrorType.FRAMEWORK`` and a :class:`TaskStreamErrorCode`. They are
local to the worker-to-task stream protocol, not the public tRPC unary
path.
"""

from trpc_sdk.exceptions import TrpcErrorType
from trpc_sdk.exceptions import TrpcException
from ._types import TaskStreamErrorCode


class TaskStreamClientReadFrameError(TrpcException):
    """Worker (client) failed to read a complete task-stream frame.

    Code: ``TaskStreamErrorCode.TRPC_CLIENT_READ_FRAME_ERR``.
    """

    def __init__(self, msg):
        """Store ``msg`` as a framework error with the read-frame code.

        Args:
            msg: Human-readable failure reason.
        """
        super().__init__(TrpcErrorType.FRAMEWORK, TaskStreamErrorCode.TRPC_CLIENT_READ_FRAME_ERR, msg)


class TaskStreamClientEncodeError(TrpcException):
    """Worker (client) failed to encode a task-stream frame.

    Code: ``TaskStreamErrorCode.TRPC_STREAM_CLIENT_ENCODE_ERR``.
    """

    def __init__(self, msg):
        """Store ``msg`` as a framework error with the client-encode code.

        Args:
            msg: Human-readable failure reason.
        """
        super().__init__(TrpcErrorType.FRAMEWORK, TaskStreamErrorCode.TRPC_STREAM_CLIENT_ENCODE_ERR, msg)


class TaskStreamClientDecodeError(TrpcException):
    """Worker (client) failed to decode a task-stream frame.

    Code: ``TaskStreamErrorCode.TRPC_STREAM_CLIENT_DECODE_ERR``.
    """

    def __init__(self, msg):
        """Store ``msg`` as a framework error with the client-decode code.

        Args:
            msg: Human-readable failure reason.
        """
        super().__init__(TrpcErrorType.FRAMEWORK, TaskStreamErrorCode.TRPC_STREAM_CLIENT_DECODE_ERR, msg)


class TaskStreamServerEncodeError(TrpcException):
    """Task (server) failed to encode a task-stream frame.

    Code: ``TaskStreamErrorCode.TRPC_STREAM_SERVER_ENCODE_ERR``.
    """

    def __init__(self, msg):
        """Store ``msg`` as a framework error with the server-encode code.

        Args:
            msg: Human-readable failure reason.
        """
        super().__init__(TrpcErrorType.FRAMEWORK, TaskStreamErrorCode.TRPC_STREAM_SERVER_ENCODE_ERR, msg)


class TaskStreamServerDecodeError(TrpcException):
    """Task (server) failed to decode a task-stream frame.

    Code: ``TaskStreamErrorCode.TRPC_STREAM_SERVER_DECODE_ERR``.
    """

    def __init__(self, msg):
        """Store ``msg`` as a framework error with the server-decode code.

        Args:
            msg: Human-readable failure reason.
        """
        super().__init__(TrpcErrorType.FRAMEWORK, TaskStreamErrorCode.TRPC_STREAM_SERVER_DECODE_ERR, msg)
