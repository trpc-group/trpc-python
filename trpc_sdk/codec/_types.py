# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Shared type definitions for the codec package."""

from __future__ import annotations

from enum import IntEnum
from typing import NewType

from trpc_sdk._inner import trpc_pb2

# Annotation alias for INIT-frame proto instances cached by session/stream id.
# Runtime NewType is identity; stored values are ``TrpcStreamInitMeta()``.
StreamInitMeta = NewType("StreamInitMeta", trpc_pb2.TrpcStreamInitMeta)


class TaskStreamErrorCode(IntEnum):
    """Framework error codes raised by the task-stream codec.

    Local to task stream; the framework-wide codes live in
    ``trpc_sdk.exceptions.TrpcErrorCode``.
    """

    TRPC_CLIENT_READ_FRAME_ERR = 171
    TRPC_CLIENT_DATA_TYPE_ERROR = 172

    TRPC_STREAM_SERVER_ENCODE_ERR = 221
    TRPC_STREAM_SERVER_DECODE_ERR = 222

    # Client-side stream encode failure.
    TRPC_STREAM_CLIENT_ENCODE_ERR = 321
    # Client-side stream decode failure.
    TRPC_STREAM_CLIENT_DECODE_ERR = 322


class TaskStreamCodecRole(IntEnum):
    """Which side of a task stream a codec instance serves.

    Member names are formatted into encode/decode error messages via
    ``role.name``, so they are part of the observable output.
    """

    CLIENT = 0
    SERVER = 1
