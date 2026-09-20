# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Option helpers for the streaming protocol.

Window size is the one stream-level option that needs clamping before it
is written onto ``trpc_pb2.TrpcStreamInitMeta``; everything else lives on
:class:`~trpc_sdk.client.Options` / :class:`~trpc_sdk.server.ServiceOption`.
"""

from __future__ import annotations

from ._constants import MAX_INIT_WINDOW_SIZE
from ._constants import MIN_INIT_WINDOW_SIZE


def get_window_size(max_window_size: int) -> int:
    """Clamp a configured window size into the protocol's valid range.

    Args:
        max_window_size: Window size from service or client options.

    Returns:
        The clamped size. ``MIN_INIT_WINDOW_SIZE`` disables flow control;
        ``MAX_INIT_WINDOW_SIZE`` is the uint32 ceiling of the wire field.
    """
    if max_window_size <= MIN_INIT_WINDOW_SIZE:
        return MIN_INIT_WINDOW_SIZE
    if max_window_size >= MAX_INIT_WINDOW_SIZE:
        return MAX_INIT_WINDOW_SIZE
    return max_window_size
