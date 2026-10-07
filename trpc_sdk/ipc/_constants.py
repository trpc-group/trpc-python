# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Constants for tRPC-Python IPC primitives."""

from __future__ import annotations

from enum import IntEnum
from enum import unique


@unique
class PipeEnd(IntEnum):
    """Which end of a pre-fork socketpair this process owns.

    The pair is created in the parent before fork. After fork each process
    keeps one end and must close the other so the peer can observe EOF.
    Values stay ``1`` / ``2`` so integer comparisons keep working without
    a cast.
    """

    # socket_pair[0]; typically kept by the worker / child.
    LEFT = 1
    # socket_pair[1]; typically kept by the dispatcher / parent.
    RIGHT = 2


@unique
class PipeKind(IntEnum):
    """Socketpair protocol passed to ``create_pipe()``.

    Both kinds are Unix-domain pairs. TCP selects ``SOCK_STREAM``; UDP
    selects ``SOCK_DGRAM``, which the C dispatcher uses for req/rsp IPC.
    Numeric values: TCP = 1 (``SOCK_STREAM``), UDP = 2 (``SOCK_DGRAM``).
    """

    TCP = 1
    UDP = 2
