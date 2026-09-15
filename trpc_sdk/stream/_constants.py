# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Constants for the tRPC streaming protocol.

Window bounds are the legal range of ``trpc_pb2.TrpcStreamInitMeta.init_window_size``.
A peer that sends ``MIN_INIT_WINDOW_SIZE`` has opted out of flow control;
the other end then leaves ``SendControl`` / ``ReceiveControl`` unset.

Stream ids ``0..99`` are reserved. Public client streams start after
:data:`STREAM_ID`; the inner worker-to-task client starts at
:data:`TASK_CLIENT_STREAM_ID` so the two id spaces cannot collide on a
shared connection.
"""

from __future__ import annotations

# Wire-format upper bound on trpc_pb2.TrpcStreamInitMeta.init_window_size (math.MaxUint32).
MAX_INIT_WINDOW_SIZE = 4294967295

# Zero means the peer opted out of flow control entirely.
MIN_INIT_WINDOW_SIZE = 0

# Fallback window when the peer's init frame carries no trpc_pb2.TrpcStreamInitMeta.
DEFAULT_INIT_WINDOW_SIZE = 65535

# Stream ids 0..99 are reserved; the first public client stream is 100.
STREAM_ID = 99

# Inner worker-to-task client starts here so ids cannot collide with STREAM_ID.
TASK_CLIENT_STREAM_ID = 1024
