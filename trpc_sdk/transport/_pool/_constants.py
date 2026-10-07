# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Constants used by the connection-pool package.

:data:`DEFAULT_STREAM_LIMIT` is the read buffer half-limit
:func:`~trpc_sdk.transport._pool._dial.dial_connection` passes to
``asyncio.open_connection`` / ``asyncio.open_unix_connection``.
"""

from __future__ import annotations

# Half the StreamReader buffer limit; the reader pauses the transport at
# 2 * this value. Same number asyncio itself defaults to.
DEFAULT_STREAM_LIMIT = 2**16  # 64 KiB
