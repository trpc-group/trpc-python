# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Cython TCP asyncio Protocol used by merge workers and executors."""

from ._tcp_protocol import TCPConnection  # pyright: ignore[reportMissingModuleSource]
from ._tcp_protocol import handle_request  # pyright: ignore[reportMissingModuleSource]

__all__ = [
    "TCPConnection",
    "handle_request",
]
