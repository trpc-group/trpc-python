# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Shared constants for the compressor package.

:class:`CompressCode` (ContentEncoding) feeds
:class:`~trpc_sdk.compressor._types.CompressType`. Values match
``TrpcCompressType`` in ``trpc.proto``.
"""

from __future__ import annotations

from enum import IntEnum
from enum import unique


@unique
class CompressCode(IntEnum):
    """ContentEncoding codes accepted by the compress plugin table.

    Members are the wire values, so they can be written into a frame head
    and compared against a decoded ``content_encoding`` without a cast.
    """

    NOOP = 0
    GZIP = 1
    SNAPPY = 2
    ZLIB = 3
    SNAPPY_STREAM = 4
    SNAPPY_BLOCK = 5
    LZ4_FRAME = 6
    LZ4_BLOCK = 7
    ZSTD = 8
