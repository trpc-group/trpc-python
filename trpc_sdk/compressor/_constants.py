# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.
"""Shared constants for the compressor package.

:class:`CompressType` (ContentEncoding) feeds
:class:`~trpc_sdk.compressor._types.CompressType`. Values match
``TrpcCompressType`` in ``trpc.proto``.
"""

from __future__ import annotations

from enum import IntEnum
from enum import unique

from trpc_sdk._inner.pb import trpc_pb2


@unique
class CompressType(IntEnum):
    """ContentEncoding codes accepted by the compress plugin table.

    Members are the wire values, so they can be written into a frame head
    and compared against a decoded ``content_encoding`` without a cast.
    """

    NOOP = trpc_pb2.TRPC_DEFAULT_COMPRESS
    GZIP = trpc_pb2.TRPC_GZIP_COMPRESS
    SNAPPY = trpc_pb2.TRPC_SNAPPY_COMPRESS
    ZLIB = trpc_pb2.TRPC_ZLIB_COMPRESS
    SNAPPY_STREAM = trpc_pb2.TRPC_SNAPPY_STREAM_COMPRESS
    SNAPPY_BLOCK = trpc_pb2.TRPC_SNAPPY_BLOCK_COMPRESS
    LZ4_FRAME = trpc_pb2.TRPC_LZ4_FRAME_COMPRESS
    LZ4_BLOCK = trpc_pb2.TRPC_LZ4_BLOCK_COMPRESS
    ZSTD = trpc_pb2.TRPC_ZSTD_COMPRESS
