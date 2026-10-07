# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Shared type definitions for the compressor package."""

from __future__ import annotations

from ._constants import CompressCode


class CompressType:
    """ContentEncoding codes, aligned with ``TrpcCompressType`` in ``trpc.proto``."""

    Noop = CompressCode.NOOP
    Gzip = CompressCode.GZIP
    Snappy = CompressCode.SNAPPY
    ZLib = CompressCode.ZLIB
    SnappyStream = CompressCode.SNAPPY_STREAM
    SnappyBlock = CompressCode.SNAPPY_BLOCK
    Lz4Frame = CompressCode.LZ4_FRAME
    Lz4Block = CompressCode.LZ4_BLOCK
    Zstd = CompressCode.ZSTD
