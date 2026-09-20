# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Shared type definitions for the compressor package."""

from __future__ import annotations

from typing import Any

from trpc_sdk._inner import trpc_pb2
from ._constants import CompressCode


class _CompressMeta(type):
    """Resolve missing ``CompressType`` attributes on ``TrpcCompressType``.

    Class-level lookup (``CompressType.TRPC_DEFAULT_COMPRESS``) hits this
    metaclass first. Names defined on ``CompressType`` win; proto-only
    names such as ``TRPC_DEFAULT_COMPRESS`` are forwarded so callers can
    use either spelling without importing the protobuf enum.
    """

    def __getattribute__(cls, name: str) -> Any:
        try:
            return super().__getattribute__(name)
        except AttributeError:
            if hasattr(trpc_pb2.TrpcCompressType, name):
                return getattr(trpc_pb2.TrpcCompressType, name)
            raise


class CompressType(metaclass=_CompressMeta):
    """ContentEncoding codes with a fallback into ``TrpcCompressType``.

    Attribute names ``Noop`` / ``Gzip`` / ``Snappy`` / ``ZLib`` are the
    historical Python API. The ``TRPC_*`` names on 4-7 are the same
    integers as :class:`~trpc_sdk.compressor._constants.CompressCode` and
    are not proto members.

    Instantiating the class is supported because existing tests and
    callers do ``CompressType().TRPC_GZIP_COMPRESS``; the instance
    ``__getattribute__`` repeats the proto fallback for that path.
    """

    Noop = CompressCode.NOOP
    Gzip = CompressCode.GZIP
    Snappy = CompressCode.SNAPPY
    ZLib = CompressCode.ZLIB

    TRPC_SNAPPY_STREAM_COMPRESS = CompressCode.SNAPPY_STREAM
    TRPC_SNAPPY_BLOCK_COMPRESS = CompressCode.SNAPPY_BLOCK
    TRPC_LZ4_FRAME_COMPRESS = CompressCode.LZ4_FRAME
    TRPC_LZ4_BLOCK_COMPRESS = CompressCode.LZ4_BLOCK

    def __init__(self) -> None:
        # Held on the instance so instance lookup can reach proto names
        # that are not defined as class attributes above.
        self._trpc_compress_type = trpc_pb2.TrpcCompressType

    def __getattribute__(self, name: str) -> Any:
        """Resolve instance attrs, then fall back to ``TrpcCompressType``."""
        try:
            return super().__getattribute__(name)
        except AttributeError:
            if hasattr(self._trpc_compress_type, name):
                return getattr(self._trpc_compress_type, name)
            raise
