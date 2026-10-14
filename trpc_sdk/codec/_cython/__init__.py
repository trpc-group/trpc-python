# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Cython unary/stream codecs and the TCP frame parser.

The ``_``-prefixed codec types are framework internals used only by
:mod:`trpc_sdk.codec._codec`; the public codec plugins are
:class:`~trpc_sdk.codec.TrpcClientCodec` / :class:`~trpc_sdk.codec.TrpcServerCodec`.
"""

from ._codec import _TrpcClientCodec  # pyright: ignore[reportMissingModuleSource]
from ._codec import _TrpcServerCodec  # pyright: ignore[reportMissingModuleSource]
from ._codec_stream import _TrpcClientStreamCodec  # pyright: ignore[reportMissingModuleSource]
from ._codec_stream import _TrpcServerStreamCodec  # pyright: ignore[reportMissingModuleSource]
from ._codec_parser import TrpcServerProtocolParser  # pyright: ignore[reportMissingModuleSource]

__all__ = [
    "TrpcServerProtocolParser",
    "_TrpcClientCodec",
    "_TrpcClientStreamCodec",
    "_TrpcServerCodec",
    "_TrpcServerStreamCodec",
]
