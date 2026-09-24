# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Cython unary/stream codecs and the TCP frame parser.

The ``_``-prefixed codec types are framework internals used only by
:mod:`trpc_sdk.codec._codec`; the public codec plugins are
:class:`~trpc_sdk.codec.ClientCodec` / :class:`~trpc_sdk.codec.ServerCodec`.
"""

from ._codec import _ClientCodec  # pyright: ignore[reportMissingModuleSource]
from ._codec import _ServerCodec  # pyright: ignore[reportMissingModuleSource]
from ._codec_stream import _ClientStreamCodec  # pyright: ignore[reportMissingModuleSource]
from ._codec_stream import _ServerStreamCodec  # pyright: ignore[reportMissingModuleSource]
from ._codec_parser import CServerParser  # pyright: ignore[reportMissingModuleSource]

__all__ = [
    "CServerParser",
    "_ClientCodec",
    "_ClientStreamCodec",
    "_ServerCodec",
    "_ServerStreamCodec",
]
