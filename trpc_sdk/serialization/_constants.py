# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Shared constants for the serialization package.

:class:`SerializationCode` (ContentType) feeds
:class:`~trpc_sdk.serialization._types.SerializationType`. 0-4 match
``TrpcContentEncodeType``. ``NOOP_ENCODE`` is 4 so it does not collide
with compress ``NOOP`` (0). 128 / 129 / 200 are local codes.
"""

from __future__ import annotations

from enum import IntEnum
from enum import unique


@unique
class SerializationCode(IntEnum):
    """ContentType codes accepted by the serialization plugin table.

    Members are the wire values, so they can be written into a frame head
    and compared against a decoded ``content_type`` without a cast.
    """

    # Built-in serializers; 0-4 align with TrpcContentEncodeType.
    # PROTO is the proto name; SerializationType.PB is the historical alias.
    PROTO = 0
    JCE = 1
    JSON = 2
    FLATBUFFER = 3
    # Distinct from compress NOOP = 0; this is TrpcContentEncodeType 4.
    NOOP_ENCODE = 4

    # Local compatibility codes. Not present on TrpcContentEncodeType.
    UNSUPPORTED = 128
    FORM = 129
    WUP = 200
