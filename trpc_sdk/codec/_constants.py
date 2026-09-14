# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Shared constants for the codec package.

:class:`CompressCode` (ContentEncoding) feeds
:class:`~trpc_sdk.codec._types.CompressType`. 0-3 match ``TrpcCompressType``
in this repo's ``trpc.proto``. 4-7 are local compatibility codes that
are not in that proto enum; they must not be reused when the proto is
updated.

:class:`SerializationCode` (ContentType) feeds
:class:`~trpc_sdk.codec._types.SerializationType`. 0-4 match
``TrpcContentEncodeType``. ``NOOP_ENCODE`` is 4 so it does not collide
with compress ``NOOP`` (0). 128 / 129 / 200 are local codes.

Frame-head length and ``trans_info`` keys are the single source for
both the Python modules and the module-level ``cdef`` copies that
``_c_codec.pyx`` / ``_c_codec_stream.pyx`` bind at import time.
``_codec_parser.pyx`` is the exception: it hardcodes its own
``FRAME_HEAD_LEN`` so the packet-scanning loop reads a C constant
instead of a module attribute.
"""

from __future__ import annotations

from enum import IntEnum
from enum import unique

from trpc_sdk._inner import trpc_pb2


@unique
class CompressCode(IntEnum):
    """ContentEncoding codes accepted by the compress plugin table.

    Members are the wire values, so they can be written into a frame head
    and compared against a decoded ``content_encoding`` without a cast.
    """

    # Built-in compressors; values align with TrpcCompressType.
    NOOP = 0
    GZIP = 1
    SNAPPY = 2
    ZLIB = 3

    # Local compatibility codes. Not present on TrpcCompressType; keep them
    # even if trpc.proto gains new members so existing callers keep working.
    SNAPPY_STREAM = 4
    SNAPPY_BLOCK = 5
    LZ4_FRAME = 6
    LZ4_BLOCK = 7


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


# tRPC wire frame head (stx + type + state + package_len + head_len +
# stream_id + reserved). Not the transport IPC envelope, which is 6 bytes.
FRAME_HEAD_LEN = 16

# RequestProtocol / StreamInit request_meta trans_info keys.
DYEING_KEY = "trpc-dyeing-key"
# Historical PascalCase name; callers import EnvTransfer as the key string.
EnvTransfer = "trpc-env"

# Task-stream frames reuse the tRPC magic even though task_stream_codec
# packs them by hand instead of going through c_codec_stream.
TASK_STREAM_MAGIC_VALUE = trpc_pb2.TRPC_MAGIC_VALUE
# Plugin name the task-stream codecs and checker register under; also the
# protocol name trpc.stream passes to get_client_codec / get_server_codec.
INNER_TASK_STREAM = "inner_task_stream"
