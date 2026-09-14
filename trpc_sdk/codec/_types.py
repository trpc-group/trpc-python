# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Shared type definitions for the codec package."""

from __future__ import annotations

from dataclasses import dataclass
from dataclasses import field
from enum import IntEnum
from typing import Any
from typing import Dict
from typing import NewType
from typing import Optional

from trpc_sdk._inner import trpc_pb2
from ._constants import CompressCode
from ._constants import SerializationCode

# Annotation alias for INIT-frame proto instances cached by session/stream id.
# Runtime NewType is identity; stored values are ``TrpcStreamInitMeta()``.
StreamInitMeta = NewType("StreamInitMeta", trpc_pb2.TrpcStreamInitMeta)


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
    integers as :class:`~trpc_sdk.codec._constants.CompressCode` and are not
    proto members.

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


class _SerializationMeta(type):
    """Resolve missing ``SerializationType`` attributes on ``TrpcContentEncodeType``.

    Class-level lookup (``SerializationType.TRPC_PROTO_ENCODE``) hits
    this metaclass first. Names defined on ``SerializationType`` win;
    proto-only names such as ``TRPC_XML_ENCODE`` are forwarded so
    callers can use either spelling without importing the protobuf enum.
    """

    def __getattribute__(cls, name: str) -> Any:
        try:
            return super().__getattribute__(name)
        except AttributeError:
            if hasattr(trpc_pb2.TrpcContentEncodeType, name):
                return getattr(trpc_pb2.TrpcContentEncodeType, name)
            raise


class SerializationType(metaclass=_SerializationMeta):
    """ContentType codes with a fallback into ``TrpcContentEncodeType``.

    Attribute names ``PB`` / ``JCE`` / ``JSON`` / ``FlatBuffer`` /
    ``Noop`` are the historical Python API. 0-4 match the proto enum;
    ``Unsupported`` / ``Form`` / ``Wup`` (128 / 129 / 200) are local
    codes that are not proto members. There is no Python ``XML`` name;
    proto ``TRPC_XML_ENCODE`` is only reachable through the fallback.

    Instantiating the class is supported so ``SerializationType().PB``
    works the same way ``CompressType().TRPC_GZIP_COMPRESS`` does.
    """

    PB = SerializationCode.PROTO
    JCE = SerializationCode.JCE
    JSON = SerializationCode.JSON
    FlatBuffer = SerializationCode.FLATBUFFER
    Noop = SerializationCode.NOOP_ENCODE
    Unsupported = SerializationCode.UNSUPPORTED
    Form = SerializationCode.FORM
    Wup = SerializationCode.WUP

    def __init__(self) -> None:
        # Held on the instance so instance lookup can reach proto names
        # that are not defined as class attributes above.
        self._trpc_content_encode_type = trpc_pb2.TrpcContentEncodeType

    def __getattribute__(self, name: str) -> Any:
        """Resolve instance attrs, then fall back to ``TrpcContentEncodeType``."""
        try:
            return super().__getattribute__(name)
        except AttributeError:
            if hasattr(self._trpc_content_encode_type, name):
                return getattr(self._trpc_content_encode_type, name)
            raise


@dataclass
class JsonData:
    """Plain-dict body used with JSON ContentType.

    Distinct from a protobuf ``Message`` so the same
    ``SerializationType.JSON`` plugin can serialize either a dict
    wrapper or a proto instance.
    """

    data: Dict[str, Any] = field(default_factory=dict)


@dataclass
class Body:
    """Mutable bytes slot used with Noop ContentType.

    ``deserialize`` writes in place and ``bytes`` is immutable, so
    callers pass this wrapper and read ``data`` afterwards. A bare
    ``bytes`` value is accepted only on serialize.
    """

    data: Optional[bytes] = None


class TaskStreamErrorCode(IntEnum):
    """Framework error codes raised by the task-stream codec.

    Local to task stream; the framework-wide codes live in
    ``trpc_sdk.exceptions.TrpcErrorCode``.
    """

    TRPC_CLIENT_READ_FRAME_ERR = 171
    TRPC_CLIENT_DATA_TYPE_ERROR = 172

    TRPC_STREAM_SERVER_ENCODE_ERR = 221
    TRPC_STREAM_SERVER_DECODE_ERR = 222

    # Client-side stream encode failure.
    TRPC_STREAM_CLIENT_ENCODE_ERR = 321
    # Client-side stream decode failure.
    TRPC_STREAM_CLIENT_DECODE_ERR = 322


class TaskStreamCodecRole(IntEnum):
    """Which side of a task stream a codec instance serves.

    Member names are formatted into encode/decode error messages via
    ``role.name``, so they are part of the observable output.
    """

    CLIENT = 0
    SERVER = 1
