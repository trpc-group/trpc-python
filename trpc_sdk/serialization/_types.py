# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Shared type definitions for the serialization package."""

from __future__ import annotations

from dataclasses import dataclass
from dataclasses import field
from typing import Any
from typing import Dict
from typing import Optional

from trpc_sdk._inner import trpc_pb2
from ._constants import SerializationCode


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
