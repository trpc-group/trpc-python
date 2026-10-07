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

from ._constants import SerializationCode


class SerializationType:
    """ContentType codes, aligned with ``TrpcContentEncodeType`` in ``trpc.proto``.

    ``Form`` / ``Wup`` are local codes; ``Unsupported`` is a skip sentinel.
    """

    PB = SerializationCode.PROTO
    JCE = SerializationCode.JCE
    JSON = SerializationCode.JSON
    FlatBuffer = SerializationCode.FLATBUFFER
    Noop = SerializationCode.NOOP_ENCODE
    Unsupported = SerializationCode.UNSUPPORTED
    Form = SerializationCode.FORM
    Wup = SerializationCode.WUP


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
