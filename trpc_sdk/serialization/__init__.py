# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""tRPC-Python serialization package: ContentType plugins and lookups.

Public symbols are re-exported here. Implementation lives in ``_``-prefixed
modules; callers should only ``from trpc_sdk.serialization import ...``.

The package is split by responsibility:

- :mod:`~trpc_sdk.serialization._types` — ``SerializationType``, ``JsonData``, ``Body``
- :mod:`~trpc_sdk.serialization._serialization` — JSON / protobuf / noop / JCE / WUP plugins
- :mod:`~trpc_sdk.serialization._utils` — ``serialize`` / ``deserialize`` / ``get_serializer``
- :mod:`~trpc_sdk.serialization._constants` — ``SerializationCode`` wire values

Importing this package registers every built-in serializer.
"""

from __future__ import annotations

from ._serialization import JceSerializer
from ._serialization import JsonSerializer
from ._serialization import NoopSerializer
from ._serialization import ProtoSerializer
from ._serialization import WupSerializer
from ._serialization import register_pb_json_format_kwargs
from ._types import Body
from ._types import JsonData
from ._types import SerializationType
from ._utils import deserialize
from ._utils import get_serializer
from ._utils import serialize

__all__ = [
    "SerializationType",
    "serialize",
    "deserialize",
    "get_serializer",
    "JsonSerializer",
    "JsonData",
    "Body",
    "register_pb_json_format_kwargs",
    "ProtoSerializer",
    "NoopSerializer",
    "JceSerializer",
    "WupSerializer",
]
