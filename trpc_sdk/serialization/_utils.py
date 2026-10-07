# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Plugin lookups and body serialize helpers.

``get_serializer`` is a thin plugin-table lookup. Built-in serializers
register from :mod:`trpc_sdk.serialization._serialization`, which the
package ``__init__`` imports. ``serialize`` / ``deserialize`` then treat
``SerializationType.Unsupported`` as a no-op without looking up a plugin.
"""

from __future__ import annotations

from typing import Any
from typing import Optional

from trpc_sdk.abc import BaseSerializer
from trpc_sdk.abc import PluginType
from trpc_sdk.plugin import get_plugin
from ._types import SerializationType


def get_serializer(serialization_type: int) -> Optional[BaseSerializer]:
    """Return the serializer registered for ``serialization_type``.

    Args:
        serialization_type: ContentType code, typically a
            ``SerializationType`` class attribute.
    """
    return get_plugin(PluginType.SERIALIZATION, serialization_type)


def serialize(serialization_type: int, body: Any) -> bytes:
    """Serialize ``body`` with the plugin registered for ``serialization_type``.

    ``None`` and ``SerializationType.Unsupported`` return empty bytes
    without touching the plugin table. Any other code looks up the
    plugin and calls ``serialize``; an unregistered type fails there.

    Args:
        serialization_type: ContentType code.
        body: Application object, or ``None``.
    """
    empty_buffer = b""
    if body is None:
        return empty_buffer

    if serialization_type == SerializationType.Unsupported:
        return empty_buffer

    serializer = get_serializer(serialization_type)
    return serializer.serialize(body)


def deserialize(serialization_type: int, data: Optional[bytes], body: Any) -> Any:
    """Populate ``body`` from ``data`` using the registered serializer.

    ``None`` body, ``None`` / empty ``data``, and
    ``SerializationType.Unsupported`` return ``None`` without a plugin
    lookup. Otherwise the plugin's ``deserialize`` mutates ``body`` in
    place and this helper returns ``None``.

    Args:
        serialization_type: ContentType code.
        data: Serialized body bytes, or ``None``.
        body: Object to populate, or ``None``.
    """
    if body is None or not data:
        return None

    if serialization_type == SerializationType.Unsupported:
        return None

    serializer = get_serializer(serialization_type)
    serializer.deserialize(data, body)
