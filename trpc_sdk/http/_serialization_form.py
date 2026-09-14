# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""``application/x-www-form-urlencoded`` serializer.

GET query strings and form bodies are parsed with ``parse_qs`` and
written onto a protobuf message via :func:`dict_to_protobuf`.
Serialize reuses the JSON serializer so the response side stays
JSON-shaped.
"""

from __future__ import annotations

from urllib.parse import parse_qs

from google.protobuf.message import Message as PbMessage

from trpc_sdk.abc import BaseSerializer
from trpc_sdk.abc import PluginInitPos
from trpc_sdk.abc import PluginType
from trpc_sdk.codec import SerializationType
from trpc_sdk.codec import get_serializer
from trpc_sdk.exceptions import ServerDecodeError
from trpc_sdk.plugin import register_plugin
from ._dict_pb_convertor import dict_to_protobuf


@register_plugin(PluginType.SERIALIZATION, SerializationType.Form, init_pos=PluginInitPos.MASTER)
class FormSerializer(BaseSerializer):
    """Serializer for HTTP form / query-string payloads."""

    @classmethod
    def serialize(cls, body) -> bytes:
        """Serialize ``body`` with the JSON serializer.

        Args:
            body: Object to serialize.

        Returns:
            JSON bytes.
        """
        json_serializer = get_serializer(SerializationType.JSON)
        return json_serializer.serialize(body)

    @classmethod
    def deserialize(cls, data: bytes, pb_msg: PbMessage):
        """Parse a query string onto ``pb_msg``.

        Args:
            data: ``application/x-www-form-urlencoded`` bytes.
            pb_msg: Protobuf message to fill.

        Raises:
            ServerDecodeError: ``dict_to_protobuf`` failed.
        """
        qs = parse_qs(data.decode("utf-8"))
        try:
            dict_to_protobuf(pb_msg, qs)
        except Exception as e:
            raise ServerDecodeError(f"deserialize form error: {e}") from e
