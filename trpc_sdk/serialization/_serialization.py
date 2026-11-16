# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.
"""Built-in ContentType serializer plugins.

This is the merged copy of the historical ``serialization_json`` /
``serialization_proto`` / ``serialization_noop`` / ``serialization_jce``
/ ``serialization_wup`` modules. Importing it (which
:mod:`trpc_sdk.serialization` does) registers every built-in serializer,
"""

from __future__ import annotations

import json
from typing import Any
from typing import Union

import google.protobuf
from google.protobuf.json_format import MessageToJson
from google.protobuf.json_format import Parse
from google.protobuf.message import Message as PbMessage

from trpc_sdk.abc import BaseSerializer
from trpc_sdk.abc import PluginInitPos
from trpc_sdk.abc import PluginType
from trpc_sdk.plugin import register_plugin

from ._constants import SerializationType
from ._types import Body
from ._types import JsonData

# protobuf >= 5.26 renamed including_default_value_fields.
_PROTOBUF_VERSION = tuple(int(x) for x in google.protobuf.__version__.split(".")[:2])
_USE_NEW_PARAM = _PROTOBUF_VERSION >= (5, 26)

# Extra kwargs for MessageToJson / Parse used by JsonSerializer. Update in
# place (``pb_to_json_kwargs.update(...)``); rebinding the name after
# ``from ... import`` does not reach this module.
pb_to_json_kwargs = {}
json_to_pb_kwargs = {}


@register_plugin(PluginType.SERIALIZATION, SerializationType.JSON, init_pos=PluginInitPos.WORKER)
class JsonSerializer(BaseSerializer):
    """JSON serializer registered as ``SerializationType.JSON``.

    Accepts either a :class:`JsonData` dict wrapper or a protobuf
    ``Message``. Protobuf conversion uses :data:`pb_to_json_kwargs` /
    :data:`json_to_pb_kwargs` when they are set; otherwise it picks the
    protobuf 5.26+ vs older ``MessageToJson`` parameter names.
    """

    def _pb_serialize(self, body: PbMessage) -> bytes:
        """Serialize a protobuf message to JSON bytes.

        When no kwargs were set, protobuf < 5.26 uses
        ``including_default_value_fields`` and protobuf >= 5.26 uses
        ``always_print_fields_with_no_presence``. Both paths keep
        proto field names and integer enums.

        Args:
            body: Protobuf message to convert.

        Returns:
            UTF-8 JSON bytes.
        """
        if pb_to_json_kwargs:
            body_json = MessageToJson(body, **pb_to_json_kwargs)
        else:
            if _USE_NEW_PARAM:
                # protobuf >= 5.26.0
                body_json = MessageToJson(body,
                                          preserving_proto_field_name=True,
                                          always_print_fields_with_no_presence=True,
                                          use_integers_for_enums=True)
            else:
                # protobuf < 5.26.0
                body_json = MessageToJson(body,
                                          preserving_proto_field_name=True,
                                          including_default_value_fields=True,
                                          use_integers_for_enums=True)
        return body_json.encode("utf-8")

    def _pb_deserialize(self, data: bytes, body: PbMessage) -> None:
        """Populate a protobuf message from JSON bytes.

        Uses :data:`json_to_pb_kwargs` when present; otherwise
        ``ignore_unknown_fields=True``.

        Args:
            data: JSON bytes.
            body: Protobuf message mutated in place.
        """
        if json_to_pb_kwargs:
            Parse(data, body, **json_to_pb_kwargs)
            return
        Parse(data, body, ignore_unknown_fields=True)

    def _json_serialize(self, body: JsonData) -> bytes:
        """Serialize :class:`JsonData` with :func:`json.dumps`.

        Args:
            body: Wrapper whose ``data`` dict is dumped.

        Returns:
            UTF-8 JSON bytes.
        """
        body_json = json.dumps(body.data)
        return body_json.encode("utf-8")

    def _json_deserialize(self, data: bytes, body: JsonData) -> None:
        """Load JSON bytes into ``body.data``.

        Args:
            data: JSON bytes.
            body: Wrapper whose ``data`` field is overwritten.
        """
        body.data = json.loads(data)

    def serialize(self, body: Union[JsonData, PbMessage]) -> bytes:
        """Serialize ``body`` to JSON bytes.

        :class:`JsonData` uses :func:`json.dumps`. A protobuf message
        uses :meth:`_pb_serialize`.

        Args:
            body: :class:`JsonData` or protobuf message.

        Returns:
            UTF-8 JSON bytes.
        """
        if isinstance(body, JsonData):
            return self._json_serialize(body)

        return self._pb_serialize(body)

    def deserialize(self, data: bytes, body: Union[JsonData, PbMessage]):
        """Populate ``body`` from JSON bytes.

        Dispatches on whether ``body`` is :class:`JsonData` or a
        protobuf message.

        Args:
            data: JSON bytes.
            body: :class:`JsonData` or protobuf message to populate.
        """
        if isinstance(body, JsonData):
            return self._json_deserialize(data, body)

        return self._pb_deserialize(data, body)


@register_plugin(PluginType.SERIALIZATION, SerializationType.PB, init_pos=PluginInitPos.WORKER)
class ProtoSerializer(BaseSerializer):
    """Protobuf serializer registered as ``SerializationType.PB``.

    ``body`` must be a protobuf ``Message``: ``SerializeToString`` /
    ``ParseFromString``. This is the default ContentType on a new
    :class:`~trpc_sdk.abc.TrpcMessage`.
    """

    def serialize(self, body: Any) -> bytes:
        """Return ``body.SerializeToString()``.

        Args:
            body: Protobuf message.
        """
        return body.SerializeToString()

    def deserialize(self, data: bytes, body: Any) -> None:
        """Call ``body.ParseFromString(data)``.

        Args:
            data: Protobuf wire bytes.
            body: Protobuf message to populate.
        """
        body.ParseFromString(data)


@register_plugin(PluginType.SERIALIZATION, SerializationType.NOOP, init_pos=PluginInitPos.WORKER)
class NoopSerializer(BaseSerializer):
    """Identity serializer registered as ``SerializationType.NOOP``.

    Used when the frame already carries raw bytes (e.g. HTTP octet-stream).
    ``Body`` is the typed wrapper; a plain
    ``bytes`` object is only accepted on serialize.
    """

    def serialize(self, body: Any) -> bytes:
        """Return ``body.data`` for :class:`Body`, otherwise ``body``.

        Args:
            body: :class:`Body` or raw bytes.
        """
        if isinstance(body, Body):
            return body.data
        return body

    def deserialize(self, data: bytes, body: Any) -> None:
        """Set ``body.data = data`` when ``body`` is a :class:`Body`.

        Args:
            data: Raw body bytes.
            body: :class:`Body` to populate, or any other object.
        """
        if isinstance(body, Body):
            body.data = data
