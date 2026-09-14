# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Built-in ContentType serializer plugins.

This is the merged copy of the historical ``serialization_json`` /
``serialization_proto`` / ``serialization_noop`` / ``serialization_jce``
/ ``serialization_wup`` modules. Importing it (which :mod:`trpc_sdk.codec`
does) registers every built-in serializer, JCE (1) and WUP (200)
included.

``jcepy`` is imported on first JCE / WUP use so processes that never
send those encodings do not need the optional dependency.
"""

from __future__ import annotations

import json
from typing import Any
from typing import Dict
from typing import Optional
from typing import Union

import google.protobuf
from google.protobuf.json_format import MessageToJson
from google.protobuf.json_format import Parse
from google.protobuf.message import Message as PbMessage

from trpc_sdk.abc import BaseSerializer
from trpc_sdk.abc import PluginInitPos
from trpc_sdk.abc import PluginType
from trpc_sdk.plugin import register_plugin
from ._types import Body
from ._types import JsonData
from ._types import SerializationType

# protobuf >= 5.26 renamed including_default_value_fields.
_PROTOBUF_VERSION = tuple(int(x) for x in google.protobuf.__version__.split(".")[:2])
_USE_NEW_PARAM = _PROTOBUF_VERSION >= (5, 26)

# for protobuf to json
_pb_to_json_kwargs = {}
# for json to protobuf
_json_to_pb_kwargs = {}


def register_pb_json_format_kwargs(pb_kwargs: Optional[Dict] = None, json_kwargs: Optional[Dict] = None):
    """Merge extra keyword arguments into protobuf JSON conversion.

    Process-wide defaults used by :class:`JsonSerializer` when a protobuf
    ``Message`` is serialized to or parsed from JSON. Later calls update
    the stored dicts in place; they do not replace them.

    Args:
        pb_kwargs: Extra kwargs for ``MessageToJson`` (protobuf to JSON).
        json_kwargs: Extra kwargs for ``Parse`` (JSON to protobuf).
    """
    global _pb_to_json_kwargs
    if pb_kwargs:
        _pb_to_json_kwargs.update(pb_kwargs)
    global _json_to_pb_kwargs
    if json_kwargs:
        _json_to_pb_kwargs.update(json_kwargs)


@register_plugin(PluginType.SERIALIZATION, SerializationType.JSON, init_pos=PluginInitPos.MASTER)
class JsonSerializer(BaseSerializer):
    """JSON serializer registered as ``SerializationType.JSON``.

    Accepts either a :class:`JsonData` dict wrapper or a protobuf
    ``Message``. Protobuf conversion uses
    :func:`register_pb_json_format_kwargs` when those kwargs are set;
    otherwise it picks the protobuf 5.26+ vs older ``MessageToJson``
    parameter names.
    """

    def __init__(self) -> None:
        super().__init__()

    def _pb_serialize(self, body: PbMessage) -> str:
        """Serialize a protobuf message to JSON bytes.

        When no kwargs were registered, protobuf < 5.26 uses
        ``including_default_value_fields`` and protobuf >= 5.26 uses
        ``always_print_fields_with_no_presence``. Both paths keep
        proto field names and integer enums.

        Args:
            body: Protobuf message to convert.

        Returns:
            UTF-8 JSON bytes.
        """
        global _pb_to_json_kwargs
        if _pb_to_json_kwargs:
            body_json = MessageToJson(body, **_pb_to_json_kwargs)
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

        Uses registered ``Parse`` kwargs when present; otherwise
        ``ignore_unknown_fields=True``.

        Args:
            data: JSON bytes.
            body: Protobuf message mutated in place.
        """
        global _json_to_pb_kwargs
        if _json_to_pb_kwargs:
            Parse(data, body, **_json_to_pb_kwargs)
            return
        Parse(data, body, ignore_unknown_fields=True)

    def _json_serialize(cls, body: JsonData) -> bytes:
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


@register_plugin(PluginType.SERIALIZATION, SerializationType.PB, init_pos=PluginInitPos.MASTER)
class ProtoSerializer(BaseSerializer):
    """Protobuf serializer registered as ``SerializationType.PB``.

    ``body`` must be a protobuf ``Message``: ``SerializeToString`` /
    ``ParseFromString``. This is the default ContentType on a new
    :class:`~trpc_sdk.codec.Message`.
    """

    @classmethod
    def serialize(cls, body: Any) -> bytes:
        """Return ``body.SerializeToString()``.

        Args:
            body: Protobuf message.
        """
        return body.SerializeToString()

    @classmethod
    def deserialize(cls, data: bytes, body: Any) -> None:
        """Call ``body.ParseFromString(data)``.

        Args:
            data: Protobuf wire bytes.
            body: Protobuf message to populate.
        """
        body.ParseFromString(data)


@register_plugin(PluginType.SERIALIZATION, SerializationType.Noop, init_pos=PluginInitPos.MASTER)
class NoopSerializer(BaseSerializer):
    """Identity serializer registered as ``SerializationType.Noop``.

    Used when the frame already carries raw bytes (HTTP octet-stream,
    task-stream payloads). ``Body`` is the typed wrapper; a plain
    ``bytes`` object is only accepted on serialize.
    """

    @classmethod
    def serialize(cls, body: Any) -> bytes:
        """Return ``body.data`` for :class:`Body`, otherwise ``body``.

        Args:
            body: :class:`Body` or raw bytes.
        """
        if isinstance(body, Body):
            return body.data
        return body

    @classmethod
    def deserialize(cls, data: bytes, body: Any) -> None:
        """Set ``body.data = data`` when ``body`` is a :class:`Body`.

        Args:
            data: Raw body bytes.
            body: :class:`Body` to populate, or any other object.
        """
        if isinstance(body, Body):
            body.data = data


@register_plugin(PluginType.SERIALIZATION, SerializationType.JCE, init_pos=PluginInitPos.MASTER)
class JceSerializer(BaseSerializer):
    """JCE serializer registered as ``SerializationType.JCE``.

    ``jcepy.core.tafcore`` is imported on first use. Output uses
    ``JceOutputStream.bodySerilization`` (the jcepy spelling). Input
    skips the leading int32 tag and copies deserialized fields onto
    ``body``.
    """

    @classmethod
    def serialize(cls, body: Any) -> bytes:
        """Serialize ``body`` with a JCE output stream.

        Args:
            body: JCE-capable application object.
        """
        from jcepy.core import tafcore

        oos = tafcore.JceOutputStream()
        oos.bodySerilization(body)
        return oos.getBuffer()

    @classmethod
    def deserialize(cls, data: bytes, body: Any) -> None:
        """Populate ``body`` from JCE bytes.

        Args:
            data: JCE-encoded body bytes.
            body: Object whose attributes are overwritten from the
                deserialized result.
        """
        from jcepy.core import tafcore

        ios = tafcore.JceInputStream(data)
        ios.read(tafcore.int32, 0, True)
        res = ios.bodydeserialize(body)
        for item in res.__dict__.items():
            setattr(body, str(item[0]), item[1])


@register_plugin(PluginType.SERIALIZATION, SerializationType.Wup, init_pos=PluginInitPos.MASTER)
class WupSerializer(BaseSerializer):
    """Taf WUP serializer registered as ``SerializationType.Wup``.

    ``jcepy.core.tafcore`` is imported on first use. WUP is the Taf
    dictionary packet (``TafUniPacket``), not the raw JCE stream used
    by :class:`JceSerializer`.
    """

    @classmethod
    def serialize(cls, body: Any) -> bytes:
        """Serialize ``body`` with ``TafUniPacket.WUPserialize``.

        Args:
            body: WUP-capable application object.
        """
        from jcepy.core import tafcore

        up = tafcore.TafUniPacket()
        return up.WUPserialize(body)

    @classmethod
    def deserialize(cls, data: bytes, body: Any) -> None:
        """Populate ``body`` from a WUP packet.

        Args:
            data: WUP-encoded body bytes.
            body: Object to populate via ``WUPdeserialize``.
        """
        from jcepy.core import tafcore

        up = tafcore.TafUniPacket()
        up.deserialize(data)
        up.WUPdeserialize(body)
