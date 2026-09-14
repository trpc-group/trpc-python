# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Plugin lookups, body codec helpers, and attachment accessors.

``get_compress`` / ``get_serializer`` are thin plugin-table lookups.
Built-in compressors register from :mod:`trpc_sdk.codec._compress` and
built-in serializers from :mod:`trpc_sdk.codec._serialization`, both
imported by the package ``__init__``. An unknown compress code returns
``None``; serialize/deserialize then treat
``SerializationType.Unsupported`` as a no-op without looking up a plugin.

The attachment accessors are the public read/write path for the raw
bytes carried after the body in a tRPC frame. They read the
:class:`~trpc_sdk.codec.Message` hanging off a ``TrpcContext`` rather
than taking a message directly, because that is the only object
handlers and client stubs have. ``trpc_sdk.abc`` does not import
``trpc_sdk.codec``, so ``TrpcContext`` can be imported at runtime.
"""

from __future__ import annotations

from typing import Any
from typing import Optional
from typing import Union

from trpc_sdk.abc import BaseCompress
from trpc_sdk.abc import BaseSerializer
from trpc_sdk.abc import PluginType
from trpc_sdk.abc import TrpcContext
from trpc_sdk.plugin import get_plugin
from ._message import get_message
from ._types import SerializationType


def get_compress(compress_type: int) -> Optional[BaseCompress]:
    """Return the compressor registered for ``compress_type``, or ``None``.

    Args:
        compress_type: ContentEncoding code, typically a ``CompressType``
            class attribute.
    """
    return get_plugin(PluginType.COMPRESS, compress_type)


def compress(compress_type: int, data: bytes) -> bytes:
    """Compress ``data`` with the plugin registered for ``compress_type``.

    Args:
        compress_type: ContentEncoding code.
        data: Raw body bytes.

    Returns:
        Compressed bytes from the plugin's ``compress`` method.
    """
    compressor = get_compress(compress_type)
    return compressor.compress(data)


def decompress(compress_type: int, data: bytes) -> bytes:
    """Decompress ``data`` with the plugin registered for ``compress_type``.

    Args:
        compress_type: ContentEncoding code.
        data: Compressed body bytes.

    Returns:
        Decompressed bytes from the plugin's ``decompress`` method.
    """
    compressor = get_compress(compress_type)
    return compressor.decompress(data)


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


def deserialize(serialization_type: int, data: bytes, body: Any) -> Any:
    """Populate ``body`` from ``data`` using the registered serializer.

    ``None`` body, ``None`` / empty ``data``, and
    ``SerializationType.Unsupported`` return ``None`` without a plugin
    lookup. Otherwise the plugin's ``deserialize`` mutates ``body`` in
    place and this helper returns ``None``.

    Args:
        serialization_type: ContentType code.
        data: Serialized body bytes.
        body: Object to populate, or ``None``.
    """
    if body is None:
        return None

    if data is None:
        return None

    if len(data) == 0:
        return None

    if serialization_type == SerializationType.Unsupported:
        return None

    serializer = get_serializer(serialization_type)
    serializer.deserialize(data, body)


def set_request_attachment(ctx: TrpcContext, attachment: Union[bytes, str]) -> None:
    """Attach raw bytes to the outgoing request frame.

    Args:
        ctx: TrpcContext holding the current :class:`Message`.
        attachment: Bytes, or a string encoded as UTF-8 first.
    """
    message = get_message(ctx)
    if isinstance(attachment, str):
        attachment = attachment.encode('utf-8')
    message.set_request_attachment(attachment)


def get_request_attachment(ctx: TrpcContext, is_str: bool = False) -> Union[bytes, str]:
    """Return the attachment carried by the request frame.

    Args:
        ctx: TrpcContext holding the current :class:`Message`.
        is_str: Decode the bytes as UTF-8 before returning.
    """
    message = get_message(ctx)
    if is_str:
        return message.request_attachment.decode('utf-8')
    return message.request_attachment


def set_response_attachment(ctx: TrpcContext, attachment: Union[bytes, str]) -> None:
    """Attach raw bytes to the outgoing response frame.

    Args:
        ctx: TrpcContext holding the current :class:`Message`.
        attachment: Bytes, or a string encoded as UTF-8 first.
    """
    message = get_message(ctx)
    if isinstance(attachment, str):
        attachment = attachment.encode('utf-8')
    message.set_response_attachment(attachment)


def get_response_attachment(ctx: TrpcContext, is_str: bool = False) -> Union[bytes, str]:
    """Return the attachment carried by the response frame.

    Args:
        ctx: TrpcContext holding the current :class:`Message`.
        is_str: Decode the bytes as UTF-8 before returning.
    """
    message = get_message(ctx)
    if is_str:
        return message.response_attachment.decode('utf-8')
    return message.response_attachment
