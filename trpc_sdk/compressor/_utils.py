# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Plugin lookups and body compress helpers.

``get_compress`` is a thin plugin-table lookup. Built-in compressors
register from :mod:`trpc_sdk.compressor._compressor`, which the package
``__init__`` imports. An unknown compress code returns ``None``.
"""

from __future__ import annotations

from typing import Optional

from trpc_sdk.abc import BaseCompress
from trpc_sdk.abc import PluginType
from trpc_sdk.plugin import get_plugin


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
