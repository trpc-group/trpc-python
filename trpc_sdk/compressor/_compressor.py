# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Built-in ContentEncoding plugin implementations.

Importing this module (which :mod:`trpc_sdk.compressor` does) registers
every built-in compressor. :class:`~trpc_sdk.compressor._types.CompressType`
and the ``compress`` / ``decompress`` / ``get_compress`` lookups live in
:mod:`trpc_sdk.compressor._types` and :mod:`trpc_sdk.compressor._utils`;
the package ``__init__`` is the single public entry point for all of them.
"""

from __future__ import annotations

import gzip
import io
import zlib
from typing import Any
from typing import Callable
from typing import Optional

from trpc_sdk.abc import BaseCompress
from trpc_sdk.abc import PluginInitPos
from trpc_sdk.abc import PluginType
from trpc_sdk.plugin import register_plugin
from ._types import CompressType


@register_plugin(PluginType.COMPRESS, CompressType.Noop, init_pos=PluginInitPos.WORKER)
class CompressNoop(BaseCompress):
    """Identity compressor registered as ``CompressType.Noop``.

    Empty or non-empty input is returned unchanged. Used when the frame
    head says the body is already raw bytes.
    """

    def compress(self, data: bytes) -> bytes:
        """Return ``data`` unchanged.

        Args:
            data: Raw body bytes.
        """
        return data

    def decompress(self, data: bytes) -> bytes:
        """Return ``data`` unchanged.

        Args:
            data: Body bytes that were stored without compression.
        """
        return data


@register_plugin(PluginType.COMPRESS, CompressType.Gzip, init_pos=PluginInitPos.WORKER)
class CompressGZip(BaseCompress):
    """gzip compressor registered as ``CompressType.Gzip``.

    Empty input is returned as-is so a zero-length body does not become
    a gzip header-only blob.
    """

    def compress(self, data: bytes) -> bytes:
        """Compress ``data`` with :func:`gzip.compress`.

        Args:
            data: Raw body bytes.
        """
        if not data:
            return data
        return gzip.compress(data)

    def decompress(self, data: bytes) -> bytes:
        """Decompress ``data`` with :func:`gzip.decompress`.

        Args:
            data: gzip-compressed body bytes.
        """
        if not data:
            return data
        return gzip.decompress(data)


@register_plugin(PluginType.COMPRESS, CompressType.ZLib, init_pos=PluginInitPos.WORKER)
class CompressZlib(BaseCompress):
    """zlib compressor registered as ``CompressType.ZLib``.

    Empty input is returned as-is so a zero-length body does not become
    a zlib header-only blob.
    """

    def compress(self, data: bytes) -> bytes:
        """Compress ``data`` with :func:`zlib.compress`.

        Args:
            data: Raw body bytes.
        """
        if not data:
            return data
        return zlib.compress(data)

    def decompress(self, data: bytes) -> bytes:
        """Decompress ``data`` with :func:`zlib.decompress`.

        Args:
            data: zlib-compressed body bytes.
        """
        if not data:
            return data
        return zlib.decompress(data)


@register_plugin(PluginType.COMPRESS, CompressType.Snappy, init_pos=PluginInitPos.WORKER)
class CompressSnappy(BaseCompress):
    """Block-mode snappy compressor registered as ``CompressType.Snappy``.

    ``python-snappy`` is imported on first use so processes that never
    send or receive snappy frames do not need the optional dependency.
    The same class is also registered as ``CompressType.SnappyBlock``.
    """

    def __init__(self) -> None:
        super().__init__()
        # Bound on first compress/decompress; stay None until then so
        # import snappy is deferred.
        self._compress: Optional[Callable[..., Any]] = None
        self._decompress: Optional[Callable[..., Any]] = None

    def _set_compress(self) -> None:
        """Bind ``snappy.compress`` the first time block compress runs."""
        if self._compress is None:
            import snappy
            self._compress = snappy.compress

    def _set_decompress(self) -> None:
        """Bind ``snappy.uncompress`` the first time block decompress runs."""
        if self._decompress is None:
            import snappy
            self._decompress = snappy.uncompress

    def compress(self, data: bytes) -> bytes:
        """Compress ``data`` with block-mode snappy.

        Args:
            data: Raw body bytes.
        """
        self._set_compress()
        if not data:
            return data
        return self._compress(data)  # pylint: disable=not-callable

    def decompress(self, data: bytes) -> bytes:
        """Decompress ``data`` with block-mode snappy.

        Args:
            data: Snappy block-compressed body bytes.
        """
        self._set_decompress()
        if not data:
            return data
        return self._decompress(data)  # pylint: disable=not-callable


# SnappyBlock (5) uses the same block implementation as the deprecated
# Snappy (2); both codes stay registered so either value decodes.
register_plugin(PluginType.COMPRESS, CompressType.SnappyBlock, init_pos=PluginInitPos.WORKER)(CompressSnappy)


@register_plugin(PluginType.COMPRESS, CompressType.SnappyStream, init_pos=PluginInitPos.WORKER)
class CompressStreamSnappy(CompressSnappy):
    """Stream-mode snappy compressor registered as ``CompressType.SnappyStream``.

    ``snappy.stream_compress`` / ``stream_decompress`` take file-like
    objects, so this subclass wraps the body in ``BytesIO`` rather than
    calling the block API used by :class:`CompressSnappy`.
    """

    def _set_compress(self) -> None:
        """Bind ``snappy.stream_compress`` the first time stream compress runs."""
        if self._compress is None:
            import snappy
            self._compress = snappy.stream_compress

    def _set_decompress(self) -> None:
        """Bind ``snappy.stream_decompress`` the first time stream decompress runs."""
        if self._decompress is None:
            import snappy
            self._decompress = snappy.stream_decompress

    def compress(self, data: bytes) -> bytes:
        """Compress ``data`` with stream-mode snappy.

        Args:
            data: Raw body bytes.
        """
        self._set_compress()
        if not data:
            return data
        src = io.BytesIO(data)
        dst = io.BytesIO()
        self._compress(src, dst)  # pylint: disable=not-callable
        return dst.getvalue()

    def decompress(self, data: bytes) -> bytes:
        """Decompress ``data`` with stream-mode snappy.

        Args:
            data: Snappy stream-compressed body bytes.
        """
        self._set_decompress()
        if not data:
            return data
        src = io.BytesIO(data)
        dst = io.BytesIO()
        self._decompress(src, dst)  # pylint: disable=not-callable
        return dst.getvalue()
