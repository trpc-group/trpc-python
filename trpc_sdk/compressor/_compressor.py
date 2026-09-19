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

    @classmethod
    def compress(cls, data: bytes) -> bytes:
        """Return ``data`` unchanged.

        Args:
            data: Raw body bytes.
        """
        return data

    @classmethod
    def decompress(cls, data: bytes) -> bytes:
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

    @classmethod
    def compress(cls, data: bytes) -> bytes:
        """Compress ``data`` with :func:`gzip.compress`.

        Args:
            data: Raw body bytes.
        """
        if len(data) == 0:
            return data
        return gzip.compress(data)

    @classmethod
    def decompress(cls, data: bytes) -> bytes:
        """Decompress ``data`` with :func:`gzip.decompress`.

        Args:
            data: gzip-compressed body bytes.
        """
        if len(data) == 0:
            return data
        return gzip.decompress(data)


@register_plugin(PluginType.COMPRESS, CompressType.ZLib, init_pos=PluginInitPos.WORKER)
class CompressZlib(BaseCompress):
    """zlib compressor registered as ``CompressType.ZLib``.

    Empty input is returned as-is so a zero-length body does not become
    a zlib header-only blob.
    """

    @classmethod
    def compress(cls, data: bytes) -> bytes:
        """Compress ``data`` with :func:`zlib.compress`.

        Args:
            data: Raw body bytes.
        """
        if len(data) == 0:
            return data
        return zlib.compress(data)

    @classmethod
    def decompress(cls, data: bytes) -> bytes:
        """Decompress ``data`` with :func:`zlib.decompress`.

        Args:
            data: zlib-compressed body bytes.
        """
        if len(data) == 0:
            return data
        return zlib.decompress(data)


@register_plugin(PluginType.COMPRESS, CompressType.Snappy, init_pos=PluginInitPos.WORKER)
class CompressSnappy(BaseCompress):
    """Block-mode snappy compressor registered as ``CompressType.Snappy``.

    ``python-snappy`` is imported on first use so processes that never
    send or receive snappy frames do not need the optional dependency.
    The same class is also registered as ``TRPC_SNAPPY_BLOCK_COMPRESS``.
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


# Same block implementation as CompressType.Snappy (proto code 2).
# Slot 5 is the Go-aligned explicit block type, not in this repo's
# TrpcCompressType. Frames that write ContentEncoding=5 need this
# second registration; 2 stays for historical proto callers.
register_plugin(PluginType.COMPRESS, CompressType.TRPC_SNAPPY_BLOCK_COMPRESS,
                init_pos=PluginInitPos.WORKER)(CompressSnappy)


@register_plugin(PluginType.COMPRESS, CompressType.TRPC_SNAPPY_STREAM_COMPRESS, init_pos=PluginInitPos.WORKER)
class CompressStreamSnappy(CompressSnappy):
    """Stream-mode snappy compressor for ``TRPC_SNAPPY_STREAM_COMPRESS``.

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
