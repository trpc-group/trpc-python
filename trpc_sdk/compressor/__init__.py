# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""tRPC-Python compressor package: ContentEncoding plugins and lookups.

Public symbols are re-exported here. Implementation lives in ``_``-prefixed
modules; callers should only ``from trpc_sdk.compressor import ...``.

The package is split by responsibility:

- :mod:`~trpc_sdk.compressor._types` — ``CompressType`` with proto fallback
- :mod:`~trpc_sdk.compressor._compressor` — built-in gzip / zlib / snappy plugins
- :mod:`~trpc_sdk.compressor._utils` — ``compress`` / ``decompress`` / ``get_compress``
- :mod:`~trpc_sdk.compressor._constants` — ``CompressCode`` wire values

Importing this package registers every built-in compressor.
"""

from __future__ import annotations

from ._compressor import CompressGZip
from ._compressor import CompressNoop
from ._compressor import CompressSnappy
from ._compressor import CompressStreamSnappy
from ._compressor import CompressZlib
from ._types import CompressType
from ._utils import compress
from ._utils import decompress
from ._utils import get_compress

__all__ = [
    "CompressType",
    "compress",
    "decompress",
    "get_compress",
    "CompressNoop",
    "CompressGZip",
    "CompressZlib",
    "CompressSnappy",
    "CompressStreamSnappy",
]
