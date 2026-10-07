# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Private inner symbols re-exported for the rest of the SDK.

:class:`MemoryInspector` is resolved lazily: it pulls in tracemalloc,
and :mod:`trpc_sdk.exceptions` imports ``trpc_pb2`` from this package
while logging is still initializing.
"""

from __future__ import annotations

from importlib import import_module

from .pb import trpc_pb2

__all__ = [
    "MemoryInspector",
    "trpc_pb2",
]


def __getattr__(name: str):
    """Load :class:`MemoryInspector` on first access.

    Args:
        name: Attribute requested on this package.

    Returns:
        The requested symbol.

    Raises:
        AttributeError: If ``name`` is not a public export.
    """
    if name == "MemoryInspector":
        value = getattr(import_module(".memory_inspection", package=__name__), name)
        globals()[name] = value
        return value
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
