# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""tRPC-Python client package: how one outbound call is assembled.

Public symbols are re-exported here. Callers should
``from trpc_sdk.client import ...``.

The package is split by responsibility:

- :mod:`~trpc_sdk.client._options` — ``Options`` container and per-request construction
- :mod:`~trpc_sdk.client._client` — unary invoke path: message, node select, timeout
- :mod:`~trpc_sdk.client._stream` — client-side stream implementations
- :mod:`~trpc_sdk.client._stream_factory` — stream factories; each call needs a fresh stream
- :mod:`~trpc_sdk.client._config` — deprecated shim over :class:`trpc_sdk.config.TrpcConfig`
"""

from __future__ import annotations

from ._options import Options
from ._options import get_options
from ._options import normalize_options
from ._client import Client
from ._client import get_client
from ._client import select_node
from ._client import set_timeout
from ._client import update_message
from ._stream import Stream
from ._stream_factory import create_stream

__all__ = [
    "Client",
    "Options",
    "Stream",
    "create_stream",
    "get_client",
    "get_options",
    "normalize_options",
    "select_node",
    "set_timeout",
    "update_message",
]
