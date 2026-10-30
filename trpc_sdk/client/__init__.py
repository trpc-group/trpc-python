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

__all__ = [
    "Client",
    "Options",
    "get_client",
    "get_options",
    "normalize_options",
    "select_node",
    "set_timeout",
    "update_message",
]
