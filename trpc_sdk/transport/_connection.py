# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Shared asyncio.Protocol base for server-side IPC and client connections.

Every dispatcher / worker / executor protocol subclasses this so they share
the same two attributes: the child process that owns the event loop, and the
frame parser that turns a byte stream into complete packages.
"""

from __future__ import annotations

import asyncio
from typing import Any


class BaseConnection(asyncio.Protocol):
    """asyncio protocol that holds the owning child process and a frame parser.

    Args:
        app: The child process this protocol is attached to (dispatcher,
            worker, or executor). Protocols reach process-wide maps and the
            event loop through ``self.app`` (``loop``, ``client_connections``,
            ``worker_connections``, ``rpcz_pipe_conn``, ...). The attribute
            name is ``app`` because that is the keyword asyncio ``partial``
            factories already pass from :mod:`trpc_sdk.server`.
        parser: Frame parser instance (already ``create()``'d). Subclasses
            that accept a factory call ``checker.create()`` and pass the
            result here; subclasses that already hold a parser pass it
            directly. ``data_received`` implementations feed bytes into
            ``self.parser`` and then call ``read()`` until a complete frame
            is available.
    """

    def __init__(self, app: Any, parser: Any) -> None:
        self.app = app
        self.parser = parser
        # asyncio Transport for this protocol, set in connection_made.
        # Named aio_connection to match tcp_protocol.pyx and existing tests.
        self.aio_connection = None
