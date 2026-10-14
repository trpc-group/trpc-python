# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""tRPC-Python protocol package: asyncio Protocol bound to a listen socket.

Public symbols are re-exported here. Callers should ``from trpc.protocol
import ...``.

This package owns the **in-process TCP Protocol** used by merge workers
and multi-listener executors: the Protocol is attached to the accepted
client socket, a per-connection parser splits the byte stream into
frames, and each complete frame is scheduled as an asyncio task that
runs the service handler and writes the response on the same
connection.

It is not the codec (what a frame means) and not the transport process
model (how frames travel between dispatcher / worker / task). Those
live in :mod:`trpc.codec` and :mod:`trpc.transport`. The UDP counterpart
on the same listen path is
:class:`trpc.transport.DefaultDatagramConnection`.

Modules:

- :mod:`~trpc_sdk.protocol._cython._tcp_protocol` — :class:`TCPConnection` and
  :func:`handle_request`
- :mod:`~trpc_sdk.protocol._conn_writer` — :class:`ConnWriter`, the handle
  that writes on a connection's transport from any thread
"""

from __future__ import annotations

from ._conn_writer import ConnWriter
from ._cython import TCPConnection  # pylint: disable=no-name-in-module
from ._cython import handle_request  # pylint: disable=no-name-in-module

__all__ = [
    "ConnWriter",
    "TCPConnection",
    "handle_request",
]
