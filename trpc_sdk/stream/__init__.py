# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""tRPC-Python stream package: public streaming RPC plus inner task IPC.

Public symbols are re-exported here. Callers should ``from trpc_sdk.stream
import ...``. Task-stream handler and method ABCs live in
:mod:`trpc_sdk.abc`.

The package is split by responsibility:

- :mod:`~trpc_sdk.stream._client` — :class:`ClientStream` and the client factory
- :mod:`~trpc_sdk.stream._server` — :class:`ServerStream` and :class:`StreamDispatcher`
- :mod:`~trpc_sdk.stream._flow_control` — send / receive window
- :mod:`~trpc_sdk.stream._task_client` / :mod:`~trpc_sdk.stream._task_service` —
  default method wrappers and :class:`TaskStreamServiceAdapter`
- :mod:`~trpc_sdk.stream._constants`, :mod:`~trpc_sdk.stream._options`,
  :mod:`~trpc_sdk.stream._types` — window bounds, stream ids, clamp helper,
  and method descriptors
"""

from __future__ import annotations

from ._client import Client
from ._client import ClientStream
from ._client import create_stream_client
from ._client import create_task_stream_client
from ._client import default_stream_client
from ._client import default_task_stream_client
from ._constants import DEFAULT_INIT_WINDOW_SIZE
from ._constants import MAX_INIT_WINDOW_SIZE
from ._constants import MIN_INIT_WINDOW_SIZE
from ._flow_control import ReceiveControl
from ._flow_control import SendControl
from ._options import get_window_size
from ._server import ServerStream
from ._server import ServerStreamHandle
from ._server import StreamDispatcher
from ._server import create_server_stream
from ._server import create_stream_dispatcher
from ._task_service import TaskStreamServiceAdapter
from ._types import ClientStreamDesc

__all__ = [
    # Constants
    "MAX_INIT_WINDOW_SIZE",
    "MIN_INIT_WINDOW_SIZE",
    "DEFAULT_INIT_WINDOW_SIZE",
    # Options
    "get_window_size",
    # Flow control
    "SendControl",
    "ReceiveControl",
    # Client
    "ClientStream",
    "ClientStreamDesc",
    "Client",
    "create_stream_client",
    "create_task_stream_client",
    "default_stream_client",
    "default_task_stream_client",
    # Server
    "ServerStream",
    "ServerStreamHandle",
    "StreamDispatcher",
    "create_stream_dispatcher",
    "create_server_stream",
    # Task stream
    "TaskStreamServiceAdapter",
]
