# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""tRPC-Python transport package: how bytes travel, not what they mean.

The package holds both halves of the wire path.

The **client half** turns an encoded request into a round trip over a pooled
connection: :class:`ClientTransport` picks TCP (single-shot or multiplexed) or
UDP from :class:`RoundTripOptions`, and :class:`ClientConn` keeps the frame
queue for streaming calls.

The **server half** never accepts connections itself. It tells
:class:`~trpc_sdk.server.TrpcServerEngine` which children to fork, and the
asyncio protocols in this package then run *inside* those children:

- dispatcher mode: :class:`DispatcherClientConnection` accepts the client
  TCP, splits frames, and writes them to a worker pipe;
  :class:`WorkerDispatcherConnection` (or the C / trpcio variants) runs
  the handler and writes the response back
- multi-listener mode: :class:`ExecutorUDPConnection` (and
  ``TCPConnection`` from ``trpc_sdk.protocol``) listen on each executor

The package is split by responsibility:

- :mod:`~trpc_sdk.transport._client_transport` — ``ClientTransport``,
  ``ClientTaskStreamTransport`` and the plugin lookups; the TCP / UDP /
  stream round trips live in ``_client_transport_tcp``,
  ``_client_transport_udp`` and ``_client_transport_stream``
- :mod:`~trpc_sdk.transport._options` — ``RoundTripOptions`` the client half
  fills per call
- :mod:`~trpc_sdk.transport._udp_connection` — datagram endpoint helpers used
  by the UDP round trip
- :mod:`~trpc_sdk.transport._transport` — ``TrpcServerTransport``:
  process-model registration on the engine
- :mod:`~trpc_sdk.transport._stream_transport` — stream write-handle map on
  top of the unary listen path
- :mod:`~trpc_sdk.transport._factory` — plugin factory,
  ``DefaultServerTransport*``, :func:`get_server_transport`
- :mod:`~trpc_sdk.transport._dispatcher`, :mod:`~trpc_sdk.transport._worker`,
  :mod:`~trpc_sdk.transport._executor`, :mod:`~trpc_sdk.transport._task` — the
  asyncio protocols each process attaches to its pipes
- :mod:`~trpc_sdk.transport._connection` — shared ``BaseConnection``
- :mod:`~trpc_sdk.transport._socket` — listen-socket constructors
- :mod:`~trpc_sdk.transport._scheduler` — worker-selection hooks
- :mod:`~trpc_sdk.transport._types`, :mod:`~trpc_sdk.transport._constants`,
  :mod:`~trpc_sdk.transport._features` — shared records, defaults (listen
  backlog, IPC frame sizes, ``RequestType``, ``ClientTimeoutReason``),
  accessors
- :mod:`~trpc_sdk.transport._ipc_codec` — pure-Python inner-IPC framing; the
  production twin is the Cython ``_c_ipc_codec`` extension

Symbols renamed or dropped relative to the pre-rewrite package are listed in
``MIGRATION.md``; the frozen original lives in :mod:`trpc_sdk.old_transport`.
"""

from __future__ import annotations

from importlib import import_module

from trpc_sdk.abc import BaseServerTransportOption

from ._c_ipc_codec import encode_worker_msg_req  # pylint: disable=no-name-in-module
from ._client_transport import ClientTaskStreamTransport
from ._client_transport import ClientTransport
from ._client_transport import get_client_transport
from ._client_transport import get_default_client_transport
from ._client_transport_stream import ClientConn
from ._connection import BaseConnection
from ._constants import ClientTimeoutReason
from ._constants import DEFAULT_BACKLOG
from ._constants import FRAME_HEAD_LEN
from ._constants import INNER_MAGIC_VALUE
from ._constants import RequestType
from ._dispatcher import DispatcherClientConnection
from ._dispatcher import DispatcherRpczConnection
from ._dispatcher import DispatcherWorkerConnection
from ._factory import DefaultServerTransport
from ._factory import DefaultServerTransportOption
from ._factory import TrpcServerTransportFactory
from ._factory import get_server_transport
from ._features import is_uvloop_enabled
from ._ipc_codec import InnerCodec
from ._ipc_codec import InnerParser
from ._ipc_codec import inner_decode
from ._ipc_codec import inner_encode
from ._options import RoundTripOptions
from ._scheduler import get_next_worker
from ._scheduler import register_dispatcher_worker_func
from ._socket import create_inet_socket
from ._socket import create_server_socket
from ._socket import create_unix_socket
from ._stream_transport import TaskServerStreamTransport
from ._stream_transport import TrpcServerStreamTransport
from ._transport import TrpcServerTransport
from ._types import ClientConnectionInfo
from ._types import SchedulerParams
from ._udp_connection import PacketConnection
from ._udp_connection import PacketReader
from ._udp_connection import PacketReaderProtocol
from ._udp_connection import PacketWriter
from ._udp_connection import open_datagram_endpoint

__all__ = [
    # Client-side transport
    "ClientTransport",
    "ClientTaskStreamTransport",
    "get_client_transport",
    "get_default_client_transport",
    "ClientConn",
    "RequestType",
    "ClientTimeoutReason",
    "RoundTripOptions",
    # Datagram endpoint helpers
    "open_datagram_endpoint",
    "PacketConnection",
    "PacketReader",
    "PacketReaderProtocol",
    "PacketWriter",
    # Listen / process-model registration
    "TrpcServerTransport",
    "TrpcServerStreamTransport",
    "TaskServerStreamTransport",
    "DefaultServerTransport",
    "DefaultServerTransportOption",
    "BaseServerTransportOption",
    "TrpcServerTransportFactory",
    "get_server_transport",
    # Dispatcher-process protocols
    "DispatcherClientConnection",
    "DispatcherWorkerConnection",
    "DispatcherRpczConnection",
    # Worker-process protocols
    "WorkerDispatcherConnection",
    "WorkerTaskConnection",
    "WorkerCTaskConnection",
    "WorkerMessageConnection",
    # Executor-process protocols
    "ExecutorUDPConnection",
    "ExecutorTaskConnection",
    # Task-process protocols
    "TaskWorkerConnection",
    "TaskStreamConnection",
    "TASK_CONN",
    # Shared types and hooks
    "BaseConnection",
    "ClientConnectionInfo",
    "SchedulerParams",
    "get_next_worker",
    "register_dispatcher_worker_func",
    # Socket helpers
    "create_server_socket",
    "create_inet_socket",
    "create_unix_socket",
    "DEFAULT_BACKLOG",
    # Pure-Python IPC framing
    "InnerParser",
    "InnerCodec",
    "inner_decode",
    "inner_encode",
    "INNER_MAGIC_VALUE",
    "FRAME_HEAD_LEN",
    # Cython inner-IPC framing used across process boundaries
    "encode_worker_msg_req",
    # Feature accessors
    "is_uvloop_enabled",
]


_LAZY_EXPORTS = {
    "TASK_CONN": ("._task", "TASK_CONN"),
    "TaskStreamConnection": ("._task", "TaskStreamConnection"),
    "TaskWorkerConnection": ("._task", "TaskWorkerConnection"),
    "ExecutorTaskConnection": ("._executor", "ExecutorTaskConnection"),
    "ExecutorUDPConnection": ("._executor", "ExecutorUDPConnection"),
    "WorkerCTaskConnection": ("._worker", "WorkerCTaskConnection"),
    "WorkerDispatcherConnection": ("._worker", "WorkerDispatcherConnection"),
    "WorkerMessageConnection": ("._worker", "WorkerMessageConnection"),
    "WorkerTaskConnection": ("._worker", "WorkerTaskConnection"),
}


def __getattr__(name: str):
    """Lazily export the child-process protocols to break an import cycle.

    ``_worker`` / ``_executor`` / ``_task`` import ``trpc_sdk.task``, which
    imports ``trpc_sdk.stream`` while that package is still initializing. They
    are only needed once a process is already serving, so resolving them on
    first access is enough.
    """
    spec = _LAZY_EXPORTS.get(name)
    if spec is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    module_name, attr_name = spec
    value = getattr(import_module(module_name, package=__name__), attr_name)
    globals()[name] = value
    return value
