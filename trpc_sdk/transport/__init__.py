# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""tRPC-Python transport package: how bytes travel, not what they mean.

The package holds both halves of the wire path.

The **client half** turns an encoded request into a round trip over a pooled
connection: :class:`ClientTransport` picks TCP or UDP from
:class:`RoundTripOptions`, and :class:`ClientConn` keeps the frame
queue for streaming calls.

The **server half** never accepts connections itself. It tells
:class:`~trpc_sdk.server.TrpcServerAdapter` which children to fork, and the
asyncio protocols in this package then run *inside* those children:

- separate mode: :class:`SeparateClientConnection` on the I/O process
  accepts the client TCP, splits frames, and writes them to an rpc task
  pipe; :class:`RpcTaskConnection` on the rpc task runs the handler and
  writes the response back
- merge mode: :class:`DefaultDatagramConnection` and
  :class:`TCPConnection` listen on the merge worker. Both live in
  :mod:`trpc_sdk.transport._server`, so a merge worker imports them from
  here.

The package is split by responsibility:

- :mod:`~trpc_sdk.transport._client` — the client half: ``ClientTransport``
  and the plugin lookups, the TCP / UDP / stream round trips, the datagram
  endpoint helpers the UDP round trip uses, and the ``RoundTripOptions``
  the client fills per call
- :mod:`~trpc_sdk.transport._pool` — client connection pools the TCP round
  trip dials through; a tRPC-Python extension with no cross-language
  counterpart, so it is scoped to the client transport half
- :mod:`~trpc_sdk.transport._server` — the server half's listen transports:
  ``TrpcServerTransport`` (bind before fork, listen after it; its private
  acceptor picks ``create_server`` or a main-thread accept loop from the
  :class:`~trpc_sdk.runtime.ThreadModel` the runtime installs),
  ``TrpcServerStreamTransport`` with its stream write handles,
  ``UserProtocolTransport`` with the ``register_protocol_handle`` registry,
  the plugin factory (``DefaultServerTransport*``,
  :func:`get_server_transport`), and the accepted connections:
  ``TcpConnectionOption`` and the Cython ``TCPConnection``
  (:mod:`~trpc_sdk.transport._server._cython`), ``ConnWriter`` for
  cross-thread writes, ``DefaultIoConnection`` with its ``ConnectionOption``
  for subclasses calling ``set_io_connection``,
  ``DefaultDatagramConnection`` for UDP listen
  sockets (built from the same ``ConnectionOption``), and
  ``ServerConnection``, the peer view both hand to ``_msg_handler``
- :mod:`~trpc_sdk.transport._separate` — the asyncio protocols on both ends
  of the separate-mode I/O process <-> rpc task pipes. The worker <-> task
  pipe protocols live in :mod:`trpc_sdk.task`
- :mod:`~trpc_sdk.transport._scheduler` — worker-selection hooks
- :mod:`~trpc_sdk.transport._types`, :mod:`~trpc_sdk.transport._constants` —
  shared records and defaults (IPC frame sizes, ``RequestType``,
  ``ClientTimeoutReason``)

Listen-socket constructors live in :mod:`trpc_sdk.utils`
(:func:`~trpc_sdk.utils.create_new_socket`).
- :mod:`~trpc_sdk.transport._cython` — Cython inner-IPC framing used
  across I/O process / rpc task / worker / task pipes

Symbols renamed or dropped relative to the pre-rewrite package are listed in
``MIGRATION.md``; the frozen original lives in :mod:`trpc_sdk.old_transport`.
"""

from __future__ import annotations

from trpc_sdk.abc import BaseServerTransportOption

from ._cython import InnerStreamParser  # pylint: disable=no-name-in-module
from ._cython import encode_worker_msg_req  # pylint: disable=no-name-in-module
from ._constants import ClientTimeoutReason
from ._constants import FRAME_HEAD_LEN
from ._constants import INNER_MAGIC_VALUE
from ._constants import RequestType
from ._pool import ConnAddress
from ._pool import ConnPoolOptions
from ._pool import ConnectionPoolManager
from ._pool import PooledConnection
from ._pool import get_pool_connection
from ._scheduler import get_next_worker
from ._scheduler import has_custom_scheduler
from ._scheduler import register_dispatcher_worker_func
from ._scheduler import select_rpc_task
from ._separate import RpcTaskConnection
from ._separate import SeparateClientConnection
from ._separate import SeparateRpcTaskConnection
from ._separate import SeparateRpczConnection
from ._types import ClientConnectionInfo
from ._types import SchedulerParams
from ._client import ClientConn
from ._client import ClientTransport
from ._client import PacketConnection
from ._client import PacketReader
from ._client import PacketReaderProtocol
from ._client import PacketWriter
from ._client import RoundTripOptions
from ._client import get_client_transport
from ._client import get_default_client_transport
from ._client import open_datagram_endpoint
from ._server import ConnWriter
from ._server import ConnectionCloseHandler
from ._server import ConnectionIdleCleanHandler
from ._server import ConnectionInitHandler
from ._server import ConnectionOption
from ._server import DefaultDatagramConnection
from ._server import DefaultIoConnection
from ._server import DefaultServerTransport
from ._server import DefaultServerTransportOption
from ._server import IpcStreamWriter
from ._server import MsgHandler
from ._server import ProtocolStarter
from ._server import ServerConnection
from ._server import TCPConnection
from ._server import TcpConnectionOption
from ._server import TrpcServerStreamTransport
from ._server import TrpcServerTransport
from ._server import TrpcServerTransportFactory
from ._server import UserProtocolTransport
from ._server import get_protocol_handle
from ._server import get_server_transport
from ._server import register_protocol_handle
from ._server import resolve_server_transport
from ._server import ssl_context_from_opts

__all__ = [
    # Client-side transport
    "ClientTransport",
    "get_client_transport",
    "get_default_client_transport",
    "ClientConn",
    "RequestType",
    "ClientTimeoutReason",
    "RoundTripOptions",
    # Client connection pool
    "ConnAddress",
    "ConnPoolOptions",
    "ConnectionPoolManager",
    "PooledConnection",
    "get_pool_connection",
    # Datagram endpoint helpers
    "open_datagram_endpoint",
    "PacketConnection",
    "PacketReader",
    "PacketReaderProtocol",
    "PacketWriter",
    # Listen / process-model registration
    "TrpcServerTransport",
    "TrpcServerStreamTransport",
    "UserProtocolTransport",
    "IpcStreamWriter",
    "ConnWriter",
    "TCPConnection",
    "TcpConnectionOption",
    "DefaultIoConnection",
    "ConnectionOption",
    "ServerConnection",
    "DefaultDatagramConnection",
    "MsgHandler",
    "ConnectionIdleCleanHandler",
    "ConnectionInitHandler",
    "ConnectionCloseHandler",
    "ProtocolStarter",
    "get_protocol_handle",
    "register_protocol_handle",
    "resolve_server_transport",
    "ssl_context_from_opts",
    "DefaultServerTransport",
    "DefaultServerTransportOption",
    "BaseServerTransportOption",
    "TrpcServerTransportFactory",
    "get_server_transport",
    # Separate-mode I/O process protocols
    "SeparateClientConnection",
    "SeparateRpcTaskConnection",
    "SeparateRpczConnection",
    # Separate-mode rpc task protocol
    "RpcTaskConnection",
    # Shared types and hooks
    "ClientConnectionInfo",
    "SchedulerParams",
    "get_next_worker",
    "has_custom_scheduler",
    "register_dispatcher_worker_func",
    "select_rpc_task",
    # Inner-IPC frame constants and Cython framing used across processes
    "INNER_MAGIC_VALUE",
    "FRAME_HEAD_LEN",
    "InnerStreamParser",
    "encode_worker_msg_req",
]
