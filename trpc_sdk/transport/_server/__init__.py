# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Server half of the transport package: the per-service listen transports.

A server transport is constructed and bound in the parent (``bind`` only
records options) and listens in the forked child (``listen`` creates the
socket on the child's event loop). The runtime decides which processes and
loops call ``listen`` and describes them in a
:class:`~trpc_sdk.runtime.ThreadModel`, which the transport reads from
:func:`~trpc_sdk.runtime.current_thread_model`; the transport alone
decides how the endpoint is served and closed. The dispatcher
/ worker / executor / task pipe protocols stay in the parent package; this
package only holds the objects a service registers on the server adapter.

The package is split by responsibility:

- :mod:`~trpc_sdk.transport._server._acceptor` — :class:`Acceptor`, which
  accepts on one loop and hands connections to serving loops, and
  :func:`serve_stream`, which picks it or ``create_server``
- :mod:`~trpc_sdk.transport._server._trpc_transport` — :class:`TrpcServerTransport`,
  the tRPC-over-TCP / UDP listener, whose socket, ``create_server`` /
  acceptor serving and closing :class:`~trpc_sdk.http.HttpServerTransport`
  reuses
- :mod:`~trpc_sdk.transport._server._custom_transport` — :class:`UserProtocolTransport` and
  the :func:`register_protocol_handle` registry
- :mod:`~trpc_sdk.transport._server._stream_connection` — :class:`TcpConnectionOption`
  for the Cython ``TCPConnection``; :class:`BaseConnection` /
  :class:`DefaultIoConnection`, the Python connection used after
  ``set_io_connection``, with its :class:`ConnectionOption` and the
  ``MsgHandler`` / ``Connection*Handler`` callback types it holds; and
  :class:`ServerConnection`, the peer view ``_msg_handler`` reads
- :mod:`~trpc_sdk.transport._server._cython` — :class:`TCPConnection`, the
  Cython asyncio Protocol for one accepted client TCP socket, and
  :func:`handle_request`
- :mod:`~trpc_sdk.transport._server._conn_writer` — :class:`ConnWriter`, the
  handle that writes on a connection's transport from any thread
- :mod:`~trpc_sdk.transport._server._datagram_connection` —
  :class:`DefaultDatagramConnection`, the datagram protocol of a
  tRPC-over-UDP listen socket, which builds one peer view per datagram
- :mod:`~trpc_sdk.transport._server._stream_transport` —
  :class:`TrpcServerStreamTransport`, the ``(session, stream)`` write-handle
  map on top of the unary listen path, and :class:`IpcStreamWriter`, the
  write handle of ``server.mode: separate``
- :mod:`~trpc_sdk.transport._server._stream_factory` — plugin factory,
  ``DefaultServerTransport*``, :func:`get_server_transport`. Importing this
  package registers the ``"trpc"`` ``SERVER_TRANSPORT`` plugin

Callers should import these symbols from :mod:`trpc_sdk.transport`.
"""

from __future__ import annotations

from ._acceptor import AcceptTarget
from ._acceptor import Acceptor
from ._acceptor import StreamListener
from ._acceptor import serve_stream
from ._conn_writer import ConnWriter
from ._cython import TCPConnection  # pylint: disable=no-name-in-module
from ._datagram_connection import DefaultDatagramConnection
from ._stream_connection import BaseConnection
from ._stream_connection import ConnectionCloseHandler
from ._stream_connection import ConnectionIdleCleanHandler
from ._stream_connection import ConnectionInitHandler
from ._stream_connection import ConnectionOption
from ._stream_connection import DefaultIoConnection
from ._stream_connection import MsgHandler
from ._stream_connection import ServerConnection
from ._stream_connection import TcpConnectionOption
from ._stream_factory import DefaultServerTransport
from ._stream_factory import DefaultServerTransportOption
from ._stream_factory import TrpcServerTransportFactory
from ._stream_factory import get_server_transport
from ._stream_transport import IpcStreamWriter
from ._stream_transport import TrpcServerStreamTransport
from ._trpc_transport import TrpcServerTransport
from ._trpc_transport import ssl_context_from_opts
from ._custom_transport import ProtocolStarter
from ._custom_transport import UserProtocolTransport
from ._custom_transport import get_protocol_handle
from ._custom_transport import register_protocol_handle
from ._custom_transport import resolve_server_transport

__all__ = [
    "AcceptTarget",
    "Acceptor",
    "BaseConnection",
    "ConnWriter",
    "ConnectionCloseHandler",
    "ConnectionIdleCleanHandler",
    "ConnectionInitHandler",
    "ConnectionOption",
    "DefaultDatagramConnection",
    "DefaultIoConnection",
    "DefaultServerTransport",
    "DefaultServerTransportOption",
    "MsgHandler",
    "ServerConnection",
    "StreamListener",
    "TCPConnection",
    "TcpConnectionOption",
    "TrpcServerTransport",
    "TrpcServerStreamTransport",
    "TrpcServerTransportFactory",
    "IpcStreamWriter",
    "ProtocolStarter",
    "UserProtocolTransport",
    "get_protocol_handle",
    "get_server_transport",
    "register_protocol_handle",
    "resolve_server_transport",
    "serve_stream",
    "ssl_context_from_opts",
]
