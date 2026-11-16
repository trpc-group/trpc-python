# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.
"""Client half of the transport package.

Turns an encoded request into a round trip over a pooled connection.
Importing this package registers :class:`ClientTransport` as the default
``PluginType.CLIENT_TRANSPORT`` plugin; :mod:`trpc_sdk.transport` imports it
eagerly so the slot is filled as soon as the transport package loads.

The package is split by responsibility:

- :mod:`~trpc_sdk.transport._client._client_transport` — ``ClientTransport``
  and the plugin lookups
- :mod:`~trpc_sdk.transport._client._tcp_round_trip`,
  :mod:`~trpc_sdk.transport._client._udp_round_trip` — unary TCP / UDP
  round trips
- :mod:`~trpc_sdk.transport._client._client_stream_conn` —
  :class:`ClientStreamConn`, the per-stream frame queue
- :mod:`~trpc_sdk.transport._client._options` — :class:`RoundTripOptions`
  the client fills per call
- :mod:`~trpc_sdk.transport._client._udp_connection` — datagram endpoint
  helpers used by the UDP round trip
- :mod:`~trpc_sdk.transport._client._pool` — the client connection pools
  the TCP round trip dials through

Callers should import these symbols from :mod:`trpc_sdk.transport`.
"""

from __future__ import annotations

from ._client_transport import ClientTransport
from ._client_transport import get_client_transport
from ._client_transport import get_default_client_transport
from ._client_stream_conn import ClientStreamConn
from ._options import RoundTripOptions
from ._udp_connection import PacketConnection
from ._udp_connection import PacketReader
from ._udp_connection import PacketReaderProtocol
from ._udp_connection import PacketWriter
from ._udp_connection import open_datagram_endpoint

__all__ = [
    "ClientStreamConn",
    "ClientTransport",
    "RoundTripOptions",
    "get_client_transport",
    "get_default_client_transport",
    "open_datagram_endpoint",
    "PacketConnection",
    "PacketReader",
    "PacketReaderProtocol",
    "PacketWriter",
]
