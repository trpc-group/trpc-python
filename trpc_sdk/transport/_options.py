# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Per-call options the client transport reads on a round trip.

:class:`RoundTripOptions` is the bag of knobs :class:`ClientTransport`
passes into the TCP / UDP / stream helpers. Request enums
(:class:`RequestType`, :class:`ClientTimeoutReason`) live in
:mod:`trpc_sdk.transport._constants` and are re-exported at the package
level; this module only holds the options object itself.
"""

from __future__ import annotations

from socket import socket
from typing import Optional

from trpc_sdk.abc import AddrType
from trpc_sdk.abc import BaseClientProtocolChecker
from trpc_sdk.abc import BaseClientTransportOption
from trpc_sdk.abc import BaseCodec
from trpc_sdk.codec import TrpcClientParser
from ._constants import RequestType
from ._pool import ConnPoolOptions
from ._pool import ConnectionPoolManager


class RoundTripOptions(BaseClientTransportOption):
    """Options for one client round trip.

    Constructed empty and filled by setters (or by assigning fields
    directly). The TCP / UDP helpers then read ``network``, ``address``,
    ``req_type``, and the pool fields to pick a path.

    Attributes:
        checker: Frame parser used to split the response stream.
        password: Optional password for the callee.
        pool: Caller-supplied connection pool. When set, the TCP helper
            uses this instead of the framework default.
        pool_opts: Options for the framework-built pool. Ignored when
            ``pool`` is already set.
        req_type: :attr:`RequestType.SendAndRecv` waits for a response;
            :attr:`RequestType.SendOnly` writes and returns.
        ssl_context: TLS context for HTTPS.
        is_task_stream: When True, :class:`ClientConn` must not close the
            pooled connection after the stream ends; the task process
            still owns it.
        disable_reuse_http_session: When True, do not reuse the HTTP
            session across calls. Default is False (reuse is allowed).
    """

    def __init__(self):
        super().__init__()
        self.checker = TrpcClientParser()
        self.password = None
        self.pool = None
        self.pool_opts = None

        self.req_type = RequestType.SendAndRecv

        self.ssl_context = None
        self.is_task_stream = False
        self.disable_reuse_http_session = False

    def set_req_type(self, req_type: int):
        """Set whether this call waits for a response.

        Args:
            req_type: :attr:`RequestType.SendAndRecv` or
                :attr:`RequestType.SendOnly`.
        """
        self.req_type = req_type

    def set_disable_reuse_http_session(self, disable_reuse_http_session: bool):
        """Disable (or re-enable) HTTP session reuse for this call.

        Args:
            disable_reuse_http_session: True to force a fresh session.
        """
        self.disable_reuse_http_session = disable_reuse_http_session

    def set_password(self, password: str):
        """Set the request password.

        Args:
            password: Password sent with the call.
        """
        self.password = password

    def set_pool(self, pool: ConnectionPoolManager):
        """Use a caller-supplied connection pool instead of the default.

        Args:
            pool: Pool manager the TCP helper will ``get`` from.
        """
        self.pool = pool

    def set_pool_options(self, pool_opts: ConnPoolOptions):
        """Set options for the framework-built connection pool.

        Ignored when :meth:`set_pool` has already supplied a pool.

        Args:
            pool_opts: Dial / size knobs for the built-in pool.
        """
        self.pool_opts = pool_opts

    def set_checker(self, checker: Optional[BaseClientProtocolChecker]) -> None:
        """Install the protocol frame checker used to split the response.

        Args:
            checker: Client checker, typically from ``get_client_checker``.
        """
        self.checker = checker

    def set_codec(self, codec: Optional[BaseCodec]) -> None:
        """Install the codec used to encode and decode payloads.

        Args:
            codec: Client codec for this round trip.
        """
        self.codec = codec

    def set_network(self, network: str) -> None:
        """Set the dial network, e.g. ``tcp`` or ``udp``.

        Args:
            network: Network type copied from client options or the node.
        """
        self.network = network

    def set_short_conn(self, short_conn: bool) -> None:
        """Use a short-lived connection for this call when True.

        Args:
            short_conn: True to skip pooling.
        """
        self.short_conn = short_conn

    def set_endpoint(self, endpoint: str) -> None:
        """Set the logical dial target string.

        Args:
            endpoint: Selector result such as ``127.0.0.1:8000``.
        """
        self.endpoint = endpoint

    def set_address(self, address: AddrType) -> None:
        """Set the remote ``(host, port)`` for this round trip.

        Args:
            address: Dial endpoint selected by the naming layer.
        """
        self.address = address

    def set_local_address(self, addr: Optional[AddrType]) -> None:
        """Bind the local end of the client socket.

        Args:
            addr: Local ``(host, port)``, or ``None`` for the default.
        """
        self.local_addr = addr

    def set_socket_type(self, socket_type: str) -> None:
        """Set the socket family label, e.g. ``net`` or ``unix``.

        Args:
            socket_type: Socket family copied from client options.
        """
        self.socket_type = socket_type

    def set_sock(self, sock: Optional[socket]) -> None:
        """Reuse a pre-created socket instead of dialing.

        Args:
            sock: Connected socket, or ``None`` to dial as usual.
        """
        self.sock = sock
