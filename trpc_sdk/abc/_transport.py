# Tencent is pleased to support the open source community by making tRPC-Agent-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Agent-Python is licensed under Apache-2.0.

"""Transport extension interfaces and shared option models.

This module defines the abstract contracts for server and client transports,
including unary round-trips and client-side streaming transport operations.
"""

from __future__ import annotations

from abc import ABC
from abc import abstractmethod
from dataclasses import dataclass
from socket import socket
from typing import Callable
from typing import Optional
from typing import Tuple

from ._cython import BaseClientProtocolChecker  # pyright: ignore[reportMissingModuleSource]
from ._cython import BaseCodec  # pyright: ignore[reportMissingModuleSource]
from ._cython import BaseServerProtocolChecker  # pyright: ignore[reportMissingModuleSource]
from ._cython import TrpcContext  # pyright: ignore[reportMissingModuleSource]
from ._server import BaseService
from ._server import BaseTrpcServerEngine

AddrType = Tuple[str, int]
"""Network endpoint as ``(host, port)``."""

ServiceHandler = Callable[[TrpcContext, bytes], bytes]
"""Unary service handler that accepts a context and request bytes."""


@dataclass
class BaseServerTransportOption:
    """Describe options used to start a server transport.

    Attributes:
        service: Optional service implementation bound to the transport.
        address: Bind host or IP address.
        port: Bind port number.
        checker: Server protocol checker used to validate inbound frames.
        handle: Optional unary service handler.
        network: Network type such as ``tcp`` or ``udp``.
        service_name: Logical service name exposed by this listener.
        worker_num: Number of worker processes or threads.
        mode: Transport runtime mode identifier.
        idle_timeout: Idle connection timeout in seconds.
        tls_key: Path to the TLS private key, if any.
        tls_cert: Path to the TLS certificate, if any.
        target: Optional target name used by the transport backend.
    """

    service: Optional[BaseService] = None
    address: str = ""
    port: int = 8000
    checker: Optional[BaseServerProtocolChecker] = None
    handle: Optional[ServiceHandler] = None
    network: str = "tcp"
    service_name: str = ""
    worker_num: int = 2
    mode: str = ""
    idle_timeout: float = 0
    tls_key: str = ""
    tls_cert: str = ""
    target: str = ""

    def set_address(self, address_info: AddrType):
        """Set the bind host and port from an address tuple.

        Args:
            address_info: Bind endpoint as ``(host, port)``.
        """
        self.address = address_info[0]
        self.port = int(address_info[1])


class BaseServerTransport(ABC):
    """Define server transport lifecycle operations."""

    @abstractmethod
    def listen_and_serve(
        self,
        opts: BaseServerTransportOption,
        server_engine: BaseTrpcServerEngine,
    ):
        """Bind the configured endpoint and start serving traffic.

        Args:
            opts: Server transport options.
            server_engine: Server engine that owns process lifecycle hooks.
        """
        ...

    @abstractmethod
    def close(self):
        """Stop the transport and release owned resources."""
        ...


@dataclass
class BaseClientTransportOption:
    """Describe options used by a client transport.

    Attributes:
        address: Remote endpoint as ``(host, port)``.
        endpoint: Logical endpoint or dial target string.
        socket_type: Socket backend type such as ``net``.
        network: Network type such as ``tcp`` or ``udp``.
        short_conn: Whether each request should use a short-lived connection.
        checker: Client protocol checker used to validate frames.
        codec: Optional codec used to encode and decode payloads.
        local_addr: Optional local bind endpoint.
        sock: Optional pre-created socket reused by the transport.
    """

    address: AddrType
    endpoint: str
    socket_type: str = "net"
    network: str = "tcp"
    short_conn: bool = False
    checker: Optional[BaseClientProtocolChecker] = None
    codec: Optional[BaseCodec] = None
    local_addr: Optional[AddrType] = None
    sock: Optional[socket] = None


class BaseClientTransport(ABC):
    """Define unary client transport round-trips."""

    @abstractmethod
    async def round_trip(
        self,
        ctx: TrpcContext,
        req_data: bytes,
        options: BaseClientTransportOption,
    ):
        """Send one request and wait for the corresponding response.

        Args:
            ctx: Request-scoped RPC context.
            req_data: Encoded request bytes.
            options: Client transport options for this call.

        Returns:
            Encoded response bytes.
        """
        ...


class BaseClientStreamTransport(ABC):
    """Define client-side streaming transport operations."""

    @abstractmethod
    async def round_trip(
        self,
        ctx: TrpcContext,
        req_data: bytes,
        options: BaseClientTransportOption,
    ):
        """Perform one request and response exchange over a stream.

        Args:
            ctx: Stream-scoped RPC context.
            req_data: Encoded request bytes.
            options: Client transport options for this call.

        Returns:
            Encoded response bytes.
        """
        ...

    @abstractmethod
    def send(
        self,
        ctx: TrpcContext,
        req_data: bytes,
        options: BaseClientTransportOption,
    ):
        """Send one streaming frame to the peer.

        Args:
            ctx: Stream-scoped RPC context.
            req_data: Encoded frame bytes.
            options: Client transport options for this send.
        """
        ...

    @abstractmethod
    async def recv(
        self,
        ctx: TrpcContext,
        options: BaseClientTransportOption,
    ) -> bytes:
        """Receive one streaming frame from the peer.

        Args:
            ctx: Stream-scoped RPC context.
            options: Client transport options for this receive.

        Returns:
            Encoded frame bytes.
        """
        ...

    @abstractmethod
    def close(self, ctx: TrpcContext):
        """Close the stream associated with the given context.

        Args:
            ctx: Stream-scoped RPC context.
        """
        ...

    @abstractmethod
    async def start(self, ctx: TrpcContext, options: BaseClientTransportOption):
        """Initialize a new client stream session.

        Args:
            ctx: Stream-scoped RPC context.
            options: Client transport options used to start the stream.
        """
        ...
