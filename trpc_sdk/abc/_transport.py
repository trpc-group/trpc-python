# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Transport extension interfaces and shared option models.

This module defines the abstract contracts for server and client transports,
including unary round-trips and client-side streaming transport operations.
"""

from __future__ import annotations

from abc import abstractmethod
from dataclasses import dataclass
from socket import socket
from typing import Callable
from typing import Optional
from typing import Tuple

from ._cython import BaseClientProtocolParser
from ._cython import BaseCodec
from ._cython import BaseServerProtocolParser
from ._cython import BaseService
from ._cython import TrpcContext
from ._plugin import BasePlugin

AddrType = Tuple[str, int]
"""Network endpoint as ``(host, port)``."""

ServiceHandler = Callable[[TrpcContext, bytes], bytes]
"""Unary service handler that accepts a context and request bytes."""

STREAM_WRITER_CTX_KEY = "stream_writer"
"""Context meta key holding the write handle of a stream frame's connection.

Set by whoever receives the frame: the TCP protocol puts the connection's
``ConnWriter`` here, and under ``server.mode: separate`` the rpc task puts an
``IpcStreamWriter``. On INIT the stream transport saves the handle for that
stream and clears the key; other frames clear it as well, so cloned contexts
never carry a stale handle. Every outgoing frame of the stream is written
through the saved handle, which has ``write(data)`` and a ``closed`` flag.
"""


@dataclass
class BaseServerTransportOption:
    """Describe options used to start a server transport.

    Attributes:
        service: Optional service implementation bound to the transport.
        address: Bind endpoint as ``(host, port)``; for a UNIX socket it is
            ``(unix_path, 0)`` and only ``address[0]`` is used.
        port: Bind port number, kept in sync with ``address[1]`` by
            :meth:`set_address`.
        parser: Server protocol parser that splits inbound frames.
        rpc_handler: Optional unary service handler.
        network: Network type such as ``tcp`` or ``udp``.
        service_name: Logical service name exposed by this listener.
        socket_type: Socket family label such as ``net`` or ``unix``.
        worker_num: Number of worker processes or threads.
        mode: Transport runtime mode identifier.
        idle_timeout: Idle connection timeout in seconds.
        tls_key: Path to the TLS private key, if any.
        tls_cert: Path to the TLS certificate, if any.
        target: Optional target name used by the transport backend.
        backlog: ``listen()`` backlog for the listen socket built by
            :meth:`BaseServerTransport.listen`.
        reuse_port: Not read by the built-in transports: whether a socket
            gets ``SO_REUSEPORT`` depends on the process model, so the
            runtime decides it after fork.
        is_stream: True when the listener carries tRPC streaming RPCs.
    """

    service: Optional[BaseService] = None
    address: AddrType = ("", 0)
    port: int = 8000
    parser: Optional[BaseServerProtocolParser] = None
    rpc_handler: Optional[ServiceHandler] = None
    network: str = "tcp"
    service_name: str = ""
    socket_type: str = ""
    worker_num: int = 2
    mode: str = ""
    idle_timeout: float = 0
    tls_key: str = ""
    tls_cert: str = ""
    target: str = ""
    backlog: int = 1024
    reuse_port: bool = False
    is_stream: bool = False

    def set_address(self, address_info: AddrType):
        """Set the bind endpoint and keep ``port`` in sync with it.

        Args:
            address_info: Bind endpoint as ``(host, port)``.
        """
        self.address = (address_info[0], int(address_info[1]))
        self.port = self.address[1]

    def set_service(self, service: Optional[BaseService]) -> None:
        """Bind the service instance this listener will dispatch into."""
        self.service = service

    def set_service_name(self, service_name: str) -> None:
        """Set the logical service name exposed by this listener."""
        self.service_name = service_name

    def set_socket_type(self, socket_type: str) -> None:
        """Set the socket family label, e.g. ``net`` or ``unix``."""
        self.socket_type = socket_type

    def set_network(self, network: str) -> None:
        """Set the listen network, e.g. ``tcp`` or ``udp``."""
        self.network = network

    def set_idle_timeout(self, timeout: float) -> None:
        """Set the idle-connection timeout copied from ``service.timeout``."""
        self.idle_timeout = timeout

    def set_parser(self, parser: Optional[BaseServerProtocolParser]) -> None:
        """Install the protocol frame parser for this listener."""
        self.parser = parser

    def set_worker_num(self, worker_num: int) -> None:
        """Copy process-model worker count onto this listener."""
        self.worker_num = worker_num

    def set_mode(self, mode: str) -> None:
        """Copy ``server.mode`` onto this listener."""
        self.mode = mode

    def set_tls_key(self, tls_key: str) -> None:
        """Set the TLS private key path; empty disables TLS."""
        self.tls_key = tls_key

    def set_tls_cert(self, tls_cert: str) -> None:
        """Set the TLS certificate path; empty disables TLS."""
        self.tls_cert = tls_cert

    def set_target(self, target: str) -> None:
        """Set an optional backend target name for this listener."""
        self.target = target


class BaseServerTransport(BasePlugin):
    """Define server transport lifecycle operations.

    The lifecycle is split around the fork. :meth:`bind` runs once in the
    parent, from ``ServiceAdapter.run``, and only records the options; the engine
    then checks the endpoint with :meth:`check_endpoint` and registers the
    transport so every forked worker inherits it. :meth:`listen` runs after
    fork, in each process that serves the endpoint. :meth:`stop` and
    :meth:`destroy` undo :meth:`listen` in two steps so a caller can stop
    accepting first and wait for the close afterwards.
    """

    @abstractmethod
    def bind(self, opts: BaseServerTransportOption) -> None:
        """Record the listener options. Called before fork.

        Must not create sockets or touch an event loop: the transport object
        is inherited by every forked worker.

        Args:
            opts: Server transport options for this listener.
        """
        ...

    def check_endpoint(self) -> bool:
        """Check, before fork, that the endpoint can be served. Default: True.

        Called by the engine right after :meth:`bind`. Logs the reason
        itself; the server exits with status 1 on False.

        Returns:
            Whether startup may continue.
        """
        return True

    @abstractmethod
    def listen(self) -> None:
        """Start serving the endpoint on the calling thread's event loop.

        Called after fork, in each process that serves the endpoint, before
        that loop runs. Everything that must be closed to stop accepting is
        kept on the transport for :meth:`stop`.

        A subclass may add parameters with defaults, such as the built-in
        transports' ``opt`` that tells them which loops to serve on.
        """
        ...

    def stop(self) -> None:
        """Stop accepting new requests; requests already taken in keep running.

        Synchronous because the runtime calls it from its stop-signal path,
        on the thread :meth:`listen` ran on, before any serving loop stops.
        Must be idempotent. Default: nothing to stop.
        """
        return None

    async def destroy(self) -> None:
        """Wait for the close to finish and release resources. Default: no-op."""
        return None


@dataclass
class BaseClientTransportOption:
    """Describe options used by a client transport.

    Attributes:
        address: Remote endpoint as ``(host, port)``. Defaults to
            ``("", 0)`` because subclasses such as
            ``trpc_sdk.transport.RoundTripOptions`` are constructed empty
            and filled by setters before the call.
        endpoint: Logical endpoint or dial target string.
        socket_type: Socket backend type such as ``net``.
        network: Network type such as ``tcp`` or ``udp``.
        short_conn: Whether each request should use a short-lived connection.
        parser: Client protocol parser used to split response frames.
        codec: Optional codec used to encode and decode payloads.
        local_addr: Optional local bind endpoint.
        sock: Optional pre-created socket reused by the transport.
    """

    address: AddrType = ("", 0)
    endpoint: str = ""
    socket_type: str = "net"
    network: str = "tcp"
    short_conn: bool = False
    parser: Optional[BaseClientProtocolParser] = None
    codec: Optional[BaseCodec] = None
    local_addr: Optional[AddrType] = None
    sock: Optional[socket] = None


class BaseClientTransport(BasePlugin):
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


class BaseClientStreamTransport(BasePlugin):
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
    async def start_stream(self, ctx: TrpcContext, options: BaseClientTransportOption):
        """Initialize a new client stream session.

        Named apart from :meth:`~trpc_sdk.abc.BasePlugin.start`, which the
        plugin manager calls with no arguments once per process.

        Args:
            ctx: Stream-scoped RPC context.
            options: Client transport options used to start the stream.
        """
        ...
