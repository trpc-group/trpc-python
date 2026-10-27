# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""tRPC server transport: record options before fork, listen after it.

:meth:`TrpcServerTransport.bind` runs in ``ServiceAdapter.init_transport`` and only keeps the
options; ``ServiceAdapter.init_transport`` then registers the transport on the engine, and every
forked worker inherits it through ``process_info.transports``.

:meth:`TrpcServerTransport.listen` runs in each serving process after fork.
It reads the loops to serve on from
:func:`~trpc_sdk.runtime.current_thread_model`, and ``opts.network``
decides how it serves:

* ``tcp``: one listen socket, served by
  :func:`~trpc_sdk.transport._server._acceptor.serve_stream` on the model's
  ``io`` group.
* ``udp``: nothing to accept, so every ``io`` reactor binds its own socket on
  the address and the kernel spreads the datagrams.

The listen sockets are recorded on the transport, which is safe because the
transport object is only *created* before fork: the list is empty there and
every forked process fills its own copy. UDP endpoints are kept on the thread
model instead, and the runtime releases them before their loop stops.
:class:`~trpc_sdk.http.HttpServerTransport` reuses all of it for aiohttp.
"""

from __future__ import annotations

import asyncio
import os
import socket
import ssl
from functools import partial
from typing import TYPE_CHECKING
from typing import Any
from typing import Callable
from typing import List
from typing import Optional
from typing import Type

from trpc_sdk.abc import BaseServerTransport
from trpc_sdk.abc import BaseServerTransportOption
from trpc_sdk.abc import TrpcContext
from trpc_sdk.abc import get_message
from trpc_sdk.log import logger
from trpc_sdk.protocol import TCPConnection
from trpc_sdk.rpcz import RPCZ_SERVER_REQ_SIZE_CTX_KEY
from trpc_sdk.rpcz import RPCZ_SERVER_RSP_SIZE_CTX_KEY
from trpc_sdk.rpcz import submit_server_span_on_executor_post_io
from trpc_sdk.telemetry import report_attr
from trpc_sdk.utils import create_new_socket
from trpc_sdk.utils import is_tcp_port_available
from trpc_sdk.utils import is_udp_port_available
from trpc_sdk.utils import is_unix_socket
from trpc_sdk.utils import now_ms
from trpc_sdk.utils import now_us
from trpc_sdk.utils import set_addr_to_ctx
from trpc_sdk.utils import unix_path_valid
from ._acceptor import StreamListener
from ._acceptor import serve_stream
from ._constants import SERVER_CONN_KEY
from ._datagram_connection import DefaultDatagramConnection
from ._stream_connection import ConnectionOption
from ._stream_connection import DefaultIoConnection
from ._stream_connection import ServerConnection
from ._stream_connection import TcpConnectionOption

if TYPE_CHECKING:
    from trpc_sdk.runtime import Reactor
    from trpc_sdk.runtime import ReactorGroup
    from trpc_sdk.runtime import ThreadModel


def ssl_context_from_opts(opts: BaseServerTransportOption) -> Optional[ssl.SSLContext]:
    """Build the server-side TLS context for one listen endpoint.

    Args:
        opts: Transport options holding the certificate and private key paths.

    Returns:
        A loaded ``SSLContext``, or ``None`` when TLS is not configured.

    Raises:
        KeyError: The private key path does not exist.
        ValueError: The certificate path does not exist.
    """
    key = opts.tls_key
    cert = opts.tls_cert

    if len(key) == 0 or len(cert) == 0:
        return None
    if not os.path.exists(key):
        raise KeyError(f"cert key file not exist: {key}")
    if not os.path.exists(cert):
        raise ValueError(f"cert key file not exist: {cert}")

    ssl_context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
    ssl_context.load_cert_chain(cert, key)
    return ssl_context


class TrpcServerTransport(BaseServerTransport):
    """Own one service's tRPC-over-TCP or tRPC-over-UDP listener.

    Constructed in the parent; :meth:`listen` must only be called after fork,
    because a socket created before the fork would be shared with every child.

    Accepted stream connections are Cython ``TCPConnection`` objects by
    default. A subclass serving another protocol over the same listen
    machinery calls :meth:`set_io_connection`, overrides :meth:`_msg_handler`
    and, when the framing differs, sets another ``opts.parser``; its
    connections are then :class:`DefaultIoConnection` objects. Datagrams are always served by :class:`DefaultDatagramConnection`, which
    hands every frame to :meth:`_msg_handler`.

    Attributes:
        opts: Options recorded by :meth:`bind`.
    """

    def __init__(self) -> None:
        super().__init__()
        self.opts = BaseServerTransportOption()
        # Listen sockets started and not stopped yet, closed by :meth:`stop`.
        self._listeners: List[StreamListener] = []
        # Listen sockets :meth:`stop` closed, waited for by :meth:`destroy`.
        self._closing: List[StreamListener] = []
        # Python connection class of accepted stream connections; ``None``
        # serves them with the Cython ``TCPConnection``.
        self._io_connection: Optional[Type[DefaultIoConnection]] = None

    def bind(self, opts: BaseServerTransportOption) -> None:
        """Record this endpoint's options. No socket is created.

        Args:
            opts: Listen address, protocol parser, handler, and timeouts.
        """
        self.opts = opts

    def _is_datagram(self) -> bool:
        """Whether this endpoint is tRPC over UDP; a composite network matches ``tcp`` first."""
        network = self.opts.network
        return network.find("tcp") == -1 and network.find("udp") != -1

    def check_endpoint(self) -> bool:
        """Check, before fork, that the configured address can be bound.

        A UNIX path is unlinked when stale. An inet address is probed: a
        stream endpoint with a connect, a datagram endpoint with a bind.

        Returns:
            Whether startup may continue; the reason is logged otherwise.
        """
        opts = self.opts
        address = opts.address
        if is_unix_socket(opts.socket_type):
            if not unix_path_valid(address[0]):
                logger.error("[server transport] cannot unlink unix address %s", address[0])
                return False
            return True
        if self._is_datagram():
            if not is_udp_port_available(address[0], address[1]):
                logger.error("[server transport] udp address %s invalid", address)
                return False
        elif not is_tcp_port_available(address[0], address[1]):
            logger.error("[server transport] tcp address %s invalid", address)
            return False
        return True

    def listen(self) -> None:
        """Serve the endpoint on the ``io`` reactors of the current thread model.

        Runs on the acceptor thread, before its loop runs.
        """
        # Local import: trpc_sdk.runtime imports this package at module level.
        from trpc_sdk.runtime import current_thread_model

        model = current_thread_model()
        if self._is_datagram():
            self._listen_datagram(model)
            return
        factory = self.protocol_factory(model.handler_pool)
        self._serve_stream(model, model.io, lambda _: factory)

    def stop(self) -> None:
        """Close every listen socket this transport started; idempotent.

        Open connections keep being served. Callable from a signal handler,
        on the thread :meth:`listen` ran on.
        """
        listeners, self._listeners = self._listeners, []
        for listener in listeners:
            listener.close()
        self._closing.extend(listeners)

    async def destroy(self) -> None:
        """Close every listen socket, wait until each ``asyncio`` server is gone, then forget them.

        On Python 3.12 and later ``wait_closed`` also waits for the accepted
        connections to close.
        """
        self.stop()
        closing, self._closing = self._closing, []
        for listener in closing:
            await listener.wait_closed()

    def set_io_connection(self, conn_cls: Type[DefaultIoConnection] = DefaultIoConnection) -> None:
        """Serve accepted stream connections with a Python connection class.

        Every frame then goes to :meth:`_msg_handler`; ``shared_pool`` is
        ignored and there is no tRPC stream support. Call before :meth:`listen`.

        Args:
            conn_cls: :class:`DefaultIoConnection` or a subclass of it.
        """
        self._io_connection = conn_cls

    def protocol_factory(self, shared_pool: Optional[Any] = None) -> Callable[[], Any]:
        """Return the connection class of accepted stream connections.

        Both connection classes take their loop from whichever one is running
        when they are constructed, so one factory is safe to share across
        loops.

        * Default: Cython :class:`TCPConnection`, which serves unary, stream
          and ``shared_pool`` requests itself.
        * After :meth:`set_io_connection`: that :class:`DefaultIoConnection`
          class, which hands every frame to :meth:`_msg_handler`. This path
          ignores ``shared_pool`` and has no tRPC stream support.

        Args:
            shared_pool: ``SharedHandlerPool`` that runs the handlers, or
                ``None`` to run them on the connection's loop.
        """
        opts = self.opts
        if self._io_connection is None:
            return partial(TCPConnection,
                           TcpConnectionOption(parser=opts.parser,
                                               rpc_handler=opts.rpc_handler,
                                               timeout=opts.idle_timeout,
                                               shared_pool=shared_pool))
        return partial(self._io_connection, ConnectionOption(parser=opts.parser, msg_handler=self._msg_handler))

    async def _msg_handler(self, conn: ServerConnection, data: bytes) -> None:
        """Serve one unary tRPC frame from a TCP connection or a UDP datagram.

        For TCP, ``conn`` is a :class:`DefaultIoConnection` and this only runs
        after :meth:`set_io_connection`; the default tRPC TCP path never calls
        this, since its connections are Cython ``TCPConnection`` objects.
        For UDP, ``conn`` is the per-datagram peer
        built by :class:`DefaultDatagramConnection`, and this runs on a handle
        thread under ``shared_pool``. It is the reference a subclass
        overrides, and may ``super()`` into for frames it does not handle
        itself. ``conn`` is put on ``ctx`` under ``SERVER_CONN_KEY``.

        Args:
            conn: Peer view the frame arrived on.
            data: One complete frame, as split by ``opts.parser``.
        """
        context = TrpcContext()
        get_message(context)
        set_addr_to_ctx(context, conn.local_addr, conn.remote_addr)
        context.timeout = self.opts.idle_timeout
        context.recv_time = now_ms()
        context.rpcz_data.server_post_sched_recv_msg_us = now_us()
        context.with_meta_data(RPCZ_SERVER_REQ_SIZE_CTX_KEY, len(data))
        context.with_meta_data(SERVER_CONN_KEY, conn)
        try:
            rsp_buff = await self.opts.rpc_handler(context, data)
            if not rsp_buff or conn.is_closing():
                return
            # Framing and sending are recorded as one instant; under
            # ``shared_pool`` the UDP send itself is posted to the I/O loop.
            send_us = now_us()
            context.rpcz_data.server_pre_sched_send_msg_us = send_us
            context.rpcz_data.server_post_sched_send_msg_us = send_us
            context.rpcz_data.server_pre_io_send_msg_us = send_us
            context.with_meta_data(RPCZ_SERVER_RSP_SIZE_CTX_KEY, len(rsp_buff))
            conn.write(rsp_buff)
            context.rpcz_data.server_post_io_send_msg_us = now_us()
            submit_server_span_on_executor_post_io(context)
        except Exception as err:  # pylint: disable=broad-except
            report_attr("trpc_executor_tcp_write_err", 1)
            logger.error(f"[server transport] failed to send rsp to client {conn.remote_addr}, err: {err}",
                         exc_info=True)

    def datagram_protocol_factory(self, shared_pool: Optional[Any] = None) -> Callable[[], Any]:
        """Return the protocol serving datagrams; safe to share across loops.

        Every frame goes through :meth:`_msg_handler`, overridden or not.

        Args:
            shared_pool: ``SharedHandlerPool`` that runs the handlers, or
                ``None`` to run them on the datagram's loop.
        """
        opts = self.opts
        return partial(DefaultDatagramConnection,
                       ConnectionOption(parser=opts.parser,
                                        msg_handler=self._msg_handler,
                                        shared_pool=shared_pool))

    def create_socket(self, reuse_port: bool, stream: bool = True) -> socket.socket:
        """Create and bind one socket on the configured address.

        Must only run after fork: a socket created before it would be shared
        with every child.

        Args:
            reuse_port: Set ``SO_REUSEPORT``.
            stream: True for a listening stream socket; False for a datagram one.

        Returns:
            A bound, non-blocking socket.
        """
        opts = self.opts
        return create_new_socket(opts.address, opts.socket_type, reuse_port, stream, opts.backlog)

    def _serve_stream(self,
                      model: ThreadModel,
                      group: ReactorGroup,
                      factory_for: Callable[[Reactor], Callable[[], asyncio.BaseProtocol]],
                      ssl_context: Optional[ssl.SSLContext] = None) -> StreamListener:
        """Bind one listen socket and serve it on ``group``; kept for :meth:`stop`.

        Args:
            model: Thread model of this process.
            group: Reactors that serve the accepted connections.
            factory_for: Protocol factory of the connections a reactor serves.
            ssl_context: Server ``SSLContext``, or ``None`` for plaintext.

        Returns:
            The started listener.
        """
        sock = self.create_socket(model.reuse_port)
        try:
            listener = serve_stream(model, group, sock, factory_for, self.opts.backlog,
                                    ssl_context=ssl_context, address=self.opts.address)
        except BaseException:
            sock.close()
            raise
        self._listeners.append(listener)
        return listener

    def _listen_datagram(self, model: ThreadModel) -> None:
        """Bind one datagram socket per ``io`` reactor; the kernel spreads datagrams over them.

        The socket is created on this thread and the endpoint on the reactor's
        loop, so the protocol object is constructed there. Each endpoint is
        kept on ``model`` for the runtime to release on that loop.

        Args:
            model: Thread model of this process.
        """
        from trpc_sdk.runtime import ResourceKind

        reactors = model.io.reactors
        reuse_port = model.reuse_port or len(reactors) > 1
        factory = self.datagram_protocol_factory(model.handler_pool)
        for reactor in reactors:
            sock = self.create_socket(reuse_port, stream=False)
            try:
                endpoint, _ = reactor.submit(partial(reactor.loop.create_datagram_endpoint,
                                                     protocol_factory=factory,
                                                     sock=sock),
                                             timeout=model.ready_timeout)
            except BaseException:
                sock.close()
                raise
            model.keep_resource(reactor, endpoint, ResourceKind.ENDPOINT)
