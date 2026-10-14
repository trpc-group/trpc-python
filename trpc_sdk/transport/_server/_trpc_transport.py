# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""tRPC server transport: record options before fork, listen after it.

:meth:`TrpcServerTransport.bind` runs in ``ServiceAdapter.run`` and only keeps the
options; ``ServiceAdapter.run`` then registers the transport on the engine, and every
forked worker inherits it through ``process_info.transports``.

:meth:`TrpcServerTransport.listen` runs in each serving process after fork.
``opts.network`` decides how it serves:

* ``tcp``: one listen socket. With no serving loops it is served by
  ``create_server`` on ``opt.loop``; otherwise an acceptor on ``opt.loop``
  hands each connection to one of ``opt.serving_loops``.
* ``udp``: nothing to accept, so every serving loop binds its own socket on
  the address and the kernel spreads the datagrams.

Everything a ``listen`` call starts is recorded on the transport, which is
safe because the transport object is only *created* before fork: the tables
are empty there and every forked process fills its own copy.
:class:`~trpc_sdk.http.HttpServerTransport` reuses all of it for aiohttp.
"""

from __future__ import annotations

import asyncio
import os
import socket
import ssl
from functools import partial
from typing import Any
from typing import Callable
from typing import Dict
from typing import Iterable
from typing import List
from typing import Optional
from typing import Sequence

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
from ._acceptor import AcceptTarget
from ._acceptor import Acceptor
from ._constants import SERVER_CONN_KEY
from ._datagram_connection import DefaultDatagramConnection
from ._listen import ListenOption
from ._stream_connection import ConnectionOption
from ._stream_connection import DefaultIoConnection
from ._stream_connection import ServerConnection
from ._stream_connection import TcpConnectionOption


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


async def release_loop_resources(resources: Iterable[Any]) -> None:
    """Release resources of one loop; await on that loop.

    aiohttp runners are cleaned up, which closes their connections; datagram
    transports are closed. One failure does not keep the rest open.

    Args:
        resources: What :meth:`TrpcServerTransport.pop_loop_resources` returned.
    """
    for resource in resources:
        try:
            cleanup = getattr(resource, "cleanup", None)
            if cleanup is not None:
                await cleanup()
            else:
                resource.close()
        except Exception as err:  # pylint: disable=broad-except
            logger.error("[server transport] failed to release %r: %s", resource, repr(err))


def _stop_site(loop: asyncio.AbstractEventLoop, site: Any) -> None:
    """Schedule ``site.stop()`` on the loop the site was started on."""
    loop.create_task(site.stop())


class TrpcServerTransport(BaseServerTransport):
    """Own one service's tRPC-over-TCP or tRPC-over-UDP listener.

    Constructed in the parent; :meth:`listen` must only be called after fork,
    because a socket created before the fork would be shared with every child.

    Accepted stream connections are Cython ``TCPConnection`` objects. A
    subclass serving another protocol over the same listen machinery
    overrides :meth:`_msg_handler` and, when the framing differs, sets another
    ``opts.parser``; its connections are then :class:`DefaultIoConnection`.
    Datagrams are always served by :class:`DefaultDatagramConnection`, which
    hands every frame to :meth:`_msg_handler`.

    Attributes:
        opts: Options recorded by :meth:`bind`.
    """

    def __init__(self) -> None:
        super().__init__()
        self.opts = BaseServerTransportOption()
        # What :meth:`stop` closes: servers, acceptors and aiohttp sites.
        self._closers: List[Callable[[], Any]] = []
        # ``asyncio.Server`` objects whose close :meth:`destroy` waits for.
        self._servers: List[asyncio.AbstractServer] = []
        # Datagram transports and aiohttp runners, keyed by the serving loop
        # that owns them (``None`` for ``opt.loop``). The runtime releases
        # them when that loop stops, not in :meth:`stop`: a datagram socket
        # must keep answering while requests drain, and neither may be closed
        # from another thread.
        self._loop_resources: Dict[Optional[int], List[Any]] = {}

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

    def listen(self, opt: Optional[ListenOption] = None) -> None:
        """Serve the endpoint on the loops in ``opt``.

        Args:
            opt: Loops, ``reuse_port`` and the optional connection override.
                ``None`` serves on the calling thread's loop alone.
        """
        if opt is None:
            opt = ListenOption(loop=asyncio.get_event_loop())
        if self._is_datagram():
            self._listen_datagram(opt)
            return
        factory = opt.protocol_factory or self.protocol_factory(opt.shared_pool)
        if not opt.serving_loops:
            self.serve_on_loop(opt.loop, factory, opt.reuse_port)
        else:
            self.serve_via_acceptor(opt, [AcceptTarget(target, factory) for target in opt.serving_loops])

    def stop(self) -> None:
        """Close every listen socket this transport started; idempotent.

        Open connections keep being served. Callable from a signal handler:
        an aiohttp site's ``stop`` is a coroutine, so it is scheduled on its
        loop and runs on that loop's next turn.
        """
        closers, self._closers = self._closers, []
        for close in closers:
            close()

    async def destroy(self) -> None:
        """Wait until every closed ``asyncio`` server is gone, then forget them.

        On Python 3.12 and later ``wait_closed`` also waits for the accepted
        connections to close.
        """
        servers, self._servers = self._servers, []
        for server in servers:
            server.close()
            await server.wait_closed()

    def pop_loop_resources(self, key: Optional[int]) -> List[Any]:
        """Take the datagram transports and aiohttp runners owned by one loop.

        Args:
            key: That serving loop's ``index``; ``None`` is ``opt.loop``.

        Returns:
            The resources, for :func:`release_loop_resources` on that loop.
        """
        return self._loop_resources.pop(key, [])

    def protocol_factory(self, shared_pool: Optional[Any] = None) -> Callable[[], Any]:
        """Return the connection class of accepted stream connections.

        Both connection classes take their loop from whichever one is running
        when they are constructed, so one factory is safe to share across
        loops.

        * :meth:`_msg_handler` not overridden: Cython :class:`TCPConnection`,
          which serves unary, stream and ``shared_pool`` requests itself.
        * Overridden by a subclass: :class:`DefaultIoConnection`, which hands
          every frame to that override. This path ignores ``shared_pool`` and
          has no tRPC stream support.

        Args:
            shared_pool: ``SharedHandlerPool`` that runs the handlers, or
                ``None`` to run them on the connection's loop.
        """
        opts = self.opts
        if type(self)._msg_handler is TrpcServerTransport._msg_handler:
            return partial(TCPConnection,
                           TcpConnectionOption(parser=opts.parser,
                                               rpc_handler=opts.rpc_handler,
                                               timeout=opts.idle_timeout,
                                               shared_pool=shared_pool))
        return partial(DefaultIoConnection, ConnectionOption(parser=opts.parser, msg_handler=self._msg_handler))

    async def _msg_handler(self, conn: ServerConnection, data: bytes) -> None:
        """Serve one unary tRPC frame from a TCP connection or a UDP datagram.

        For TCP, ``conn`` is a :class:`DefaultIoConnection`; the default tRPC
        TCP path never calls this, since its connections are Cython
        ``TCPConnection`` objects. 
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

    def datagram_protocol_factory(self, opt: ListenOption) -> Callable[[], Any]:
        """Return the protocol serving datagrams; safe to share across loops.

        Every frame goes through :meth:`_msg_handler`, overridden or not.

        Args:
            opt: Supplies ``shared_pool``.
        """
        opts = self.opts
        return partial(DefaultDatagramConnection,
                       ConnectionOption(parser=opts.parser,
                                        msg_handler=self._msg_handler,
                                        shared_pool=opt.shared_pool))

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

    def serve_on_loop(self,
                      loop: asyncio.AbstractEventLoop,
                      factory: Callable[[], asyncio.BaseProtocol],
                      reuse_port: bool) -> asyncio.AbstractServer:
        """Serve a new listen socket with ``create_server`` on ``loop``.

        Args:
            loop: Loop that accepts and serves; must not be running yet.
            factory: Protocol factory of accepted connections.
            reuse_port: Set ``SO_REUSEPORT`` on the socket.

        Returns:
            The started server, also kept for :meth:`stop` and :meth:`destroy`.
        """
        sock = self.create_socket(reuse_port)
        coro = loop.create_server(factory, sock=sock, reuse_port=reuse_port, backlog=self.opts.backlog)
        server = loop.run_until_complete(coro)
        self._servers.append(server)
        self._closers.append(server.close)
        return server

    def serve_via_acceptor(self,
                           opt: ListenOption,
                           targets: Sequence[AcceptTarget],
                           ssl_context: Optional[ssl.SSLContext] = None) -> Acceptor:
        """Accept on ``opt.loop`` and hand every connection to one of ``targets``.

        Args:
            opt: ``loop`` watches the socket; ``reuse_port`` and
                ``schedule_target`` are applied.
            targets: Serving loops and the protocol factory for each.
            ssl_context: Server ``SSLContext``, or ``None`` for plaintext.

        Returns:
            The started acceptor, also kept for :meth:`stop`.
        """
        sock = self.create_socket(opt.reuse_port)
        acceptor = Acceptor(sock=sock,
                            targets=targets,
                            loop=opt.loop,
                            address=self.opts.address,
                            ssl_context=ssl_context,
                            schedule_target=opt.schedule_target)
        acceptor.start()
        self._closers.append(acceptor.close)
        return acceptor

    def keep_site(self, loop: asyncio.AbstractEventLoop, site: Any) -> None:
        """Record a started aiohttp site so :meth:`stop` closes it.

        Args:
            loop: Loop the site was started on.
            site: The started ``web.BaseSite``.
        """
        self._closers.append(partial(_stop_site, loop, site))

    def keep_loop_resource(self, key: Optional[int], resource: Any) -> None:
        """Record a resource the runtime releases when the loop at ``key`` stops.

        Args:
            key: That serving loop's ``index``; ``None`` is ``opt.loop``.
            resource: A datagram transport (``close()``) or an aiohttp runner
                (``await cleanup()``).
        """
        self._loop_resources.setdefault(key, []).append(resource)

    def _listen_datagram(self, opt: ListenOption) -> None:
        """Bind one datagram socket per serving loop and serve it there.

        The socket is created on this thread and the endpoint on the target
        loop, so the protocol object is constructed there. Each loop's
        datagram transport is kept for
        :meth:`pop_loop_resources`.

        Args:
            opt: With no serving loops the endpoint is served on ``opt.loop``.
        """
        factory = self.datagram_protocol_factory(opt)
        if not opt.serving_loops:
            sock = self.create_socket(opt.reuse_port, stream=False)
            endpoint = opt.loop.create_datagram_endpoint(protocol_factory=factory, sock=sock)
            transport, _ = opt.loop.run_until_complete(endpoint)
            self.keep_loop_resource(None, transport)
            return

        for target in opt.serving_loops:
            sock = self.create_socket(opt.reuse_port, stream=False)
            try:
                transport, _ = target.submit(partial(target.loop.create_datagram_endpoint,
                                                     protocol_factory=factory,
                                                     sock=sock),
                                             timeout=opt.ready_timeout)
            except BaseException:
                sock.close()
                raise
            self.keep_loop_resource(target.index, transport)
