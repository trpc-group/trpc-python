# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Listener selection, the user protocol registry, and single-loop UDP / HTTP listeners.

:func:`classify_address` is the one place that decides which listener serves
a listen address. ``merge`` and ``separate`` both call it, so the two modes
can never disagree about what an address is; they only differ in which
process binds it:

* ``merge``: the server process binds everything.
* ``separate``: the I/O process binds the TCP addresses, whose tRPC frames it
  can cut and forward. USER, UDP and HTTP addresses are bound by every rpc
  task itself, with ``SO_REUSEPORT``, because a frame the I/O process cannot
  delimit (or an aiohttp request) cannot be forwarded over the pipe.

A service whose ``protocol`` has a starter registered with
:func:`register_protocol_handle` is not served by the built-in tcp/udp/http
listeners. Instead the starter is called once per listen address, in every
process that serves that address, and binds its own socket. The registry is
a module-level dict filled at import time, **before**
:meth:`~trpc_sdk.server.Server.serve`. That is what makes it work across the
fork: every child inherits the same entries, while a table hung on a worker
instance would be invisible to application code, which never gets to see
the worker before ``serve()`` runs it.

The helpers below take a worker for its per-address tables (``addresses``,
``socket_types``, ``checkers``, ``handlers``, ``timeouts``, ``tran_opts``,
``services``), so any :class:`~trpc_sdk.server.BaseWorker` that called
``_bind_process_info`` can use them. ``start_*`` drive the given loop with
``run_until_complete`` and therefore need a loop that is not running yet;
:class:`~trpc_sdk.server.MergeWorker` reuses the factories underneath for its
loop threads.
"""

from __future__ import annotations

import asyncio
import socket
from functools import partial
from typing import Any
from typing import Callable
from typing import Dict
from typing import Iterable
from typing import Optional
from typing import Tuple

from aiohttp import hdrs
from aiohttp import web

from trpc_sdk.config import TrpcConfig
from trpc_sdk.http import HttpMessageHandle
from trpc_sdk.transport import ExecutorUDPConnection
from trpc_sdk.transport import create_server_socket
from ._constants import LISTEN_BACKLOG
from ._constants import AddressKind
from ._constants import Network
from ._utils import add_user_route
from ._utils import get_https_certs
from ._utils import reg_static_file

# ``starter(worker, index)``: ``worker`` is the
# :class:`~trpc_sdk.server.BaseWorker` serving the address, which gives the
# starter its event loop (``worker.loop``) and the per-address tables
# (``addresses``, ``handlers``, ``timeouts``, ``tran_opts``, ...); ``index``
# selects the address. Called after the loop exists and before it runs.
ProtocolStarter = Callable[[Any, int], None]

_PROTOCOL_HANDLES: Dict[str, ProtocolStarter] = {}


def register_protocol_handle(protocol: str, starter: ProtocolStarter) -> None:
    """Serve every address of ``protocol`` with ``starter`` instead of a built-in listener.

    Must be called at import time, before ``Server.serve()``; a registration
    made later never reaches the processes that were already forked.
    Registering a protocol again replaces its starter.

    Args:
        protocol: Service ``protocol`` value from the YAML config.
        starter: Called as ``starter(worker, index)`` for each matching
            listen address. It must bind its own socket on ``worker.loop``;
            in ``separate`` mode several rpc tasks bind the same address, so
            it should set ``SO_REUSEPORT``.
    """
    _PROTOCOL_HANDLES[protocol] = starter


def get_protocol_handle(protocol: str) -> Optional[ProtocolStarter]:
    """Return the starter registered for ``protocol``.

    Args:
        protocol: Service ``protocol`` value.

    Returns:
        The starter, or ``None`` when the built-in listeners serve it.
    """
    return _PROTOCOL_HANDLES.get(protocol)


def classify_address(worker: Any, index: int) -> AddressKind:
    """Decide which listener serves the listen address at ``index``.

    Priority: a protocol registered with :func:`register_protocol_handle`,
    then the network type. A composite network such as ``tcp,http`` matches
    TCP first, as merge mode always has.

    Args:
        worker: Worker whose per-address tables describe the address.
        index: Position of the address in ``worker.addresses``.

    Returns:
        The kind of listener, :attr:`AddressKind.UNKNOWN` when none applies.
    """
    if get_protocol_handle(worker.services[index].opt.protocol) is not None:
        return AddressKind.USER
    network = worker.networks[index]
    if network.find(Network.TCP) != -1:
        return AddressKind.TCP
    if network.find(Network.UDP) != -1:
        return AddressKind.UDP
    if network.find(Network.HTTP) != -1:
        return AddressKind.HTTP
    return AddressKind.UNKNOWN


def start_user_protocol(worker: Any, index: int) -> None:
    """Hand the address at ``index`` to the starter registered for its protocol.

    Args:
        worker: Worker serving the address; passed on to the starter.
        index: Position of the address in ``worker.addresses``. Must be
            classified :attr:`AddressKind.USER`.
    """
    get_protocol_handle(worker.services[index].opt.protocol)(worker, index)


def udp_protocol_factory(worker: Any, index: int) -> Callable[[], asyncio.DatagramProtocol]:
    """Return the datagram protocol factory for the address at ``index``.

    Safe to share across loops: the protocol takes its loop from whichever
    one is running when it is constructed.

    Args:
        worker: Worker that owns the handler, checker and timeout tables.
        index: Position of the address in ``worker.addresses``.
    """
    return partial(ExecutorUDPConnection,
                   app=worker,
                   checker=worker.checkers[index],
                   handle=worker.handlers[index],
                   timeout=worker.timeouts[index])


def create_udp_socket(worker: Any, index: int) -> socket.socket:
    """Bind one datagram socket for the address at ``index``.

    The trailing ``stream=False`` is what makes ``create_server_socket``
    hand back a ``SOCK_DGRAM`` socket instead of a listening stream one.

    Args:
        worker: Worker whose ``addresses`` / ``socket_types`` describe it.
        index: Position of the address in ``worker.addresses``.

    Returns:
        A bound, non-blocking datagram socket with ``SO_REUSEPORT`` set, so
        every serving loop (or rpc task) can bind the same address.
    """
    return create_server_socket(worker.addresses[index], worker.socket_types[index], True, False)


def start_udp(worker: Any, index: int, loop: asyncio.AbstractEventLoop) -> None:
    """Serve tRPC over UDP on one address from one loop.

    Several processes or loops may each call this for the same address; the
    kernel spreads datagrams over their ``SO_REUSEPORT`` sockets on Linux.

    Args:
        worker: Worker serving the address.
        index: Position of the address in ``worker.addresses``.
        loop: Loop that will own the endpoint; must not be running yet.
    """
    sock = create_udp_socket(worker, index)
    endpoint = loop.create_datagram_endpoint(protocol_factory=udp_protocol_factory(worker, index), sock=sock)
    loop.run_until_complete(endpoint)


def create_http_msg_handle(worker: Any, index: int) -> HttpMessageHandle:
    """Build the aiohttp-to-tRPC bridge for the HTTP address at ``index``.

    It holds no loop state, so one instance may serve every loop serving that
    address.

    Args:
        worker: Worker that owns the handler and timeout tables.
        index: Position of the address in ``worker.addresses``.
    """
    return HttpMessageHandle(index, worker, is_base64=TrpcConfig.server_config().encode_base64)


def build_http_app(worker: Any, index: int, msg_handle: HttpMessageHandle) -> web.Application:
    """Assemble the aiohttp application for one HTTP endpoint.

    Routes are added most specific first: user routes, then static files,
    then a catch-all that forwards anything left to the tRPC handler.
    Registering the catch-all earlier would shadow the other two.

    Args:
        worker: Worker serving the address.
        index: Position of the address in ``worker.addresses``.
        msg_handle: Bridge from :func:`create_http_msg_handle`.

    Returns:
        An application ready to be handed to an ``AppRunner``.
    """
    service_adapter = worker.services[index]
    app = web.Application(client_max_size=TrpcConfig.server_config().max_rbuf_size)

    add_user_route(app, service_adapter.opt.service_name, msg_handle)

    reg_static_file(app)

    resource = app.router.add_resource(r'/{name:.*}')
    resource.add_route(hdrs.METH_ANY, msg_handle.handle_http_request)

    return app


async def setup_http_runner(worker: Any, index: int, msg_handle: HttpMessageHandle) -> web.AppRunner:
    """Build and start an aiohttp runner on the loop that will serve it.

    Must be awaited on the target loop: ``setup()`` is where the runner's
    ``web.Server`` captures its loop.

    Args:
        worker: Worker serving the address.
        index: Position of the address in ``worker.addresses``.
        msg_handle: Bridge from :func:`create_http_msg_handle`.

    Returns:
        A runner whose ``server`` attribute is usable as a protocol factory.
    """
    runner = web.AppRunner(build_http_app(worker, index, msg_handle))
    await runner.setup()
    return runner


def start_http_site(worker: Any,
                    index: int,
                    msg_handle: HttpMessageHandle,
                    loop: asyncio.AbstractEventLoop) -> Tuple[web.AppRunner, web.TCPSite]:
    """Serve one HTTP endpoint from one loop through ``web.TCPSite``.

    The site binds for itself with ``SO_REUSEPORT``, so several rpc tasks can
    each run one on the same address.

    Args:
        worker: Worker serving the address. Its ``tran_opts`` entry decides
            whether the endpoint is HTTPS.
        index: Position of the address in ``worker.addresses``.
        msg_handle: Bridge from :func:`create_http_msg_handle`.
        loop: Loop that will serve the site; must not be running yet.

    Returns:
        The runner and the started site. Keep both: the runner must stay
        referenced while it serves, and the site is what
        :func:`stop_http_sites` closes.
    """
    # None unless the transport options carry a cert/key pair.
    ssl_context = get_https_certs(worker.tran_opts[index])
    runner = loop.run_until_complete(setup_http_runner(worker, index, msg_handle))
    site = web.TCPSite(runner,
                       host=worker.addresses[index][0],
                       port=worker.addresses[index][1],
                       reuse_port=True,
                       backlog=LISTEN_BACKLOG,
                       ssl_context=ssl_context)
    loop.run_until_complete(site.start())
    return runner, site


def stop_http_sites(loop: asyncio.AbstractEventLoop, sites: Iterable[web.BaseSite]) -> None:
    """Stop accepting on each site; open connections are left alone.

    ``BaseSite.stop`` is a coroutine, so it is scheduled on ``loop`` and runs
    on the loop's next turn. Callable from a signal handler.

    Args:
        loop: Loop the sites were started on.
        sites: Sites returned by :func:`start_http_site`.
    """
    for site in sites:
        loop.create_task(site.stop())
