# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""User protocol starters, served through :class:`UserProtocolTransport`.

A service whose ``protocol`` has a starter registered with
:func:`register_protocol_handle` is not served by the built-in tcp/udp/http
transports. ``ServiceAdapter.init_transport`` swaps its transport for a
:class:`UserProtocolTransport` (see :func:`resolve_server_transport`), whose
``listen`` calls the starter once in every process that serves the endpoint;
the starter binds its own socket.

The registry is a module-level dict filled at import time, **before**
:meth:`~trpc_sdk.server.Server.serve`. That is what makes it work across the
fork: every child inherits the same entries, while a table hung on a worker
instance would be invisible to application code, which never gets to see the
worker before ``serve()`` runs it.
"""

from __future__ import annotations

import asyncio
from typing import Any
from typing import Callable
from typing import Dict
from typing import Optional

from trpc_sdk.abc import BaseServerTransport
from trpc_sdk.abc import BaseServerTransportOption

# ``starter(worker, index)``: ``worker`` is the
# :class:`~trpc_sdk.runtime.BaseWorker` serving the address, which gives the
# starter its event loop (``worker.loop``) and the per-address tables
# (``addresses``, ``handlers``, ``timeouts``, ``tran_opts``, ...); ``index``
# selects the address. Called after the loop exists and before it runs.
ProtocolStarter = Callable[[Any, int], None]

_PROTOCOL_HANDLES: Dict[str, ProtocolStarter] = {}


def register_protocol_handle(protocol: str, starter: ProtocolStarter) -> None:
    """Serve every address of ``protocol`` with ``starter`` instead of a built-in listener.

    Must be called before ``Server.serve()``: the transport is resolved when
    the services run, and a registration made later never reaches the
    processes that were already forked. Registering a protocol again
    replaces its starter.

    Args:
        protocol: Service ``protocol`` value from the YAML config.
        starter: Called as ``starter(worker, index)`` for each matching
            listen address. It must bind its own socket on ``worker.loop``;
            in ``separate`` and ``peer`` mode several processes bind the same
            address, so it should set ``SO_REUSEPORT``.
    """
    _PROTOCOL_HANDLES[protocol] = starter


def get_protocol_handle(protocol: str) -> Optional[ProtocolStarter]:
    """Return the starter registered for ``protocol``.

    Args:
        protocol: Service ``protocol`` value.

    Returns:
        The starter, or ``None`` when the built-in transports serve it.
    """
    return _PROTOCOL_HANDLES.get(protocol)


class UserProtocolTransport(BaseServerTransport):
    """Serve one endpoint with a starter registered by the application.

    The framework neither checks the address before fork nor closes anything
    the starter opened: the starter owns its socket.

    Attributes:
        starter: The registered ``starter(worker, index)``.
        opts: Options recorded by :meth:`bind`.
    """

    def __init__(self, starter: ProtocolStarter) -> None:
        super().__init__()
        self.starter = starter
        self.opts = BaseServerTransportOption()

    def bind(self, opts: BaseServerTransportOption) -> None:
        """Record the options. Called before fork.

        Args:
            opts: Listen options of the service.
        """
        self.opts = opts

    def listen(self) -> None:
        """Call the starter with the serving worker, as seen from the calling thread's loop.

        The worker and this endpoint's index come from the current thread
        model; the starter keeps whatever it opened.

        Raises:
            ValueError: The thread model names no worker: the starter needs one.
        """
        from trpc_sdk.runtime import current_thread_model

        model = current_thread_model()
        if model.worker is None:
            raise ValueError(f"user protocol {self.opts.address} needs a worker in the thread model")
        worker = model.worker_view(asyncio.get_event_loop())
        self.starter(worker, model.worker.transports.index(self))


def resolve_server_transport(protocol: str, transport: BaseServerTransport) -> BaseServerTransport:
    """Return the transport that serves a service of ``protocol``.

    Args:
        protocol: Service ``protocol`` value.
        transport: The transport chosen when the service was created.

    Returns:
        A :class:`UserProtocolTransport` when ``protocol`` has a starter
        registered, else ``transport`` unchanged.
    """
    starter = get_protocol_handle(protocol)
    if starter is None:
        return transport
    return UserProtocolTransport(starter)
