# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""UDP ``protocol: user`` for the single-process helloworld layouts.

Import this module before ``Server.serve()``. Importing registers
``register_protocol_handle("user", _start_user)``. A config with no
``protocol: user`` service never calls the starter.

The runtime starts that starter once, on the serving loop whose index is 0:

* single-loop merge: the main loop
* threaded merge: ``trpc-loop-0``
* separate-thread: ``trpc-handle-0``

Built-in ``protocol: trpc`` UDP is a different listener and still follows
the I/O loops. The reply is ``user:<index>:<pid>``. In these layouts
``index`` is 0 and ``pid`` is the server process.

``SO_REUSEPORT`` is set, matching the multi-process starters. These layouts
bind the address once, so the option does not create extra sockets.
"""

from __future__ import annotations

import asyncio
import os
from typing import List

from trpc_sdk.runtime import current_handler_thread
from trpc_sdk.server import register_protocol_handle
from trpc_sdk.utils import create_new_socket

USER_PROTOCOL = "user"

# Asyncio datagram transports created by :func:`_start_user`. The runtime's
# ``worker._user_transports`` is a different table (a dict of framework
# transports on a merge worker) and is destroyed on shutdown with a no-op
# ``destroy``. Replacing or appending to it would drop that table or raise.
# These endpoints stay referenced here for the life of the process; the
# framework does not close a user protocol's socket.
_HELD: List[asyncio.BaseTransport] = []


class _UserEcho(asyncio.DatagramProtocol):
    """UDP protocol that answers ``user:<index>:<pid>`` to any datagram."""

    def connection_made(self, transport) -> None:
        """Keep the transport used to send the reply.

        Args:
            transport: The endpoint this protocol was bound with.
        """
        self.transport = transport

    def datagram_received(self, data: bytes, addr) -> None:
        """Reply with this serving loop's index and pid.

        The runtime calls :func:`_start_user` on the loop whose index is 0,
        and that loop's identity is already installed on this thread.

        Args:
            data: Ignored. Any payload gets the same reply.
            addr: Where to send the reply.
        """
        del data
        info = current_handler_thread()
        index = info.index if info is not None else -1
        self.transport.sendto(f"user:{index}:{os.getpid()}".encode(), addr)


def _start_user(worker, index: int) -> None:
    """Bind one ``SO_REUSEPORT`` UDP socket for the user protocol.

    Called once, on serving loop 0, before that loop runs. ``index`` selects
    the listen address, not the serving-loop index in the reply.

    Args:
        worker: Worker serving the address. On a threaded worker this is a
            view whose ``loop`` is serving loop 0, not the main loop.
        index: Position of the address in ``worker.addresses``.
    """
    sock = create_new_socket(worker.addresses[index], worker.socket_types[index], True, False)
    sock.setblocking(False)
    endpoint = worker.loop.create_datagram_endpoint(lambda: _UserEcho(), sock=sock)
    transport, _protocol = worker.loop.run_until_complete(endpoint)
    _HELD.append(transport)


register_protocol_handle(USER_PROTOCOL, _start_user)
