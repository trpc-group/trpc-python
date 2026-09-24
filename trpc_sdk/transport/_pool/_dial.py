# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Dial a TCP or Unix :class:`~trpc_sdk.transport._pool.Connection`.

:func:`dial_connection` is the shared connect path used by
:class:`~trpc_sdk.transport._pool.ConnectionPoolManager` (as
``ConnectionPoolManager.dial``) and by
:class:`~trpc_sdk.transport._pool.ShortConnFactory`. Keeping it
in this module avoids a cycle between those two files.
"""

from __future__ import annotations

import socket
from asyncio import BaseEventLoop

from trpc_sdk.abc import BaseClientTransportOption
from trpc_sdk.common import wait_for_by_loop
from trpc_sdk.config import SocketType
from trpc_sdk.utils import is_ipv6
from ._tcp_connection import Connection
from ._tcp_connection import open_connection
from ._tcp_connection import open_unix_connection


async def dial_connection(
        options: BaseClientTransportOption,
        timeout: float,
        loop: BaseEventLoop) -> Connection:
    """Open a TCP or Unix :class:`~trpc_sdk.transport._pool.Connection`.

    A connected ``socket.socket`` is adopted. A ``(host, port)``
    pair dials Unix, IPv6, or IPv4 from ``socket_type`` / address
    family. ``local_addr`` is passed only on the IP paths.

    Args:
        options: Remote address or connected sock, plus bind hints.
        timeout: Seconds to wait for the connect coroutine.
        loop: Event loop recorded on the new connection.

    Returns:
        A connected :class:`~trpc_sdk.transport._pool.Connection`.

    Raises:
        AssertionError: ``address`` is neither a socket nor a
            two-tuple.
    """
    addr = options.sock or options.address
    if isinstance(addr, socket.socket):
        if addr.family == socket.AF_UNIX:
            co_conn = open_unix_connection(sock=addr, loop=loop)
        else:
            co_conn = open_connection(sock=addr, loop=loop)
        return await wait_for_by_loop(co_conn, timeout, loop=loop)

    if isinstance(addr, tuple) and len(addr) == 2:
        host, port = addr
        port = int(port)
        if options.socket_type == SocketType.UNIX:
            co_conn = open_unix_connection(host=host, loop=loop)
        elif is_ipv6(host):
            co_conn = open_connection(host=host,
                                      port=port,
                                      loop=loop,
                                      family=socket.AF_INET6,
                                      local_addr=options.local_addr)
        else:
            co_conn = open_connection(host=host, port=port, loop=loop, local_addr=options.local_addr)
        return await wait_for_by_loop(co_conn, timeout, loop=loop)

    assert False, f"address is error type {options.address}"
