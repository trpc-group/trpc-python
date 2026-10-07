# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Dial a TCP or Unix stream for the client connection pools.

:func:`dial_connection` is the shared connect path used by
:class:`~trpc_sdk.transport._pool.ConnectionPoolManager` (as
``ConnectionPoolManager.dial``) and by
:class:`~trpc_sdk.transport._pool.ShortConnFactory`. Keeping it
in this module avoids a cycle between those two files.

The stdlib ``asyncio.open_connection`` / ``open_unix_connection`` do the
dial. asyncio already enables ``TCP_NODELAY`` on every TCP transport, so
only ``SO_LINGER`` and ``SO_KEEPALIVE`` are applied here.
"""

from __future__ import annotations

import asyncio
import socket
from asyncio import BaseEventLoop
from asyncio import StreamReader
from asyncio import StreamWriter
from typing import Awaitable
from typing import Tuple

from trpc_sdk.abc import BaseClientTransportOption
from trpc_sdk.common import wait_for_by_loop
from trpc_sdk.config import SocketType
from trpc_sdk.utils import is_ipv6
from trpc_sdk.utils import is_valid_socket
from trpc_sdk.utils import set_no_close_wait
from trpc_sdk.utils import set_tcp_keep_alive

from ._constants import DEFAULT_STREAM_LIMIT


async def dial_connection(
        options: BaseClientTransportOption,
        timeout: float,
        loop: BaseEventLoop) -> Tuple[StreamReader, StreamWriter]:
    """Open a TCP or Unix stream.

    A connected ``socket.socket`` is adopted. A ``(host, port)``
    pair dials Unix, IPv6, or IPv4 from ``socket_type`` / address
    family. ``local_addr`` is passed only on the IP paths.

    Args:
        options: Remote address or connected sock, plus bind hints.
        timeout: Seconds to wait for the connect coroutine.
        loop: Event loop the timeout is scheduled on.

    Returns:
        The stdlib ``(reader, writer)`` pair of the connected stream.

    Raises:
        AssertionError: ``address`` is neither a socket nor a
            two-tuple.
    """
    addr = options.sock or options.address
    if isinstance(addr, socket.socket):
        if addr.family == socket.AF_UNIX:
            co_open = asyncio.open_unix_connection(sock=addr, limit=DEFAULT_STREAM_LIMIT)
        else:
            co_open = asyncio.open_connection(sock=addr, limit=DEFAULT_STREAM_LIMIT)
        return await wait_for_by_loop(_open(co_open), timeout, loop=loop)

    if isinstance(addr, tuple) and len(addr) == 2:
        host, port = addr
        port = int(port)
        if options.socket_type == SocketType.UNIX:
            co_open = asyncio.open_unix_connection(host, limit=DEFAULT_STREAM_LIMIT)
        elif is_ipv6(host):
            co_open = asyncio.open_connection(host,
                                              port,
                                              limit=DEFAULT_STREAM_LIMIT,
                                              family=socket.AF_INET6,
                                              local_addr=options.local_addr)
        else:
            co_open = asyncio.open_connection(host,
                                              port,
                                              limit=DEFAULT_STREAM_LIMIT,
                                              local_addr=options.local_addr)
        return await wait_for_by_loop(_open(co_open), timeout, loop=loop)

    assert False, f"address is error type {options.address}"


async def _open(co_open: Awaitable[Tuple[StreamReader, StreamWriter]]) -> Tuple[StreamReader, StreamWriter]:
    """Await the stdlib dial, then apply the pool's TCP socket options.

    Args:
        co_open: ``asyncio.open_connection`` / ``open_unix_connection``
            coroutine.

    Returns:
        The connected ``(reader, writer)`` pair.
    """
    reader, writer = await co_open
    sock = writer.get_extra_info("socket")
    if is_valid_socket(sock) and sock.proto == socket.IPPROTO_TCP:
        set_no_close_wait(sock)
        set_tcp_keep_alive(sock)
    return reader, writer
