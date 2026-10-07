# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Listen-socket constructors used by dispatcher and executor processes.

These helpers run in the child after fork. The returned socket is already
bound (and, for stream sockets, listening) and marked non-blocking so
asyncio can wrap it with ``create_server`` / ``create_datagram_endpoint``.
"""

from __future__ import annotations

import os
import socket
from typing import Tuple

from trpc_sdk.utils import is_ipv4
from trpc_sdk.utils import is_ipv6
from trpc_sdk.utils import is_unix_socket
from trpc_sdk.utils import set_no_close_wait
from trpc_sdk.utils import set_tcp_keep_alive
from trpc_sdk.utils import set_tcp_no_delay
from trpc_sdk.utils import unlink_unix_socket_file

from ._constants import DEFAULT_BACKLOG


def create_server_socket(address: Tuple[str, int],
                         socket_type: str,
                         reuse_port: bool = False,
                         stream: bool = True) -> socket.socket:
    """Create and bind the listen socket for one service address.

    Args:
        address: ``(host, port)`` for inet, or ``(unix_path, unused_port)``
            for a UNIX socket. Only ``address[0]`` is used for UNIX.
        socket_type: Socket family name; UNIX sockets are detected by
            :func:`trpc_sdk.utils.is_unix_socket`.
        reuse_port: When True, set ``SO_REUSEPORT`` so every executor in
            multi-listener mode can bind the same inet address.
        stream: True for TCP / UNIX stream; False for UDP datagram.

    Returns:
        A non-blocking, already-bound socket. Stream sockets are listening.
    """
    if is_unix_socket(socket_type):
        return create_unix_socket(address)
    return create_inet_socket(address, reuse_port, stream)


def create_inet_socket(address: Tuple[str, int],
                    reuse_port: bool = False,
                    stream: bool = True) -> socket.socket:
    """Create, bind, and optionally listen on an IPv4 or IPv6 socket.

    Args:
        address: ``(host, port)`` to bind. The host decides AF_INET vs AF_INET6.
        reuse_port: When True, set ``SO_REUSEPORT``.
        stream: True for ``SOCK_STREAM`` (TCP); False for ``SOCK_DGRAM`` (UDP).

    Returns:
        A non-blocking bound socket. TCP sockets also have NODELAY, keepalive,
        and lingering disabled, and are already listening.
    """
    sock = None
    stream_option = socket.SOCK_STREAM if stream else socket.SOCK_DGRAM
    if is_ipv4(address[0]):
        sock = socket.socket(socket.AF_INET, stream_option)
    elif is_ipv6(address[0]):
        sock = socket.socket(socket.AF_INET6, stream_option)
    if stream:
        sock.setblocking(False)
        set_tcp_no_delay(sock)
        set_no_close_wait(sock)
        set_tcp_keep_alive(sock)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    if reuse_port:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
    sock.bind(address)
    if stream:
        sock.listen(DEFAULT_BACKLOG)
    sock.setblocking(False)
    return sock


def create_unix_socket(address: Tuple[str, int]) -> socket.socket:
    """Create and listen on a UNIX-domain stream socket.

    Unlinks ``address[0]`` first when that path already exists, so a stale
    socket file from a previous process does not make bind fail.

    Args:
        address: ``(unix_path, unused_port)``; only the path is bound.

    Returns:
        A non-blocking listening UNIX stream socket.
    """
    if os.path.exists(address[0]):
        unlink_unix_socket_file(address[0])
    stream_option = socket.SOCK_STREAM
    sock = socket.socket(socket.AF_UNIX, stream_option)
    sock.bind(address[0])
    sock.listen(DEFAULT_BACKLOG)
    sock.setblocking(False)
    return sock
