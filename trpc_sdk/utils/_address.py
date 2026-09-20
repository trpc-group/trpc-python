# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Host/port helpers for asyncio transports and the tRPC message context.

:data:`AddrType` is defined in :mod:`trpc.utils._types` and re-exported
here so ``from trpc.utils._address import AddrType`` stays valid. Unix
paths arrive as a string ``sockname``/``peername`` and are coerced to
``(path, 0)`` before being written onto the context.
"""

from __future__ import annotations

import asyncio
from typing import Tuple

from ._types import AddrType


def get_address_from_str(address: str) -> AddrType:
    """Parse ``host:port`` using the last colon so IPv6 hosts still work.

    Args:
        address: Text form such as ``127.0.0.1:8080`` or ``[::1]:8080``
            without brackets (callers that add brackets strip them first).

    Returns:
        ``(host, port)`` tuple.

    Raises:
        ValueError: If ``address`` contains no colon.
    """
    data = address.split(":")
    if len(data) < 2:
        raise ValueError("Invalid address format")
    colon_index = address.rfind(':')
    host = address[:colon_index]
    port = int(address[colon_index + 1:])
    return (host, port)


def convert_address_to_str(address: AddrType) -> str:
    """Format an address tuple, or return a string address unchanged.

    Unix transports give ``sockname`` as a path string; those are passed
    through. Inet tuples become ``host:port`` without IPv6 brackets.

    Args:
        address: ``(host, port)`` or an already-formatted string.

    Returns:
        Text form of the address.
    """
    if isinstance(address, str):
        return address
    return f"{address[0]}:{address[1]}"


def get_addr_from_transport(transport: asyncio.Transport) -> Tuple[str, str]:
    """Return ``(local, peer)`` as strings from an asyncio transport.

    Missing extra info falls back to ``0.0.0.0:0``.

    Args:
        transport: Connected asyncio transport.

    Returns:
        Pair of ``host:port`` (or path) strings.
    """
    local_addr = transport.get_extra_info('sockname') or ("0.0.0.0", 0)
    peer_addr = transport.get_extra_info('peername') or ("0.0.0.0", 0)
    return (convert_address_to_str(local_addr), convert_address_to_str(peer_addr))


def set_addr_to_ctx_from_transport(ctx, transport: asyncio.Transport) -> None:
    """Copy local/peer addresses from ``transport`` onto ``ctx`` and its message.

    Unix sockets yield a path string; those are stored as ``(path, 0)``.
    The protocol message gets host strings via ``set_local_addr`` /
    ``set_remote_addr``; the context also stores ``remote_port`` /
    ``local_port``.

    Args:
        ctx: Server or client context with ``get_client_message``.
        transport: Connected asyncio transport.

    Raises:
        ValueError: If sockname/peername is not a length-2 tuple after coercion.
    """
    local_addr = transport.get_extra_info('sockname') or ("0.0.0.0", 0)
    peer_addr = transport.get_extra_info('peername') or ("0.0.0.0", 0)
    if isinstance(local_addr, str):
        local_addr = (local_addr, 0)
    if isinstance(peer_addr, str):
        peer_addr = (peer_addr, 0)
    if not isinstance(local_addr, tuple) or len(local_addr) != 2:
        raise ValueError(f"Invalid local address {local_addr}")
    if not isinstance(peer_addr, tuple) or len(peer_addr) != 2:
        raise ValueError(f"Invalid peer address {peer_addr}")
    msg = ctx.get_client_message()
    msg.set_local_addr(local_addr[0])
    msg.set_remote_addr(peer_addr[0])
    ctx.remote_ip = peer_addr[0]
    ctx.remote_port = peer_addr[1]
    ctx.local_ip = local_addr[0]
    ctx.local_port = local_addr[1]


def make_addr_info_to_bytes(transport: asyncio.Transport) -> bytes:
    """Encode local and peer addresses as ``local|peer`` bytes for IPC.

    Args:
        transport: Connected asyncio transport.

    Returns:
        UTF-8 payload consumed by :func:`parse_addr_info_from_bytes`.
    """
    local_addr, peer_addr = get_addr_from_transport(transport)
    return local_addr.encode() + b"|" + peer_addr.encode()


def parse_addr_info_from_bytes(addr_info: bytes) -> Tuple[AddrType, AddrType]:
    """Decode the ``local|peer`` blob produced by :func:`make_addr_info_to_bytes`.

    Args:
        addr_info: Bytes with a single ``|`` separator.

    Returns:
        ``(local, peer)`` as :data:`AddrType` tuples.
    """
    local_addr, peer_addr = addr_info.split(b"|")
    return (get_address_from_str(local_addr.decode()), get_address_from_str(peer_addr.decode()))
