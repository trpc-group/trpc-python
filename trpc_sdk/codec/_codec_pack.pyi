# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Type hints for the leftover hand-packed frame builder.

Nothing in the tree imports this module. New code uses
:func:`trpc_sdk.codec._c_codec._common_pack`. ``struct_pack`` writes
``stream_id`` as 2 bytes and ``frame_reserved`` as 4 bytes, which does
not match the real 16-byte tRPC head.
"""

from __future__ import annotations


def endian_swap_uint2bytes(i: int) -> bytes:
    """Return a 4-byte big-endian encoding of ``i``.

    Args:
        i: Host-endian 32-bit value.
    """
    ...


def endian_swap_ushort2bytes(i: int) -> bytes:
    """Return a 2-byte big-endian encoding of ``i``.

    Args:
        i: Host-endian 16-bit value.
    """
    ...


def endian_swap_uchar2bytes(i: int) -> bytes:
    """Return a 1-byte encoding of ``i``.

    Args:
        i: 8-bit value; no endian swap is needed.
    """
    ...


def struct_pack(
    req_head: bytes,
    frame_head_len: int,
    magic_value: int,
    frame_type: int,
    frame_state: int,
    stream_id: int,
    frame_reserved: int,
    body: bytes,
) -> bytes:
    """Join a frame head, protocol head, and body with ``b''.join``.

    Field widths do not match the on-wire 16-byte head, so the result
    is not a valid tRPC frame.

    Args:
        req_head: Serialized protocol head (``RequestProtocol``).
        frame_head_len: Claimed frame-head size, usually 16.
        magic_value: ``TRPC_MAGIC_VALUE``.
        frame_type: Unary vs stream.
        frame_state: Stream sub-type; 0 on unary.
        stream_id: Written as ``ushort``, not the on-wire ``uint``.
        frame_reserved: Written as ``uint``, not the on-wire ``ushort``.
        body: Serialized (and optionally compressed) RPC body.
    """
    ...
