# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

# cython: language_level=3, boundscheck=False, wraparound=False

"""Legacy hand-packed unary frame builder.

Still compiled from ``setup.py``. Nothing in the tree imports it:
``_codec.pyx`` / ``_codec_stream.pyx`` comment out
``from ._codec_pack import struct_pack``. New code uses
:func:`trpc_sdk.codec._cython._codec._common_pack`.

``struct_pack`` field widths do not match the real 16-byte head:
``stream_id`` is written as ``ushort`` (2 bytes) and
``frame_reserved`` as ``uint`` (4 bytes). The on-wire layout is
``stream_id`` 4 bytes then ``frame_reserved`` 2 bytes. That is why
this helper cannot be reused as a drop-in packer.
"""

from libc.stdint cimport uint16_t
from libc.stdint cimport uint32_t
from libc.stdint cimport uint8_t

cpdef bytes endian_swap_uint2bytes(uint32_t i):
    """Return a 4-byte big-endian encoding of ``i``.

    Args:
        i: Host-endian 32-bit value.

    Returns:
        Four bytes, most significant first.
    """
    i = (i >> 24) | ((i >> 8) & 0x0000FF00) | ((i << 8) & 0x00FF0000) | (i << 24)
    return (<char *>&i)[:sizeof(uint32_t)]

cpdef bytes endian_swap_ushort2bytes(uint16_t i):
    """Return a 2-byte big-endian encoding of ``i``.

    Args:
        i: Host-endian 16-bit value.

    Returns:
        Two bytes, most significant first.
    """
    i = (i >> 8) | (i << 8)
    return (<char *>&i)[:sizeof(uint16_t)]

cpdef bytes endian_swap_uchar2bytes(uint8_t i):
    """Return a 1-byte encoding of ``i``.

    Args:
        i: 8-bit value; no endian swap is needed.

    Returns:
        A single byte.
    """
    return (<char *>&i)[:sizeof(uint8_t)]

cpdef bytes struct_pack(bytes req_head, uint8_t frame_head_len, uint16_t magic_value,
                       int frame_type, int frame_state, int stream_id,
                       int frame_reserved, bytes body):
    """Join a frame head, protocol head, and body with ``b''.join``.

    Writes ``stream_id`` as 2 bytes and ``frame_reserved`` as 4 bytes.
    The real tRPC head is the opposite (stream id 4, reserved 2), so
    the result is not a valid on-wire frame.

    Args:
        req_head: Serialized protocol head (``RequestProtocol``).
        frame_head_len: Claimed frame-head size, usually 16.
        magic_value: ``TRPC_MAGIC_VALUE``.
        frame_type: Unary vs stream.
        frame_state: Stream sub-type; 0 on unary.
        stream_id: Written as ``ushort``, not the on-wire ``uint``.
        frame_reserved: Written as ``uint``, not the on-wire ``ushort``.
        body: Serialized (and optionally compressed) RPC body.

    Returns:
        Concatenated bytes; not a correct 16-byte tRPC frame.
    """
    cdef int head_len = len(req_head)
    cdef int body_len = len(body)
    cdef int total_len = frame_head_len + head_len + body_len
    return b''.join(
        (endian_swap_ushort2bytes(magic_value),
         endian_swap_uchar2bytes(frame_type),
         endian_swap_uchar2bytes(frame_state),
         endian_swap_uint2bytes(total_len),
         endian_swap_ushort2bytes(head_len),
         endian_swap_ushort2bytes(stream_id),
         endian_swap_uint2bytes(frame_reserved),
         req_head,
         body)
    )
