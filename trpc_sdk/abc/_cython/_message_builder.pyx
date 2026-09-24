# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

# cython: language_level=3, boundscheck=False, wraparound=False

"""Frame-header parsing and the context / message clone helpers.

The server receive path calls :func:`create_message`, client stubs call
:func:`clone_client_message`, and stream frames call
:func:`clone_context_and_message`. Every clone helper returns a
``(context, message)`` pair; ownership is one-way, so callers that only
hold the message cannot walk back to the context.

Assigning message fields directly bypasses the derived-field setters; the
copy helpers below go through ``_set_*`` wherever a derived field exists.
"""

import copy

from libc.stdint cimport uint8_t
from libc.stdint cimport uint16_t
from libc.stdint cimport uint32_t
from libc.string cimport memcpy

from ._context cimport TrpcContext
from ._message cimport TrpcFrameHead
from ._message cimport TrpcMessage


cdef Py_ssize_t _FRAME_HEAD_SIZE = 16


cpdef TrpcFrameHead create_frame_head(
    uint8_t frame_type,
    uint8_t frame_state,
    uint32_t stream_id,
):
    """Create a frame header with the supplied routing fields.

    ``stx``, ``package_len``, and ``head_len`` stay 0; the pack paths fill
    them when the frame is serialized.

    Args:
        frame_type: ``TrpcDataFrameType`` value.
        frame_state: ``TrpcStreamFrameType`` value, or 0 on unary.
        stream_id: Stream identifier; 0 on unary.

    Returns:
        A zeroed header with those three fields set.
    """
    cdef TrpcFrameHead frame_head = TrpcFrameHead()
    frame_head.frame_type = frame_type
    frame_head.frame_state = frame_state
    frame_head.stream_id = stream_id
    return frame_head


cpdef TrpcFrameHead parse_frame_head(bytes data):
    """Parse a fixed-width tRPC frame header.

    Args:
        data: Buffer containing at least the 16-byte frame header.

    Returns:
        Parsed frame-header values in host byte order.

    Raises:
        ValueError: If the buffer is shorter than a complete frame header.
    """
    cdef const uint8_t* data_ptr
    cdef uint16_t value16
    cdef uint32_t value32
    cdef TrpcFrameHead frame_head

    if data is None or len(data) < _FRAME_HEAD_SIZE:
        raise ValueError("frame header requires at least 16 bytes")

    data_ptr = <const uint8_t*>data
    frame_head = TrpcFrameHead()

    memcpy(&value16, data_ptr, sizeof(uint16_t))
    frame_head.stx = ntohs(value16)
    frame_head.frame_type = data_ptr[2]
    frame_head.frame_state = data_ptr[3]

    memcpy(&value32, data_ptr + 4, sizeof(uint32_t))
    frame_head.package_len = ntohl(value32)
    memcpy(&value16, data_ptr + 8, sizeof(uint16_t))
    frame_head.head_len = ntohs(value16)
    memcpy(&value32, data_ptr + 10, sizeof(uint32_t))
    frame_head.stream_id = ntohl(value32)
    memcpy(&value16, data_ptr + 14, sizeof(uint16_t))
    frame_head.frame_reserved = ntohs(value16)

    return frame_head


cdef inline void _copy_frame_head(
    TrpcFrameHead source,
    TrpcFrameHead target,
) noexcept:
    """Copy native frame-header fields without sharing mutable state."""
    target.stx = source.stx
    target.frame_type = source.frame_type
    target.frame_state = source.frame_state
    target.package_len = source.package_len
    target.head_len = source.head_len
    target.stream_id = source.stream_id
    target.frame_reserved = source.frame_reserved


cdef inline void _copy_common_message(
    TrpcMessage source,
    TrpcMessage target,
) except *:
    """Copy fields shared by all message-cloning paths.

    Caller / callee identity, client metadata, and errors are left to the
    direction-specific helpers below.
    """
    if source.frame_head is None:
        target.frame_head = None
    else:
        _copy_frame_head(source.frame_head, target.frame_head)
    target.timeout = source.timeout
    target.serialization_type = source.serialization_type
    target._set_server_rpc_name(source.get_server_rpc_name())
    target._set_client_rpc_name(source.get_client_rpc_name())
    target.server_req_head = source.server_req_head
    target.server_rsp_head = source.server_rsp_head
    target.server_raw_req = source.server_raw_req
    target.server_raw_rsp = source.server_raw_rsp
    target.dyeing = source.dyeing
    target.dyeing_key = source.dyeing_key
    target.server_meta_data = source.server_meta_data
    target.logger = source.logger
    target.namespace = source.namespace
    target.env_name = source.env_name
    target.set_name = source.set_name
    target.env_transfer = source.env_transfer
    target.session_id = source.session_id
    target.request_attachment = source.request_attachment
    target.response_attachment = source.response_attachment


cdef inline void _copy_server_to_client_message(
    TrpcMessage source,
    TrpcMessage target,
) except *:
    """Map an inbound server message to a downstream client message."""
    if source.server_meta_data is None:
        target.client_meta_data = None
    else:
        target.client_meta_data = source.server_meta_data.copy()

    # The old callee becomes the caller of the downstream RPC.
    target._set_caller_service_name(source.get_callee_service_name())
    target.caller_app = source.callee_app
    target.caller_server = source.callee_server
    target.caller_service = source.callee_service
    target.caller_method = source.callee_method


cdef inline void _copy_message(
    TrpcMessage source,
    TrpcMessage target,
) except *:
    """Copy caller and callee routing state without flipping them.

    Successive frames of one stream keep the same identity on both sides.
    """
    target._set_caller_service_name(source.get_caller_service_name())
    target.caller_app = source.caller_app
    target.caller_server = source.caller_server
    target.caller_service = source.caller_service
    target.caller_method = source.caller_method

    target._set_callee_service_name(source.get_callee_service_name())
    target.callee_app = source.callee_app
    target.callee_server = source.callee_server
    target.callee_service = source.callee_service
    target.callee_method = source.callee_method


cpdef tuple create_message(TrpcContext context):
    """Attach a fresh empty message to ``context`` (server receive entry).

    Always overwrites ``context.msg``, unlike :func:`get_message`.

    Args:
        context: Server request context.

    Returns:
        ``(context, message)`` with the same context instance.
    """
    cdef TrpcMessage message = TrpcMessage()
    context.msg = message
    return context, message


cpdef TrpcMessage get_message(TrpcContext context):
    """Return ``context.msg``, creating the message on first access.

    Args:
        context: Context that should own the message.

    Returns:
        The existing or newly created message.
    """
    cdef TrpcMessage message

    if context.msg is None:
        message = TrpcMessage()
        context.msg = message
    return context.msg


cpdef tuple clone_context_and_message(TrpcContext context):
    """Clone context and message for another frame of the same stream.

    Caller and callee identity are preserved, so the new message describes
    the same RPC rather than a downstream one.

    Args:
        context: Source context.

    Returns:
        ``(new_context, new_message)``; the context is a shallow copy.
    """
    cdef TrpcContext new_context = copy.copy(context)
    cdef TrpcMessage new_message = TrpcMessage()
    cdef TrpcMessage source = context.msg

    new_context.msg = new_message
    if source is None:
        return new_context, new_message

    _copy_common_message(source, new_message)
    _copy_message(source, new_message)
    return new_context, new_message


cpdef tuple clone_message(TrpcContext context):
    """Clone context and message for an outbound downstream call.

    The old callee becomes the new caller, so the clone describes the RPC
    this process is about to issue rather than the one it is serving.

    Args:
        context: Source context.

    Returns:
        ``(new_context, new_message)``; the context is a shallow copy.
    """
    cdef TrpcContext new_context = copy.copy(context)
    cdef TrpcMessage new_message = TrpcMessage()
    cdef TrpcMessage source = context.msg

    new_context.msg = new_message
    if source is None:
        return new_context, new_message

    _copy_common_message(source, new_message)
    _copy_server_to_client_message(source, new_message)
    return new_context, new_message


cpdef tuple clone_client_message(TrpcContext context):
    """Clone for a client stub; the name generated stubs call.

    Args:
        context: Source context.

    Returns:
        The same pair as :func:`clone_message`.
    """
    return clone_message(context)
