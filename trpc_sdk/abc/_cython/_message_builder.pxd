# Tencent is pleased to support the open source community by making tRPC-Agent-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Agent-Python is licensed under Apache-2.0.

# cython: language_level=3, boundscheck=False, wraparound=False

from libc.stdint cimport uint8_t
from libc.stdint cimport uint16_t
from libc.stdint cimport uint32_t

from ._context cimport TrpcContext
from ._message cimport TrpcFrameHead
from ._message cimport TrpcMessage


cdef extern from "arpa/inet.h":
    uint16_t ntohs(uint16_t netshort) noexcept nogil
    uint32_t ntohl(uint32_t netlong) noexcept nogil


cpdef TrpcFrameHead new_frame_head(
    uint8_t frame_type,
    uint8_t frame_state,
    uint32_t stream_id,
)
cpdef TrpcFrameHead parse_frame_head(bytes data)

cpdef TrpcContext create_new_context(TrpcContext context)
cpdef TrpcContext create_client_context_from_server(TrpcContext context)
cpdef TrpcMessage get_or_create_message(TrpcContext context)
cpdef TrpcMessage create_new_message_for_context(TrpcContext context)
