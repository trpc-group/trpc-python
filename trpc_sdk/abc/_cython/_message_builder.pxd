# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

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


cpdef TrpcFrameHead create_frame_head(
    uint8_t frame_type,
    uint8_t frame_state,
    uint32_t stream_id,
)
cpdef TrpcFrameHead parse_frame_head(bytes data)

# Every clone returns a (context, message) tuple; get_message returns the
# message alone, attaching one to the context on first access.
cpdef tuple create_message(TrpcContext context)
cpdef TrpcMessage get_message(TrpcContext context)
cpdef tuple clone_context_and_message(TrpcContext context)
cpdef tuple clone_message(TrpcContext context)
cpdef tuple clone_client_message(TrpcContext context)
