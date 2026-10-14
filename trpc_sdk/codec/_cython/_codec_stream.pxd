# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

# cython: language_level=3, boundscheck=False, wraparound=False

# Stream-frame encode / decode. Unary frames are _codec.pxd.
# Encode INIT / CLOSE / FEEDBACK ignore the body argument; the payload
# is message.stream_frame. Decode "body" is the complete 16-byte-headed
# frame. _common_pack writes head_len = 0. encode_stream / decode_stream
# override _Codec and dispatch on frame_state to the per-frame methods.

from trpc_sdk.abc._cython cimport _Codec
from trpc_sdk.abc._cython cimport TrpcContext
from trpc_sdk.abc._cython cimport TrpcMessage


# Declared here rather than taken from the message module: byte-order
# conversion belongs to this pack path, and the message type has no reason
# to re-export C functions on its behalf.
cdef extern from "arpa/inet.h":
    unsigned short htons(unsigned short hostshort)
    unsigned int htonl(unsigned int hostlong)


cdef class _TrpcClientStreamCodec(_Codec):
    cpdef object encode_stream(self, TrpcContext context, object body)
    cpdef object decode_stream(self, TrpcContext context, object buffer)

    # Encode: INIT / CLOSE / FEEDBACK serialize stream_frame; DATA uses body.
    cpdef bytes encode_init_frame(self, TrpcMessage message, bytes body)
    cpdef bytes encode_data_frame(self, TrpcMessage message, bytes body)
    cpdef bytes encode_close_frame(self, TrpcMessage message, bytes body)
    cpdef bytes encode_feedback_frame(self, TrpcMessage message, bytes body)

    # Decode: argument is the complete frame; only DATA returns a body.
    cpdef bytes decode_init_frame(self, TrpcMessage message, bytes body)
    cpdef bytes decode_data_frame(self, TrpcMessage message, bytes body)
    cpdef bytes decode_close_frame(self, TrpcMessage message, bytes body)
    cpdef bytes decode_feedback_frame(self, TrpcMessage message, bytes body)


cdef class _TrpcServerStreamCodec(_Codec):
    cpdef object encode_stream(self, TrpcContext context, object body)
    cpdef object decode_stream(self, TrpcContext context, object buffer)

    # Encode: INIT / CLOSE / FEEDBACK serialize stream_frame; DATA uses body.
    cpdef bytes encode_init_frame(self, TrpcMessage message, bytes body)
    cpdef bytes encode_data_frame(self, TrpcMessage message, bytes body)
    cpdef bytes encode_close_frame(self, TrpcMessage message, bytes body)
    cpdef bytes encode_feedback_frame(self, TrpcMessage message, bytes body)

    # Decode: argument is the complete frame; only DATA returns a body.
    cpdef bytes decode_init_frame(self, TrpcMessage message, bytes body)
    cpdef bytes decode_data_frame(self, TrpcMessage message, bytes body)
    cpdef bytes decode_close_frame(self, TrpcMessage message, bytes body)
    cpdef bytes decode_feedback_frame(self, TrpcMessage message, bytes body)
