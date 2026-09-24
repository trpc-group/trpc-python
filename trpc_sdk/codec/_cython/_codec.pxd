# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

# cython: language_level=3, boundscheck=False, wraparound=False

# Unary tRPC encode / decode. Stream frames are _codec_stream.pxd.
# _ClientCodec.request_id is a per-instance wrap counter, not a
# globally unique id. encode / decode override _Codec, so their
# signatures must match it exactly.

from trpc_sdk.abc._cython cimport _Codec
from trpc_sdk.abc._cython cimport TrpcMessage


# Declared here rather than taken from the message module: byte-order
# conversion belongs to this pack path, and the message type has no reason
# to re-export C functions on its behalf.
cdef extern from "arpa/inet.h":
    unsigned short htons(unsigned short hostshort)
    unsigned int htonl(unsigned int hostlong)


cdef class _ClientCodec(_Codec):
    cdef:
        cdef public long request_id  # Per-instance encode counter; wraps at UINT32_MAX.

    cpdef object encode(self, TrpcMessage message, object body)
    cpdef object decode(self, TrpcMessage message, object buffer)
    cpdef bytes _unpack(self, object rsp, TrpcMessage message, bytes buffer)
    cpdef void update_msg_by_msg(self, TrpcMessage origin_message, TrpcMessage target_message)


cdef class _ServerCodec(_Codec):
    cpdef object encode(self, TrpcMessage message, object body)
    cpdef object decode(self, TrpcMessage message, object buffer)
    cpdef bytes _unpack(self, object req, TrpcMessage message, bytes buffer)
