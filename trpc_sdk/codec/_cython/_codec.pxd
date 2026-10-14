# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

# cython: language_level=3, boundscheck=False, wraparound=False

# Unary tRPC encode / decode. Stream frames are _codec_stream.pxd.
# _TrpcClientCodec.request_id is a per-instance wrap counter, not a
# globally unique id. encode_data / decode_data override _Codec, so their
# signatures must match it exactly.

from trpc_sdk.abc._cython cimport _Codec
from trpc_sdk.abc._cython cimport TrpcContext
from trpc_sdk.abc._cython cimport TrpcMessage


# Declared here rather than taken from the message module: byte-order
# conversion belongs to this pack path, and the message type has no reason
# to re-export C functions on its behalf.
cdef extern from "arpa/inet.h":
    unsigned short htons(unsigned short hostshort)
    unsigned int htonl(unsigned int hostlong)


cdef class _TrpcClientCodec(_Codec):
    cdef:
        cdef public long request_id  # Per-instance encode counter; wraps at UINT32_MAX.

    cpdef object encode_data(self, TrpcContext context, object body)
    cpdef object decode_data(self, TrpcContext context, object buffer)
    cpdef bytes _unpack(self, object rsp, TrpcMessage message, bytes buffer)


cdef class _TrpcServerCodec(_Codec):
    cpdef object encode_data(self, TrpcContext context, object body)
    cpdef object decode_data(self, TrpcContext context, object buffer)
    cpdef bytes _unpack(self, object req, TrpcMessage message, bytes buffer)
