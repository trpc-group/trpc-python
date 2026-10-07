# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

# cython: language_level=3, boundscheck=False, wraparound=False

from ._message cimport TrpcMessage


# Private C-level codec root. Framework Cython codecs inherit it through
# trpc_sdk.abc._cython cimport; BaseCodec layers the plugin contract on top.
# Parameters and returns are ``object``: stream INIT / CLOSE / FEEDBACK pass
# body=None, a Python ``decode`` override may return a coroutine, and
# overriding cpdef signatures must match exactly.
cdef class _Codec:
    cpdef object encode(self, TrpcMessage message, object body)
    cpdef object decode(self, TrpcMessage message, object buffer)
    cpdef object encode_stream(self, TrpcMessage message, object body)
    cpdef object decode_stream(self, TrpcMessage message, object buffer)


cdef class BaseServerProtocolChecker:
    cdef:
        bytearray _buffer  # Mutable receive buffer retained across feed calls.

    cpdef void feed_data(self, bytes data) except *
    cpdef list read(self)
    cpdef void reset(self) except *
