# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

# distutils: language = c++
# cython: language_level=3, boundscheck=False, wraparound=False

# Length-only tRPC frame splitter. Checks magic + package_len; does
# not parse RequestProtocol.

from libc.stdint cimport uint16_t
from libc.stdint cimport uint32_t

from trpc_sdk.abc._cython cimport BaseServerProtocolChecker

cdef extern from "arpa/inet.h":
    uint16_t ntohs(uint16_t netshort)
    uint32_t ntohl(uint32_t netlong)


cdef class TrpcServerProtocolChecker(BaseServerProtocolChecker):
    cdef:
        cdef char* buf      # Growable stream buffer; a half-frame survives across feed_data.
        cdef int length     # Bytes currently valid in buf.
        cdef int size       # Allocated capacity; grow uses (size + append) * 2.

    cpdef void feed_data(self, bytes data) except *
    cpdef list read(self)
    cpdef void resize(self, size_t new_number)
    cpdef void reset(self) except *
