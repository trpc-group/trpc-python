# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

# cython: language_level=3, boundscheck=False, wraparound=False


cdef class BaseServerProtocolChecker:
    cdef:
        bytearray _buffer  # Mutable receive buffer retained across feed calls.

    cpdef void feed_data(self, bytes data) except *
    cpdef bytes read(self)
    cpdef void reset(self) except *
