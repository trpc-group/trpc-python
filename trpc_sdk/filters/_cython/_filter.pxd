# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

# cython: language_level=3, boundscheck=False, wraparound=False


cdef class FilterChain:
    cdef:
        list filters  # Onion-ordered filter instances applied to each call.
        bint is_server  # Whether this chain runs on the server side.
