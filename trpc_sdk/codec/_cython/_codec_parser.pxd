# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

# distutils: language = c++
# cython: language_level=3, boundscheck=False, wraparound=False

# Length-only tRPC frame splitter. Checks magic + package_len; does
# not parse RequestProtocol. mem_cpy is the leftover byte-by-byte
# compact replaced by memmove in read.

cdef extern from "arpa/inet.h":
    unsigned short ntohs(unsigned short netshort)
    unsigned int ntohl(unsigned int netlong)
    unsigned short htons(unsigned short hostshort)
    unsigned int htonl(unsigned int hostlong)


cdef class CServerParser:
    cdef:
        cdef char* buf      # Growable stream buffer; a half-frame survives across feeds_data.
        cdef int length     # Bytes currently valid in buf.
        cdef int size       # Allocated capacity; grow uses (size + append) * 2.

    cpdef void feeds_data(self, bytes data)
    cpdef list read(self)
    cdef void mem_cpy(self, int start_index, int length)
    cpdef void resize(self, size_t new_number)
    cpdef void reset(self)
