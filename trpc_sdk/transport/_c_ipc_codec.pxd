# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

# distutils: language = c++
# cython: language_level=3, boundscheck=False, wraparound=False
#
# C-level declarations for the production inner-IPC codec.
# DataHeader is the C dispatcher datagram layout from driver.h.
# Stream-IPC still uses the historical hand-packed +28 / +18 / +10
# frames; those offsets are not sizeof(DataHeader).

from libc.stdint cimport uint8_t
from libc.stdint cimport uint16_t
from libc.stdint cimport uint32_t


cdef extern from "../driver/driver.h":
    # Datagram-IPC header used on the C dispatcher pipe. Not the stream
    # path's hand-packed magic/len/fd layout.
    cdef cppclass DataHeader:
        uint16_t magic_num
        uint32_t len
        uint32_t fd
        uint32_t server_fd
        long session_id
        uint16_t reactor_id
        uint16_t worker_id
        double recv_time
        uint8_t port_id
        uint8_t pipe_id
        uint8_t type
        uint8_t flags
    cdef int GLOBAL_MAX_IPC_SIZE

ctypedef unsigned short ushort
ctypedef unsigned int uint
ctypedef unsigned long ulong


cdef class InnerStreamParser:
    cdef:
        # Growable stream buffer; a half-frame survives across feeds_data.
        char* buf
        int write_len
        int size

    cpdef void feeds_data(self, bytes data)
    cpdef list read(self)
    cpdef void reset(self)
    cdef void _resize(self, size_t new_number) except *


cdef class InnerDatagramParser:
    # No buf: one datagram is one frame, so there is nothing to reassemble.
    cdef:
        int write_len
        int size

    cpdef int feeds_data(self, bytes data)
    cpdef void reset(self)
