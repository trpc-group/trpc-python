# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

# distutils: language = c++
# cython: language_level=3, boundscheck=False, wraparound=False

ctypedef unsigned short ushort
ctypedef unsigned int uint
ctypedef unsigned long ulong

# Fixed head sizes of the two Python-protocol frames. Declared as an anonymous
# enum rather than module-level cdef variables so that _task_queue in this
# package, which cimports them, gets compile-time constants instead of a
# cross-module lookup.
cdef enum:
    TASK_REQ_HEAD_LEN = 26
    TASK_RSP_HEAD_LEN = 14

# Fixed 26-byte head of a Python-protocol request frame, in host byte order.
# body is not part of the head on the wire: it aliases the caller's buffer on
# the encode path and is left unset on the decode path.
ctypedef struct TaskRequestHead:
    ushort magic
    uint package_len
    uint pkg_id
    ulong recv_time
    ulong timeout
    char* body

# Fixed 14-byte head of a Python-protocol response frame. ret_code replaces the
# timestamps the request carries; body follows the same aliasing rule.
ctypedef struct TaskResponseHead:
    ushort magic
    uint package_len
    uint pkg_id
    uint ret_code
    char* body

cdef char* inner_encode_task_req_c(TaskRequestHead* req) nogil
cpdef bytes inner_encode_task_req(uint pkg_id, ulong recv_time, ulong timeout, object data)
cdef TaskRequestHead* inner_decode_task_req_c(char* from_prt) nogil
cpdef object inner_decode_task_req(bytes data)
cdef char* inner_encode_task_rsp_c(TaskResponseHead* rsp) nogil
cpdef bytes inner_encode_task_rsp(uint pkg_id, uint ret_code, object data)
cdef TaskResponseHead* inner_decode_task_rsp_c(char* from_prt) nogil
cpdef object inner_decode_task_rsp(bytes data)
