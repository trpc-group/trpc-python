# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

# cython: language_level=3, boundscheck=False, wraparound=False

from libc.stdint cimport uint16_t

from ._message cimport TrpcMessage


cdef class RpczData:
    cdef:
        public long server_post_sched_recv_msg_us
        public long server_pre_sched_send_msg_us
        public long server_post_sched_send_msg_us
        public long server_pre_io_send_msg_us
        public long server_post_io_send_msg_us
        public long client_pre_io_send_msg_us
        public long client_post_io_send_msg_us
        public long client_post_sched_send_msg_us
        public long client_post_sched_recv_msg_us

cdef class TrpcContext:
    cdef:
        public long deadline
        public int timeout
        public TrpcMessage msg
        public object meta_data
        public str remote_ip
        public uint16_t remote_port
        public str local_ip
        public uint16_t local_port
        public double recv_time
        public RpczData rpcz_data

    cpdef void with_meta_data(self, object key, object value) except *
    cpdef object get_meta_data(self, object key, object default=*)
    cpdef object pop_meta_data(self, object key, object default=*)
    cpdef object get_client_message(self)
    cpdef long get_timeout_ms(self)
    cpdef long get_timeout_us(self)
    cpdef double get_timeout_sec(self)
