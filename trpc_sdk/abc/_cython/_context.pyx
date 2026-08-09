# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

# cython: language_level=3, boundscheck=False, wraparound=False


cdef class RpczData:
    """Store RPC scheduling and I/O timestamps in microseconds."""

    def __cinit__(self):
        self.server_post_sched_recv_msg_us = 0
        self.server_pre_sched_send_msg_us = 0
        self.server_post_sched_send_msg_us = 0
        self.server_pre_io_send_msg_us = 0
        self.server_post_io_send_msg_us = 0
        self.client_pre_io_send_msg_us = 0
        self.client_post_io_send_msg_us = 0
        self.client_post_sched_send_msg_us = 0
        self.client_post_sched_recv_msg_us = 0


cdef class TrpcContext:
    """Own the runtime state and message for one RPC invocation.

    Ownership is intentionally one-way: the context holds the message, and the
    message has no reference back to the context. Public native fields provide
    direct access to statically typed Cython callers.
    """

    def __cinit__(self):
        self.deadline = 0
        self.timeout = 0
        self.msg = None
        self.meta_data = {}
        self.remote_ip = ""
        self.remote_port = 0
        self.local_ip = ""
        self.local_port = 0
        self.recv_time = 0.0
        self.rpcz_data = RpczData()

    cpdef void with_meta_data(self, object key, object value) except *:
        """Associate a value with a context-local metadata key."""
        self.meta_data[key] = value

    cpdef object get_meta_data(self, object key, object default=None):
        """Return a metadata value or the supplied default."""
        return self.meta_data.get(key, default)

    cpdef object pop_meta_data(self, object key, object default=None):
        """Remove and return a metadata value or the supplied default."""
        return self.meta_data.pop(key, default)

    def __copy__(self):
        """Return a shallow copy without copying the owned message."""
        cdef TrpcContext context = TrpcContext()
        cdef object key
        cdef object value

        context.deadline = self.deadline
        context.timeout = self.timeout
        context.recv_time = self.recv_time
        context.remote_ip = self.remote_ip
        context.remote_port = self.remote_port
        context.local_ip = self.local_ip
        context.local_port = self.local_port
        context.rpcz_data.server_post_sched_recv_msg_us = (
            self.rpcz_data.server_post_sched_recv_msg_us
        )
        context.rpcz_data.server_pre_sched_send_msg_us = (
            self.rpcz_data.server_pre_sched_send_msg_us
        )
        context.rpcz_data.server_post_sched_send_msg_us = (
            self.rpcz_data.server_post_sched_send_msg_us
        )
        context.rpcz_data.server_pre_io_send_msg_us = (
            self.rpcz_data.server_pre_io_send_msg_us
        )
        context.rpcz_data.server_post_io_send_msg_us = (
            self.rpcz_data.server_post_io_send_msg_us
        )
        context.rpcz_data.client_pre_io_send_msg_us = (
            self.rpcz_data.client_pre_io_send_msg_us
        )
        context.rpcz_data.client_post_io_send_msg_us = (
            self.rpcz_data.client_post_io_send_msg_us
        )
        context.rpcz_data.client_post_sched_send_msg_us = (
            self.rpcz_data.client_post_sched_send_msg_us
        )
        context.rpcz_data.client_post_sched_recv_msg_us = (
            self.rpcz_data.client_post_sched_recv_msg_us
        )
        for key, value in self.meta_data.items():
            context.meta_data[key] = value

        return context
