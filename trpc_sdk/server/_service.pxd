# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

# cython: language_level=3, boundscheck=False, wraparound=False

# Cython declarations for Service (implemented in _service.pyx).
# Other Cython modules ``cimport`` this type. Python callers import the compiled
# ``trpc_sdk.server._service`` module (re-exported from ``trpc_sdk.server``).
# Decode/encode, timeout merge, queue-wait, and the filter chain live in the
# .pyx; this header is the C-level layout and API.

from trpc_sdk.abc._cython cimport TrpcContext
from trpc_sdk.codec._message cimport Message
from trpc_sdk.filters.filter cimport FilterChain

from ._option cimport ServiceOption


cdef class Service:
    # Per-service handler path after the transport has a complete request frame.
    # One instance per YAML ``server.service[]`` entry.
    cdef:
        cdef public ServiceOption opt       # Per-endpoint options; filled by ``trpc.new``.
        cdef public dict unary_handlers     # RPC name -> ``Method``; empty until ``register``.
        cdef public dict stream_handlers    # RPC name -> ``StreamDesc``; empty until ``register``.
        cdef public object queue_window     # ``QueueWindow``; built even for DEFAULT policy.
        cdef public object registry_thread  # ``RegistryThread``, or None until ``start_registry``.

    # Timeout / queue-wait budget (unary hot path).
    cdef float _merge_timeout_budget(self, TrpcContext ctx, Message msg)
    cdef void _fill_msg_from_option(self, Message msg, TrpcContext ctx)
    cdef (float, long, long) _prepare_request_budget(self, Message msg, TrpcContext ctx)
    cdef double _current_time_ms(self)
    cdef bint _check_queue_wait(self, int limit_ms, int waited_ms, double now_ms_ts)

    # Response encoding: errors stay in the frame, not raised to transport.
    cdef bytes _encode_queue_timeout_rsp(self, Message msg)
    cdef bytes _encode_func_not_found_rsp(self, TrpcContext ctx)
    cdef bytes _encode_error_rsp(self, object msg, object error)

    # Filter chain: decompress + deserialize in place, then return ``opt.filters``.
    cdef FilterChain _prepare_filter_chain(self, Message msg, bytes req_body_buf, object req_object)

    # Public lifecycle.
    cpdef void register(self, object service_desc, bint fix_rpc_name)
    cpdef void run(self)
    cpdef void start_registry(self)
    cpdef object close(self)
