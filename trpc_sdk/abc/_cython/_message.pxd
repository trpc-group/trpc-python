# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

# cython: language_level=3, boundscheck=False, wraparound=False

from libc.stdint cimport uint8_t
from libc.stdint cimport uint16_t
from libc.stdint cimport uint32_t

cimport cython


cdef class TrpcFrameHead:
    cdef:
        public uint16_t stx             # Protocol magic number identifying a tRPC frame.
        public uint8_t frame_type       # Payload type encoded by this frame.
        public uint8_t frame_state      # Streaming lifecycle state represented by the frame.
        public uint32_t package_len     # Total frame size in bytes, including the header.
        public uint16_t head_len        # Serialized header size in bytes.
        public uint32_t stream_id       # Identifier used to correlate streaming frames.
        public uint16_t frame_reserved  # Reserved protocol bits for future extensions.

    # Setters mirror the public fields one for one. They exist so Python
    # callers and statically typed Cython callers share a single spelling;
    # every one of them stores the host-endian value unchanged.
    cpdef void set_stx(self, uint16_t stx) except *
    cpdef void set_frame_type(self, uint8_t frame_type) except *
    cpdef void set_frame_state(self, uint8_t frame_state) except *
    cpdef void set_package_len(self, uint32_t package_len) except *
    cpdef void set_head_len(self, uint16_t head_len) except *
    cpdef void set_stream_id(self, uint32_t stream_id) except *
    cpdef void set_frame_reserved(self, uint16_t frame_reserved) except *


cdef class TrpcMessage:
    cdef:
        # Frequently accessed independent values use public C fields. Statically
        # typed Cython callers read and write them without a function call.
        public TrpcFrameHead frame_head
        public int timeout                  # Request timeout in milliseconds.
        public int serialization_type       # Protocol-specific serialization identifier.
        public int compress_type            # Protocol-specific compression identifier.
        public uint32_t stream_id            # Stream identifier shared by related frames.
        public bint dyeing                   # Whether traffic dyeing is enabled.
        public str dyeing_key                # Routing key used for dyed traffic.
        public object server_meta_data       # Metadata owned by the server side.
        public object client_meta_data       # Metadata owned by the client side.
        public object server_rsp_err         # Error produced while serving a request.
        public object client_rsp_err         # Error returned by a downstream call.
        public object server_req_head        # Protocol-specific inbound request header.
        public object server_rsp_head        # Protocol-specific outbound response header.
        public object client_req_head        # Protocol-specific downstream request header.
        public object client_rsp_head        # Protocol-specific downstream response header.
        public object server_raw_req         # Protocol-native inbound request object (e.g. aiohttp Request).
        public object server_raw_rsp         # Protocol-native outbound response object (e.g. aiohttp Response).
        public str caller_app
        public str caller_server
        public str caller_service
        public str callee_app
        public str callee_server
        public str callee_service
        public str callee_method
        public str local_ip                  # Local IP of the RPC, without port.
        public str remote_addr               # Peer network endpoint for the RPC.
        public str logger                    # Logger category or identifier.
        public str caller_method             # Method invoked by the upstream caller.
        public str namespace                 # Deployment namespace.
        public str env_name                  # Deployment environment name.
        public str set_name                  # Logical deployment set name.
        public str env_transfer              # Environment-transfer routing value.
        public str callee_container_name     # Target container name.
        public str callee_set_name           # Target deployment set name.
        public object stream_frame           # Protocol-specific streaming frame payload.
        public uint32_t request_id            # Request correlation identifier.
        public int session_id                # Stateful transport session identifier.
        public bytes request_attachment      # Opaque request attachment.
        public bytes response_attachment     # Opaque response attachment.

        # These source names require semantic setters because changing them also
        # updates parsed or derived fields. Their backing storage remains private.
        str _server_rpc_name
        str _client_rpc_name
        str _caller_service_name
        str _callee_service_name

    # Cython-only accessors preserve encapsulation for the four semantic names.
    @cython.final
    cdef str get_server_rpc_name(self)
    @cython.final
    cdef str get_client_rpc_name(self)
    @cython.final
    cdef str get_caller_service_name(self)
    @cython.final
    cdef str get_callee_service_name(self)

    # These setters synchronize all fields derived from the supplied name.
    @cython.final
    cdef void _set_server_rpc_name(self, str server_rpc_name) except *
    @cython.final
    cdef void _set_client_rpc_name(self, str client_rpc_name) except *
    @cython.final
    cdef void _set_caller_service_name(self, str caller_service_name) except *
    @cython.final
    cdef void _set_callee_service_name(self, str callee_service_name) except *

    # Python-visible setters. Plain fields are assigned directly; the four
    # semantic names forward to the cdef helpers above so derived fields stay
    # in sync no matter which spelling the caller uses.
    cpdef void set_frame_head(self, TrpcFrameHead frame_head) except *
    cpdef void set_timeout(self, int timeout) except *
    cpdef void set_serialization_type(self, int serialization_type) except *
    cpdef void set_compress_type(self, int compress_type) except *
    cpdef void set_stream_id(self, uint32_t stream_id) except *
    cpdef void set_dyeing(self, bint dyeing) except *
    cpdef void set_dyeing_key(self, str dyeing_key) except *
    cpdef void set_server_rpc_name(self, str server_rpc_name) except *
    cpdef void set_client_rpc_name(self, str client_rpc_name) except *
    cpdef void set_server_meta_data(self, object server_meta_data) except *
    cpdef void set_client_meta_data(self, object client_meta_data) except *
    cpdef void set_caller_service_name(self, str caller_service_name) except *
    cpdef void set_callee_service_name(self, str callee_service_name) except *
    cpdef void set_server_rsp_err(self, object server_rsp_err) except *
    cpdef void set_client_rsp_err(self, object client_rsp_err) except *
    cpdef void set_server_req_head(self, object server_req_head) except *
    cpdef void set_server_rsp_head(self, object server_rsp_head) except *
    cpdef void set_client_req_head(self, object client_req_head) except *
    cpdef void set_client_rsp_head(self, object client_rsp_head) except *
    cpdef void set_server_raw_req(self, object server_raw_req) except *
    cpdef void set_server_raw_rsp(self, object server_raw_rsp) except *
    cpdef void set_local_ip(self, str local_ip) except *
    cpdef void set_remote_addr(self, str remote_addr) except *
    cpdef void set_logger(self, str logger) except *
    cpdef void set_caller_app(self, str caller_app) except *
    cpdef void set_caller_server(self, str caller_server) except *
    cpdef void set_caller_service(self, str caller_service) except *
    cpdef void set_caller_method(self, str caller_method) except *
    cpdef void set_callee_app(self, str callee_app) except *
    cpdef void set_callee_server(self, str callee_server) except *
    cpdef void set_callee_service(self, str callee_service) except *
    cpdef void set_callee_method(self, str callee_method) except *
    cpdef void set_namespace(self, str namespace) except *
    cpdef void set_env_name(self, str env_name) except *
    cpdef void set_setname(self, str set_name) except *
    cpdef void set_env_transfer(self, str env_transfer) except *
    cpdef void set_callee_container_name(self, str callee_container_name) except *
    cpdef void set_callee_set_name(self, str callee_set_name) except *
    cpdef void set_stream_frame(self, object stream_frame) except *
    cpdef void set_request_id(self, uint32_t request_id) except *
    cpdef void set_session_id(self, int session_id) except *
    cpdef void set_request_attachment(self, bytes attachment) except *
    cpdef void set_response_attachment(self, bytes attachment) except *
