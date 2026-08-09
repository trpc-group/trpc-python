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
        public str caller_app
        public str caller_server
        public str caller_service
        public str callee_app
        public str callee_server
        public str callee_service
        public str callee_method
        public str local_addr                # Local network endpoint for the RPC.
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
