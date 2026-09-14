# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

# cython: language_level=3, boundscheck=False, wraparound=False

# 16-byte tRPC frame head plus the per-RPC Message used by codec
# encode/decode. Message holds a TrpcContext back-reference; TrpcMessage
# does not. Multi-byte CFrameHead fields are host-endian here.


ctypedef unsigned int uint
ctypedef unsigned short ushort
ctypedef unsigned char uchar


cdef unsigned short endian_byte_swapper_ushort(unsigned short src)
cdef unsigned int endian_byte_swapper_uint(unsigned int src)


cdef extern from "arpa/inet.h":
    unsigned short ntohs(unsigned short netshort)
    unsigned int ntohl(unsigned int netlong)
    unsigned short htons(unsigned short hostshort)
    unsigned int htonl(unsigned int hostlong)


cdef class CFrameHead:
    cdef:
        cdef public ushort stx             # Host-endian magic; encode writes TRPC_MAGIC_VALUE.
        cdef public uchar frame_type      # Unary vs stream (TrpcDataFrameType).
        cdef public uchar frame_state     # Stream sub-type; 0 on unary.
        cdef public uint package_len     # Total frame size, including this 16-byte head.
        cdef public ushort head_len        # Protocol-header size after the frame head.
        cdef public uint stream_id       # Stream identifier; 0 on unary.
        cdef public ushort frame_reserved  # Unused reserved field.

    cpdef void set_stx(self, ushort stx)
    cpdef void set_frame_type(self, uchar frame_type)
    cpdef void set_frame_state(self, uchar frame_state)
    cpdef void set_package_len(self, uint package_len)
    cpdef void set_head_len(self, ushort head_len)
    cpdef void set_stream_id(self, uint stream_id)
    cpdef void set_frame_reserved(self, ushort frame_reserved)


cpdef CFrameHead create_frame_head(uchar frame_type, uchar frame_state, uint stream_id)
cpdef CFrameHead parse_frame_head(bytes data)


cdef class Message:
    cdef:
        cdef public object context              # Back-ref to TrpcContext; TrpcContext.msg points here.
        cdef public CFrameHead frame_head       # 16-byte tRPC head for this RPC / stream frame.
        cdef public int timeout                 # Request timeout in milliseconds.
        cdef public int serialization_type      # Body serializer id (SerializationType).
        cdef public int compress_type           # Body compressor id (CompressType).
        cdef public uint stream_id              # Stream id; 0 on unary.
        cdef public bint dyeing                 # Whether traffic dyeing is enabled.
        cdef public str dyeing_key              # Routing key used for dyed traffic.
        cdef public str server_rpc_name         # Inbound RPC name; set_* also syncs callee_method.
        cdef public str client_rpc_name         # Outbound RPC name; set_* also syncs callee_method.
        cdef public object server_meta_data     # Inbound / server-owned transmissive metadata.
        cdef public object client_meta_data     # Outbound / client-owned transmissive metadata.
        cdef public str caller_service_name     # Fully qualified caller; set_* parses app/server/service.
        cdef public str callee_service_name     # Fully qualified callee; set_* parses app/server/service.
        cdef public object server_rsp_err       # Error encoded into the server response.
        cdef public object client_rsp_err       # Error returned by a downstream call.
        cdef public object server_req_head      # Protocol-specific inbound request header.
        cdef public object server_rsp_head      # Protocol-specific outbound response header.
        cdef public object client_req_head      # Protocol-specific downstream request header.
        cdef public object client_rsp_head      # Protocol-specific downstream response header.
        cdef public str local_addr              # Local network endpoint for this RPC.
        cdef public str remote_addr             # Peer network endpoint for this RPC.
        cdef public str logger                  # Logger category or identifier.
        cdef public str caller_app              # Parsed from caller_service_name unless "*".
        cdef public str caller_server
        cdef public str caller_service
        cdef public str caller_method
        cdef public str callee_app              # Parsed from callee_service_name unless "*".
        cdef public str callee_server
        cdef public str callee_service
        cdef public str callee_method           # Also synced from set_server/client_rpc_name.
        cdef public str namespace               # Deployment namespace.
        cdef public str env_name                # Deployment environment name.
        cdef public str set_name                # Logical deployment set name (set via set_setname).
        cdef public str env_transfer            # Environment-transfer routing value.
        cdef public str callee_container_name   # Target container name.
        cdef public str callee_set_name         # Target deployment set name.
        cdef public object stream_frame         # Protocol-specific streaming frame payload.
        cdef public uint request_id             # RPC request correlation id.
        cdef public int session_id              # Transport session id, not request_id.
        cdef public bytes request_attachment    # Opaque request attachment.
        cdef public bytes response_attachment   # Opaque response attachment.

    cpdef void set_frame_head(self, CFrameHead frame_head)
    cpdef void set_timeout(self, int timeout)
    cpdef void set_serialization_type(self, int serialization_type)
    cpdef void set_compress_type(self, int compress_type)
    cpdef void set_stream_id(self, uint stream_id)
    cpdef void set_dyeing(self, bint dyeing)
    cpdef void set_dyeing_key(self, str dyeing_key)
    cpdef void set_server_rpc_name(self, str server_rpc_name)
    cpdef void set_client_rpc_name(self, str client_rpc_name)
    cpdef void set_server_meta_data(self, object server_meta_data)
    cpdef void set_client_meta_data(self, object client_meta_data)
    cpdef void set_caller_service_name(self, str caller_service_name)
    cpdef void set_callee_service_name(self, str callee_service_name)
    cpdef void set_server_rsp_err(self, object server_rsp_err)
    cpdef void set_client_rsp_err(self, object client_rsp_err)
    cpdef void set_server_req_head(self, object server_req_head)
    cpdef void set_server_rsp_head(self, object server_rsp_head)
    cpdef void set_client_req_head(self, object client_req_head)
    cpdef void set_client_rsp_head(self, object client_rsp_head)
    cpdef void set_local_addr(self, str local_addr)
    cpdef void set_remote_addr(self, str remote_addr)
    cpdef void set_logger(self, str logger)
    cpdef void set_caller_app(self, str caller_app)
    cpdef void set_caller_server(self, str caller_server)
    cpdef void set_caller_service(self, str caller_service)
    cpdef void set_caller_method(self, str caller_method)
    cpdef void set_callee_app(self, str callee_app)
    cpdef void set_callee_server(self, str callee_server)
    cpdef void set_callee_service(self, str callee_service)
    cpdef void set_callee_method(self, str callee_method)
    cpdef void set_namespace(self, str namespace)
    cpdef void set_env_name(self, str env_name)
    cpdef void set_setname(self, str set_name)
    cpdef void set_env_transfer(self, str env_transfer)
    cpdef void set_callee_container_name(self, str callee_container_name)
    cpdef void set_callee_set_name(self, str callee_set_name)
    cpdef void set_stream_frame(self, object stream_frame)
    cpdef void set_request_id(self, uint request_id)
    cpdef void set_session_id(self, int session_id)
    cpdef void set_request_attachment(self, bytes attachment)
    cpdef void set_response_attachment(self, bytes attachment)
