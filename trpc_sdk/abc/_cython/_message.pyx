# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

# cython: language_level=3, boundscheck=False, wraparound=False

cimport cython

from trpc_sdk.exceptions import TrpcErrorCode
from trpc_sdk.exceptions import create_frame_error

from ._utils cimport get_app_server_service
from ._utils cimport get_method_from_rpc_func


cdef class TrpcFrameHead:
    """Store the fixed-width fields of a tRPC transport frame header.

    Independent native fields remain public so statically typed Cython callers
    can read and write them without method or Python dispatch overhead.

    Every field holds a host-endian integer. Byte-order conversion belongs to
    the parse and pack paths, so the setters below never swap bytes.
    """

    def __cinit__(self):
        self.stx = 0
        self.frame_type = 0
        self.frame_state = 0
        self.package_len = 0
        self.head_len = 0
        self.stream_id = 0
        self.frame_reserved = 0

    cpdef void set_stx(self, uint16_t stx) except *:
        """Set the protocol magic number.

        Args:
            stx: Host-endian magic; a decode mismatch invalidates the frame.
        """
        self.stx = stx

    cpdef void set_frame_type(self, uint8_t frame_type) except *:
        """Set the payload type carried by this frame.

        Args:
            frame_type: ``TrpcDataFrameType`` value distinguishing unary from
                streaming frames.
        """
        self.frame_type = frame_type

    cpdef void set_frame_state(self, uint8_t frame_state) except *:
        """Set the streaming lifecycle state.

        Args:
            frame_state: ``TrpcStreamFrameType`` value; stays 0 on unary frames.
        """
        self.frame_state = frame_state

    cpdef void set_package_len(self, uint32_t package_len) except *:
        """Set the total frame size.

        Args:
            package_len: Byte count including the 16-byte frame header itself.
        """
        self.package_len = package_len

    cpdef void set_head_len(self, uint16_t head_len) except *:
        """Set the serialized protocol-header size.

        Args:
            head_len: Byte count of the protocol header that follows the frame
                header, excluding the frame header.
        """
        self.head_len = head_len

    cpdef void set_stream_id(self, uint32_t stream_id) except *:
        """Set the identifier correlating frames of one stream.

        Args:
            stream_id: Stream identifier; stays 0 on unary frames.
        """
        self.stream_id = stream_id

    cpdef void set_frame_reserved(self, uint16_t frame_reserved) except *:
        """Set the reserved protocol bits.

        Args:
            frame_reserved: Reserved value, normally 0.
        """
        self.frame_reserved = frame_reserved


cdef class TrpcMessage:
    """Store protocol-independent state associated with an RPC message.

    The message contains transport and protocol data only. Runtime ownership
    belongs to ``TrpcContext``. Independent fields are public for direct Cython
    access, while names with derived components use final C-level
    synchronization helpers.
    """

    def __cinit__(self):
        self.frame_head = TrpcFrameHead()
        self.timeout = 0
        self.serialization_type = 0
        self.compress_type = 0
        self.stream_id = 0
        self.dyeing = False
        self.dyeing_key = ""
        self._server_rpc_name = ""
        self._client_rpc_name = ""
        self.server_meta_data = {}
        self.client_meta_data = {}
        self._caller_service_name = ""
        self._callee_service_name = ""
        # Success placeholder, not None: a handler that never touches the field
        # must still encode a valid status, and readers dereference ``.code``
        # unconditionally.
        self.server_rsp_err = create_frame_error(TrpcErrorCode.TRPC_INVOKE_SUCCESS, "")
        self.client_rsp_err = None
        self.server_req_head = None
        self.server_rsp_head = None
        self.client_req_head = None
        self.client_rsp_head = None
        self.server_raw_req = None
        self.server_raw_rsp = None
        self.caller_app = ""
        self.caller_server = ""
        self.caller_service = ""
        self.caller_method = ""
        self.callee_app = ""
        self.callee_server = ""
        self.callee_service = ""
        self.callee_method = ""
        self.local_ip = ""
        self.remote_addr = ""
        self.logger = ""
        self.namespace = ""
        self.env_name = ""
        self.set_name = ""
        self.env_transfer = ""
        self.callee_container_name = ""
        self.callee_set_name = ""
        self.stream_frame = None
        self.request_id = 0
        self.session_id = 0
        self.request_attachment = b""
        self.response_attachment = b""

    @cython.final
    cdef str get_server_rpc_name(self):
        """Return the RPC name observed by the server."""
        return self._server_rpc_name

    @cython.final
    cdef str get_client_rpc_name(self):
        """Return the RPC name selected by the client."""
        return self._client_rpc_name

    @cython.final
    cdef str get_caller_service_name(self):
        """Return the fully qualified upstream service name."""
        return self._caller_service_name

    @cython.final
    cdef str get_callee_service_name(self):
        """Return the fully qualified downstream service name."""
        return self._callee_service_name

    property server_rpc_name:
        def __get__(self):
            return self.get_server_rpc_name()

        def __set__(self, value):
            self._set_server_rpc_name(value)

    property client_rpc_name:
        def __get__(self):
            return self.get_client_rpc_name()

        def __set__(self, value):
            self._set_client_rpc_name(value)

    property caller_service_name:
        def __get__(self):
            return self.get_caller_service_name()

        def __set__(self, value):
            self._set_caller_service_name(value)

    property callee_service_name:
        def __get__(self):
            return self.get_callee_service_name()

        def __set__(self, value):
            self._set_callee_service_name(value)

    @cython.final
    cdef void _set_server_rpc_name(self, str server_rpc_name) except *:
        """Set the server RPC name and synchronize the callee method."""
        if self._server_rpc_name == server_rpc_name:
            return
        self._server_rpc_name = server_rpc_name
        self.callee_method = get_method_from_rpc_func(server_rpc_name)

    @cython.final
    cdef void _set_client_rpc_name(self, str client_rpc_name) except *:
        """Set the client RPC name and synchronize the callee method."""
        if self._client_rpc_name == client_rpc_name:
            return
        self._client_rpc_name = client_rpc_name
        self.callee_method = get_method_from_rpc_func(client_rpc_name)

    @cython.final
    cdef void _set_caller_service_name(self, str caller_service_name) except *:
        """Set and parse the fully qualified upstream service name."""
        cdef str app
        cdef str server
        cdef str service

        if self._caller_service_name == caller_service_name:
            return
        self._caller_service_name = caller_service_name
        if caller_service_name == "*":
            return
        app, server, service = get_app_server_service(caller_service_name)
        self.caller_app = app
        self.caller_server = server
        self.caller_service = service

    @cython.final
    cdef void _set_callee_service_name(self, str callee_service_name) except *:
        """Set and parse the fully qualified downstream service name."""
        cdef str app
        cdef str server
        cdef str service

        if self._callee_service_name == callee_service_name:
            return
        self._callee_service_name = callee_service_name
        if callee_service_name == "*":
            return
        app, server, service = get_app_server_service(callee_service_name)
        self.callee_app = app
        self.callee_server = server
        self.callee_service = service

    cpdef void set_frame_head(self, TrpcFrameHead frame_head) except *:
        """Replace the fixed-width frame header.

        The header object is adopted, not copied, so the caller must not keep
        mutating it afterwards.

        Args:
            frame_head: Header holding host-endian values.
        """
        self.frame_head = frame_head

    cpdef void set_timeout(self, int timeout) except *:
        """Set the request timeout.

        Args:
            timeout: Timeout in milliseconds.
        """
        self.timeout = timeout

    cpdef void set_serialization_type(self, int serialization_type) except *:
        """Set the body serializer.

        Args:
            serialization_type: ``SerializationType`` identifier.
        """
        self.serialization_type = serialization_type

    cpdef void set_compress_type(self, int compress_type) except *:
        """Set the body compressor.

        Args:
            compress_type: ``CompressType`` identifier.
        """
        self.compress_type = compress_type

    cpdef void set_stream_id(self, uint32_t stream_id) except *:
        """Set the stream identifier tracked on the message.

        This mirrors ``frame_head.stream_id`` for callers that hold only the
        message; updating one does not update the other.

        Args:
            stream_id: Stream identifier; 0 on unary calls.
        """
        self.stream_id = stream_id

    cpdef void set_dyeing(self, bint dyeing) except *:
        """Enable or disable traffic dyeing.

        Args:
            dyeing: True when the request is dyed.
        """
        self.dyeing = dyeing

    cpdef void set_dyeing_key(self, str dyeing_key) except *:
        """Set the routing key used for dyed traffic.

        Args:
            dyeing_key: Dyeing key.
        """
        self.dyeing_key = dyeing_key

    cpdef void set_server_rpc_name(self, str server_rpc_name) except *:
        """Set the inbound RPC name and resynchronize ``callee_method``.

        Args:
            server_rpc_name: RPC name observed by the server.
        """
        self._set_server_rpc_name(server_rpc_name)

    cpdef void set_client_rpc_name(self, str client_rpc_name) except *:
        """Set the outbound RPC name and resynchronize ``callee_method``.

        Args:
            client_rpc_name: RPC name selected for the downstream call.
        """
        self._set_client_rpc_name(client_rpc_name)

    cpdef void set_server_meta_data(self, object server_meta_data) except *:
        """Replace the server-owned transmissive metadata.

        Args:
            server_meta_data: Transmissive map, usually a dict.
        """
        self.server_meta_data = server_meta_data

    cpdef void set_client_meta_data(self, object client_meta_data) except *:
        """Replace the client-owned transmissive metadata.

        Args:
            client_meta_data: Transmissive map, usually a dict.
        """
        self.client_meta_data = client_meta_data

    cpdef void set_caller_service_name(self, str caller_service_name) except *:
        """Set the upstream service name and reparse app / server / service.

        Args:
            caller_service_name: Fully qualified upstream service name; ``"*"``
                is stored without reparsing.
        """
        self._set_caller_service_name(caller_service_name)

    cpdef void set_callee_service_name(self, str callee_service_name) except *:
        """Set the downstream service name and reparse app / server / service.

        Args:
            callee_service_name: Fully qualified downstream service name;
                ``"*"`` is stored without reparsing.
        """
        self._set_callee_service_name(callee_service_name)

    cpdef void set_server_rsp_err(self, object server_rsp_err) except *:
        """Set the error encoded into the server response.

        Args:
            server_rsp_err: ``create_frame_error`` result; the success variant
                rather than None marks a clean response.
        """
        self.server_rsp_err = server_rsp_err

    cpdef void set_client_rsp_err(self, object client_rsp_err) except *:
        """Set the error returned by a downstream call.

        Args:
            client_rsp_err: Downstream error, or None when the call succeeded.
        """
        self.client_rsp_err = client_rsp_err

    cpdef void set_server_req_head(self, object server_req_head) except *:
        """Set the protocol-specific inbound request header.

        Args:
            server_req_head: Decoded request protocol header.
        """
        self.server_req_head = server_req_head

    cpdef void set_server_rsp_head(self, object server_rsp_head) except *:
        """Set the protocol-specific outbound response header.

        Args:
            server_rsp_head: Response protocol header sent upstream.
        """
        self.server_rsp_head = server_rsp_head

    cpdef void set_client_req_head(self, object client_req_head) except *:
        """Set the protocol-specific downstream request header.

        Args:
            client_req_head: Request protocol header sent downstream.
        """
        self.client_req_head = client_req_head

    cpdef void set_client_rsp_head(self, object client_rsp_head) except *:
        """Set the protocol-specific downstream response header.

        Args:
            client_rsp_head: Response protocol header returned by the callee.
        """
        self.client_rsp_head = client_rsp_head

    cpdef void set_server_raw_req(self, object server_raw_req) except *:
        """Set the protocol-native inbound request object.

        Held in-process only; it is never serialized or propagated
        downstream, unlike ``server_meta_data``.

        Args:
            server_raw_req: Complete request object of the inbound
                protocol, e.g. the aiohttp ``Request`` for HTTP.
        """
        self.server_raw_req = server_raw_req

    cpdef void set_server_raw_rsp(self, object server_raw_rsp) except *:
        """Set the protocol-native outbound response object.

        Args:
            server_raw_rsp: Complete response object that the inbound
                protocol sends back, e.g. the aiohttp ``Response`` for HTTP.
        """
        self.server_raw_rsp = server_raw_rsp

    cpdef void set_local_ip(self, str local_ip) except *:
        """Set the local IP of this RPC.

        Args:
            local_ip: Local IP, without port.
        """
        self.local_ip = local_ip

    cpdef void set_remote_addr(self, str remote_addr) except *:
        """Set the peer network endpoint of this RPC.

        Args:
            remote_addr: Remote address.
        """
        self.remote_addr = remote_addr

    cpdef void set_logger(self, str logger) except *:
        """Set the logger category.

        Args:
            logger: Logger name.
        """
        self.logger = logger

    cpdef void set_caller_app(self, str caller_app) except *:
        """Overwrite the parsed caller app without touching the full name.

        Args:
            caller_app: Caller application name.
        """
        self.caller_app = caller_app

    cpdef void set_caller_server(self, str caller_server) except *:
        """Overwrite the parsed caller server without touching the full name.

        Args:
            caller_server: Caller server name.
        """
        self.caller_server = caller_server

    cpdef void set_caller_service(self, str caller_service) except *:
        """Overwrite the parsed caller service without touching the full name.

        Args:
            caller_service: Caller service name.
        """
        self.caller_service = caller_service

    cpdef void set_caller_method(self, str caller_method) except *:
        """Set the method invoked by the upstream caller.

        Args:
            caller_method: Caller method name.
        """
        self.caller_method = caller_method

    cpdef void set_callee_app(self, str callee_app) except *:
        """Overwrite the parsed callee app without touching the full name.

        Args:
            callee_app: Callee application name.
        """
        self.callee_app = callee_app

    cpdef void set_callee_server(self, str callee_server) except *:
        """Overwrite the parsed callee server without touching the full name.

        Args:
            callee_server: Callee server name.
        """
        self.callee_server = callee_server

    cpdef void set_callee_service(self, str callee_service) except *:
        """Overwrite the parsed callee service without touching the full name.

        Args:
            callee_service: Callee service name.
        """
        self.callee_service = callee_service

    cpdef void set_callee_method(self, str callee_method) except *:
        """Overwrite the callee method without touching the RPC name.

        Args:
            callee_method: Callee method name.
        """
        self.callee_method = callee_method

    cpdef void set_namespace(self, str namespace) except *:
        """Set the deployment namespace.

        Args:
            namespace: Namespace name.
        """
        self.namespace = namespace

    cpdef void set_env_name(self, str env_name) except *:
        """Set the deployment environment name.

        Args:
            env_name: Environment name.
        """
        self.env_name = env_name

    cpdef void set_setname(self, str set_name) except *:
        """Set the logical deployment set name.

        The asymmetric method name is historical; the field is ``set_name``.

        Args:
            set_name: Deployment set name.
        """
        self.set_name = set_name

    cpdef void set_env_transfer(self, str env_transfer) except *:
        """Set the environment-transfer routing value.

        Args:
            env_transfer: Environment-transfer value.
        """
        self.env_transfer = env_transfer

    cpdef void set_callee_container_name(self, str callee_container_name) except *:
        """Set the target container name.

        Args:
            callee_container_name: Container name.
        """
        self.callee_container_name = callee_container_name

    cpdef void set_callee_set_name(self, str callee_set_name) except *:
        """Set the target deployment set name.

        Args:
            callee_set_name: Callee set name.
        """
        self.callee_set_name = callee_set_name

    cpdef void set_stream_frame(self, object stream_frame) except *:
        """Set the protocol-specific streaming frame payload.

        Args:
            stream_frame: Stream frame object, or None on unary calls.
        """
        self.stream_frame = stream_frame

    cpdef void set_request_id(self, uint32_t request_id) except *:
        """Set the request correlation identifier.

        Args:
            request_id: Request identifier.
        """
        self.request_id = request_id

    cpdef void set_session_id(self, int session_id) except *:
        """Set the stateful transport session identifier.

        This identifies the connection-level session, not the request.

        Args:
            session_id: Session identifier.
        """
        self.session_id = session_id

    cpdef void set_request_attachment(self, bytes attachment) except *:
        """Set the opaque request attachment.

        Args:
            attachment: Attachment bytes travelling beside the request body.
        """
        self.request_attachment = attachment

    cpdef void set_response_attachment(self, bytes attachment) except *:
        """Set the opaque response attachment.

        Args:
            attachment: Attachment bytes travelling beside the response body.
        """
        self.response_attachment = attachment

    def __repr__(self):
        """Return a concise representation of the callee and response header."""
        return "%s %s" % (self.callee_service_name, self.client_rsp_head)
