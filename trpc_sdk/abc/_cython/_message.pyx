# Tencent is pleased to support the open source community by making tRPC-Agent-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Agent-Python is licensed under Apache-2.0.

# cython: language_level=3, boundscheck=False, wraparound=False

cimport cython

from ._utils cimport get_app_server_service
from ._utils cimport get_method_from_rpc_func


cdef class TrpcFrameHead:
    """Store the fixed-width fields of a tRPC transport frame header.

    Independent native fields remain public so statically typed Cython callers
    can read and write them without method or Python dispatch overhead.
    """

    def __cinit__(self):
        self.stx = 0
        self.frame_type = 0
        self.frame_state = 0
        self.package_len = 0
        self.head_len = 0
        self.stream_id = 0
        self.frame_reserved = 0


cdef class TrpcMessage:
    """Store protocol-independent state associated with an RPC message.

    The message contains transport and protocol data only. Runtime ownership
    belongs to ``TrpcContext``; the message deliberately has no context
    reference. Independent fields are public for direct Cython access, while
    names with derived components use final C-level synchronization helpers.
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
        self.server_rsp_err = None
        self.client_rsp_err = None
        self.server_req_head = None
        self.server_rsp_head = None
        self.client_req_head = None
        self.client_rsp_head = None
        self.caller_app = ""
        self.caller_server = ""
        self.caller_service = ""
        self.caller_method = ""
        self.callee_app = ""
        self.callee_server = ""
        self.callee_service = ""
        self.callee_method = ""
        self.local_addr = ""
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

    def __repr__(self):
        """Return a concise representation of the callee and response header."""
        return "%s %s" % (self.callee_service_name, self.client_rsp_head)
