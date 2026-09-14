# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

# cython: language_level=3, boundscheck=False, wraparound=False

"""Unary tRPC encode / decode for client and server.

Packs ``trpc_pb2.RequestProtocol`` / ``trpc_pb2.ResponseProtocol`` after the 16-byte frame
head. Stream frames live in :mod:`trpc_sdk.codec._c_codec_stream`. Python
:class:`~trpc_sdk.codec.ClientCodec` / ``ServerCodec`` dispatch unary
here and stream there.

Wire layout produced by :func:`_common_pack`::

    [16-byte CFrameHead][trpc_pb2.RequestProtocol|trpc_pb2.ResponseProtocol][body][attachment]

``head_len`` in the frame head is the protocol-header size, not the
16-byte frame head. Body occupies
``[16 + head_len, len(buffer) - attachment_size)``; the attachment is
the tail.
"""

import copy
from libc.string cimport memcpy
from cpython.mem cimport PyMem_Free
from cpython.mem cimport PyMem_Malloc

from trpc_sdk._inner import trpc_pb2
from trpc_sdk.exceptions import ClientDecodeError
from trpc_sdk.exceptions import ClientEncodeError
from trpc_sdk.exceptions import ServerDecodeError
from trpc_sdk.exceptions import TrpcErrorType
from trpc_sdk.exceptions import TrpcException
from ._constants import DYEING_KEY as _PY_DYEING_KEY
from ._constants import EnvTransfer as _PY_ENV_TRANSFER
from ._constants import FRAME_HEAD_LEN as _PY_FRAME_HEAD_LEN
from ._message cimport CFrameHead
from ._message cimport Message
from ._message cimport htonl
from ._message cimport htons
from ._message cimport parse_frame_head

cdef int FRAME_HEAD_LEN = _PY_FRAME_HEAD_LEN
# Per-instance request_id wrap; not a globally unique id. A uint32 width
# fact rather than a protocol constant, so it stays out of _constants.py.
cdef unsigned int UINT32_MAX_VALUE = 4294967295
# trpc_pb2.RequestProtocol.trans_info keys; _constants.py is the single source.
cdef str DYEING_KEY = _PY_DYEING_KEY
cdef str EnvTransfer = _PY_ENV_TRANSFER
ctypedef unsigned int uint
ctypedef unsigned short ushort
ctypedef unsigned char uchar


cdef class CClientCodec:
    """Client-side unary codec: request encode and response decode.

    ``request_id`` is a per-instance counter incremented on each
    ``encode``. It wraps at ``UINT32_MAX_VALUE`` and is not unique
    across processes or codec instances.
    """

    cpdef bytes encode(self, Message message, bytes body):
        """Pack a client request body into a unary tRPC frame.

        If ``message.client_req_head`` is already a
        ``trpc_pb2.RequestProtocol``, that instance is reused. Otherwise a
        ``deepcopy`` of ``server_req_head`` is taken when the inbound
        head is a ``trpc_pb2.RequestProtocol`` (proxy / nested-call path);
        otherwise a fresh ``trpc_pb2.RequestProtocol`` is created. The chosen
        head is written back with ``set_client_req_head``.

        Empty ``caller_service_name`` is filled with the historical
        placeholder ``trpc.app.server.service``. When ``callee_app`` or
        ``callee_method`` is still empty, the matching ``set_*`` is
        called after clearing the raw field so derived app / server /
        service / method fields sync.

        Args:
            message: Outbound RPC state; fields are copied into the
                request head.
            body: Already serialized (and optionally compressed) body.

        Returns:
            Complete unary frame including the 16-byte head.

        Raises:
            ClientEncodeError: ``client_req_head`` is set but is not a
                ``trpc_pb2.RequestProtocol``.
        """
        # Build the downstream request head.
        if message.client_req_head:
            # Head is set: check that the user-created request head is usable.
            if not isinstance(message.client_req_head, trpc_pb2.RequestProtocol):
                raise ClientEncodeError("client encode req head type invalid")
            req = message.client_req_head
        else:
            # Head is empty: copy the server req head (nested / proxy call).
            if isinstance(message.server_req_head, trpc_pb2.RequestProtocol):
                req = copy.deepcopy(message.server_req_head)
            else:
                req = trpc_pb2.RequestProtocol()
            # Store the new client req head.
            message.set_client_req_head(req)

        # Increment and wrap so request_id stays within uint32.
        self.request_id = (self.request_id + 1)%UINT32_MAX_VALUE
        req.request_id = self.request_id

        message.set_request_id(req.request_id)

        if len(message.caller_service_name) == 0:
            req.caller = b"trpc.app.server.service"
            message.set_caller_app("app")
            message.set_caller_server("server")
            message.set_caller_service("service")
            message.caller_service_name = "trpc.app.server.service"
        else:
            req.caller = message.caller_service_name.encode('utf8')

        # Set the callee service name.
        req.callee = message.callee_service_name.encode('utf8')
        # Set the downstream RPC method name; the client stub fills this.
        req.func = message.client_rpc_name.encode('utf8')
        # Set the downstream serialization type.
        req.content_type = message.serialization_type
        # Set the downstream compression type.
        req.content_encoding = message.compress_type
        # Set the remaining timeout for the downstream.
        req.timeout = message.timeout

        if not message.callee_app and message.callee_service_name:
            callee_service_name = message.callee_service_name
            message.callee_service_name = ""
            message.set_callee_service_name(callee_service_name)

        if not message.callee_method and message.client_rpc_name:
            client_rpc_name = message.client_rpc_name
            message.client_rpc_name = ""
            message.set_client_rpc_name(client_rpc_name)

        # Set tracing metadata.
        for key, value in message.client_meta_data.items():
            if isinstance(value, str):
                value = value.encode('utf8')
            req.trans_info[key] = value

        # Set dyeing info.
        if message.dyeing:
            req.message_type = req.message_type | trpc_pb2.TRPC_DYEING_MESSAGE
        if message.dyeing_key:
            req.trans_info[DYEING_KEY] = message.dyeing_key.encode('utf8')
        # Set env-transfer info.
        if len(message.env_transfer) > 0:
            req.trans_info[EnvTransfer] = message.env_transfer.encode('utf8')

        req.attachment_size = len(message.request_attachment)
        cdef bytes req_head = req.SerializeToString()
        return _common_pack(req_head, message.frame_head, body, message.request_attachment)


    cpdef bytes decode(self, Message message, bytes buffer):
        """Unpack a unary response frame into the message and body.

        Reuses ``message.client_rsp_head`` when the caller already
        installed a ``trpc_pb2.ResponseProtocol``; otherwise a fresh head is
        created and stored.

        Args:
            message: Client RPC state to fill from the response.
            buffer: Complete unary frame, including the 16-byte head.

        Returns:
            Response body between the protocol head and the attachment.

        Raises:
            ClientDecodeError: Frame shorter than 16 bytes, or
                ``client_rsp_head`` is set but is not a
                ``trpc_pb2.ResponseProtocol``. ``_unpack`` may raise further
                length errors.
        """
        if len(buffer) < FRAME_HEAD_LEN:
            raise ClientDecodeError("client decode rsp buf len invalid")

        if message.client_rsp_head:
            # Head is set: check that the user-created response head is usable.
            if not isinstance(message.client_rsp_head, trpc_pb2.ResponseProtocol):
                raise ClientDecodeError("client decode rsp head type invalid")
            rsp = message.client_rsp_head
        else:
            # Head is empty: create a default response head.
            rsp = trpc_pb2.ResponseProtocol()
            message.set_client_rsp_head(rsp)
        return self._unpack(rsp, message, buffer)

    cpdef bytes _unpack(self, object rsp, Message message, bytes buffer):
        """Parse the frame head, ``trpc_pb2.ResponseProtocol``, body, and attachment.

        ``head_len`` is the protocol-header size after the 16-byte
        frame head. Body is
        ``[FRAME_HEAD_LEN + head_len, len(buffer) - attachment_size)``.
        Both ``rsp.ret`` and ``rsp.func_ret`` are stored as
        ``TrpcErrorType.BUSINESS`` on ``client_rsp_err``.

        Args:
            rsp: ``trpc_pb2.ResponseProtocol`` instance filled in place.
            message: Client RPC state updated with codec fields.
            buffer: Complete unary frame, including the 16-byte head.

        Returns:
            Response body bytes, not including the attachment.

        Raises:
            ClientDecodeError: Empty or oversized protocol head, or
                ``attachment_size`` overlapping the body.
        """
        cdef uint buffer_length = len(buffer)

        frame_head = parse_frame_head(buffer)
        if frame_head.head_len == 0:
            raise ClientDecodeError("client decode pb head len empty")
        if frame_head.head_len > len(buffer) - FRAME_HEAD_LEN:
            raise ClientDecodeError("client decode pb head len invalid")

        # Parse the response head.
        head_buffer = buffer[FRAME_HEAD_LEN:FRAME_HEAD_LEN + frame_head.head_len]
        rsp.ParseFromString(head_buffer)
        message.set_frame_head(frame_head)
        message.set_compress_type(int(rsp.content_encoding))
        message.set_serialization_type(int(rsp.content_type))

        # Reset transmissive info returned by the downstream.
        if len(rsp.trans_info) > 0:
            trans_info = {}
            for key, value in rsp.trans_info.items():
                trans_info[key] = value
            message.set_client_meta_data(trans_info)
        if rsp.ret != 0:
            message.set_client_rsp_err(TrpcException(TrpcErrorType.BUSINESS, rsp.ret, rsp.error_msg))
        elif rsp.func_ret != 0:
            message.set_client_rsp_err(TrpcException(TrpcErrorType.BUSINESS, rsp.func_ret, rsp.error_msg))

        body_start = FRAME_HEAD_LEN + frame_head.head_len
        attachment_size = int(rsp.attachment_size)
        body_end = buffer_length - attachment_size
        if body_end < body_start:
            raise ClientDecodeError("client decode attachment size invalid")
        message.set_response_attachment(buffer[body_end:buffer_length])
        return buffer[body_start:body_end]

    cpdef void update_msg_by_msg(self, Message origin_message, Message target_message):
        """Copy a decoded response from a temporary message onto the request.

        Multiplexed receive builds a scratch message, then moves the
        head, frame, compress / serialize types, trans_info, error, and
        attachment onto the original request message. Equivalent to the
        second half of ``_unpack`` without touching the body.

        Args:
            origin_message: Temporary message that already ran decode.
            target_message: The in-flight request message to update.
        """
        # Update the request msg from a temporary msg.
        if origin_message.client_rsp_head:
            target_message.set_client_rsp_head(origin_message.client_rsp_head)

        if origin_message.frame_head:
            target_message.set_frame_head(origin_message.frame_head)

        rsp = origin_message.client_rsp_head
        target_message.set_compress_type(int(rsp.content_encoding))
        target_message.set_serialization_type(int(rsp.content_type))
        # Reset transmissive info returned by the downstream.
        if len(rsp.trans_info) > 0:
            trans_info = {}
            for key, value in rsp.trans_info.items():
                trans_info[key] = value
            target_message.set_client_meta_data(trans_info)
        if rsp.ret != 0:
            target_message.set_client_rsp_err(TrpcException(TrpcErrorType.BUSINESS, rsp.ret, rsp.error_msg))
        elif rsp.func_ret != 0:
            target_message.set_client_rsp_err(TrpcException(TrpcErrorType.BUSINESS, rsp.func_ret, rsp.error_msg))
        target_message.set_response_attachment(origin_message.response_attachment)
        return
    

cdef class CServerCodec:
    """Server-side unary codec: request decode and response encode."""

    cpdef bytes encode(self, Message message, bytes body):
        """Pack a server response body into a unary tRPC frame.

        ``server_rsp_err`` is mapped onto the protocol head:
        ``TrpcErrorType.BUSINESS`` writes ``func_ret``; any other type
        writes ``ret``. ``version`` / ``call_type`` / ``request_id``
        were copied from the request during ``decode`` and stay on
        ``server_rsp_head``.

        Args:
            message: Inbound RPC state plus the handler error.
            body: Already serialized (and optionally compressed) body.

        Returns:
            Complete unary frame including the 16-byte head.
        """
        rsp = message.server_rsp_head
        if not isinstance(message.server_rsp_head, trpc_pb2.ResponseProtocol):
            rsp = trpc_pb2.ResponseProtocol()

        # Convert the handler error into the protocol-head error-code fields.
        err = message.server_rsp_err
        rsp.error_msg = err.msg.encode('utf8')
        if err.type == TrpcErrorType.BUSINESS:
            rsp.func_ret = err.code
        else:
            rsp.ret = err.code

        for key, value in message.server_meta_data.items():
            if isinstance(value, str):
                value = value.encode('utf8')
            rsp.trans_info[key] = value
        rsp.attachment_size = len(message.response_attachment)
        rsp_head = rsp.SerializeToString()
        return _common_pack(rsp_head, message.frame_head, body, message.response_attachment)


    cpdef bytes decode(self, Message message, bytes buffer):
        """Unpack a unary request frame and seed the response head.

        After ``_unpack``, a ``trpc_pb2.ResponseProtocol`` is built by copying
        ``version``, ``call_type``, ``request_id``, ``message_type``,
        ``content_type``, and ``content_encoding`` from the request.
        The reply encode path relies on those fields matching.

        When ``caller_app`` / ``callee_app`` / ``callee_method`` is
        empty, the matching ``set_*`` is called after clearing the raw
        name so derived fields sync. Dyeing and env-transfer values
        are taken from ``trans_info`` keys ``trpc-dyeing-key`` and
        ``trpc-env``.

        Args:
            message: Fresh server RPC state to fill.
            buffer: Complete unary frame, including the 16-byte head.

        Returns:
            Request body between the protocol head and the attachment.

        Raises:
            ServerDecodeError: Frame shorter than 16 bytes.
                ``_unpack`` may raise further length errors.
        """
        req = trpc_pb2.RequestProtocol()

        if len(buffer) < FRAME_HEAD_LEN:
            raise ServerDecodeError("server decode req buf len invalid")

        req_body = self._unpack(req, message, buffer)

        # Set the protocol-specific request head.
        message.set_server_req_head(req)

        # Build the response head.
        rsp = trpc_pb2.ResponseProtocol()
        rsp.version = trpc_pb2.TRPC_PROTO_V1
        rsp.call_type = req.call_type
        rsp.request_id = req.request_id
        rsp.message_type = req.message_type
        rsp.content_type = req.content_type
        rsp.content_encoding = req.content_encoding
        message.set_server_rsp_head(rsp)

        # Fields the tRPC framework needs.
        # Set the timeout allowed by the upstream.
        message.set_timeout(req.timeout)
        # Set the upstream service name.
        message.set_caller_service_name(req.caller.decode('utf8'))
        message.set_callee_service_name(req.callee.decode('utf8'))
        # Set the RPC method name (command) of this request.
        message.set_server_rpc_name(req.func.decode('utf8'))
        # Set the body serialization type.
        message.set_serialization_type(req.content_type)
        # Set the body compression type.
        message.set_compress_type(req.content_encoding)
        # Set the dyeing flag.
        message.set_dyeing((req.message_type & trpc_pb2.TRPC_DYEING_MESSAGE) != 0)

        # Parse tracing metadata and store it on the msg.
        if len(req.trans_info) > 0:
            message.set_server_meta_data(dict(req.trans_info))

            # Dyeing key.
            if DYEING_KEY in req.trans_info:
                message.set_dyeing_key(req.trans_info[DYEING_KEY].decode('utf8'))

            # Env-transfer info.
            if EnvTransfer in req.trans_info:
                message.set_env_transfer(req.trans_info[EnvTransfer].decode('utf8'))

        # Parse app / server from the service name.
        if not message.caller_app and message.caller_service_name:
            caller_service_name = message.caller_service_name
            message.caller_service_name = ""
            message.set_caller_service_name(caller_service_name)

        if not message.callee_app and message.callee_service_name:
            callee_service_name = message.callee_service_name
            message.callee_service_name = ""
            message.set_callee_service_name(caller_service_name)

        if not message.callee_method and message.server_rpc_name:
            server_rpc_name = message.server_rpc_name
            message.server_rpc_name = ""
            message.set_server_rpc_name(server_rpc_name)

        return req_body


    cpdef bytes _unpack(self, object req, Message message, bytes buffer):
        """Parse the frame head, ``trpc_pb2.RequestProtocol``, body, and attachment.

        Same three-way length split as the client unpacker: protocol
        head, body, then trailing attachment.

        Args:
            req: ``trpc_pb2.RequestProtocol`` instance filled in place.
            message: Server RPC state; receives the frame head and
                request attachment.
            buffer: Complete unary frame, including the 16-byte head.

        Returns:
            Request body bytes, not including the attachment.

        Raises:
            ServerDecodeError: Empty or oversized protocol head, or
                ``attachment_size`` overlapping the body.
        """

        cdef uint buffer_length = len(buffer)

        frame_head = parse_frame_head(buffer)
        if frame_head.head_len == 0:
            raise ServerDecodeError("server decode pb head len empty")
        if frame_head.head_len > len(buffer) - FRAME_HEAD_LEN:
            raise ServerDecodeError("server decode pb head len invalid expect %s but get %s" %
                (frame_head.head_len, len(buffer)))

        # Parse the request head.
        head_buffer = buffer[FRAME_HEAD_LEN:FRAME_HEAD_LEN + frame_head.head_len]
        req.ParseFromString(head_buffer)
        message.set_frame_head(frame_head)
        body_start = FRAME_HEAD_LEN + frame_head.head_len
        attachment_size = int(req.attachment_size)
        body_end = buffer_length - attachment_size
        if body_end < body_start:
            raise ServerDecodeError("server decode attachment size invalid")
        message.set_request_attachment(buffer[body_end:buffer_length])
        return buffer[body_start:body_end]


cpdef bytes _common_pack(bytes req_head, CFrameHead frame_head, bytes body, bytes attachment = b""):
    """Hand-pack a unary frame: 16-byte head + protocol head + body + attachment.

    Multi-byte fields are written big-endian with ``htons`` / ``htonl``.
    Attachment is ``memcpy``'d only when non-empty. ``PyMem_Malloc``
    is not null-checked (historical).

    Args:
        req_head: Serialized ``trpc_pb2.RequestProtocol`` or ``trpc_pb2.ResponseProtocol``.
        frame_head: Host-endian type / state / stream id / reserved.
        body: Serialized (and optionally compressed) RPC body.
        attachment: Optional trailing attachment; default empty.

    Returns:
        Complete unary frame bytes.
    """
    cdef ushort head_len = len(req_head)
    cdef uint body_len = len(body)
    cdef uint attachment_len = len(attachment)
    cdef uint total_len = FRAME_HEAD_LEN + head_len + body_len + attachment_len


    cdef char* to_prt = <char *> PyMem_Malloc(total_len)
    cdef ushort magic = trpc_pb2.TRPC_MAGIC_VALUE
    (<ushort *>(to_prt))[0] = htons(magic)
    (<uchar *>(to_prt+2))[0] = frame_head.frame_type
    (<uchar *>(to_prt+3))[0] = frame_head.frame_state
    (<uint *>(to_prt+4))[0] = htonl(total_len)
    (<ushort *>(to_prt+8))[0] = htons(head_len)
    (<uint *>(to_prt+10))[0] = htonl(frame_head.stream_id)
    (<ushort *>(to_prt+14))[0] = htons(frame_head.frame_reserved)

    cdef char* req_head_ptr = req_head
    memcpy(to_prt+FRAME_HEAD_LEN, req_head_ptr, head_len)

    cdef char* body_ptr = body
    memcpy(to_prt+FRAME_HEAD_LEN+head_len, body_ptr, body_len)

    cdef char* attachment_ptr = attachment
    if attachment_len > 0:
        memcpy(to_prt+FRAME_HEAD_LEN+head_len+body_len, attachment_ptr, attachment_len)

    results = bytes(to_prt[:total_len])

    PyMem_Free(to_prt)

    return results
