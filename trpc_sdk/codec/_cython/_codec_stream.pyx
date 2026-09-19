# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

# cython: language_level=3, boundscheck=False, wraparound=False

"""Stream-frame tRPC encode / decode for client and server.

Four ``frame_state`` values after a stream ``frame_type``:

* INIT carries ``trpc_pb2.TrpcStreamInitMeta`` (caller / callee / func / trans_info).
* DATA carries the business body only.
* CLOSE carries ``trpc_pb2.TrpcStreamCloseMeta``.
* FEEDBACK carries ``trpc_pb2.TrpcStreamFeedBackMeta``.

Unlike unary :func:`trpc_sdk.codec._cython._codec._common_pack`, this module's
``_common_pack`` writes ``head_len = 0`` at offset 8: stream meta lives
in the body slot, not as a separate protocol head.

``encode_init_frame`` / ``encode_close_frame`` / ``encode_feedback_frame``
ignore the ``body`` argument. The payload is ``message.stream_frame``
serialized. The signature stays aligned with ``encode_data_frame`` so
:mod:`trpc_sdk.codec._codec` can dispatch all four the same way.

Decode parameters are named ``body`` but are the **complete frame**,
including the 16-byte head; every decoder slices
``body[FRAME_HEAD_LEN:]``. INIT / CLOSE / FEEDBACK return ``b''``
(no business body); only DATA returns the payload.

Server ``decode_init_frame`` stores caller / callee / func on the
message and ``set_stream_frame``. Later DATA / CLOSE frames recover
``server_rpc_name`` from the INIT cache in
:meth:`trpc_sdk.codec.ServerCodec.set_init_meta` keyed by
``session_id + stream_id``.
"""

from libc.string cimport memcpy
from cpython.mem cimport PyMem_Free
from cpython.mem cimport PyMem_Malloc

from trpc_sdk._inner import trpc_pb2
from trpc_sdk.exceptions import ClientEncodeError
from trpc_sdk.exceptions import ServerEncodeError
from trpc_sdk.exceptions import TrpcErrorType
from trpc_sdk.exceptions import TrpcException
from trpc_sdk.abc._cython cimport TrpcFrameHead
from trpc_sdk.abc._cython cimport TrpcMessage
from .._constants import DYEING_KEY as _PY_DYEING_KEY
from .._constants import EnvTransfer as _PY_ENV_TRANSFER
from .._constants import FRAME_HEAD_LEN as _PY_FRAME_HEAD_LEN

cdef int FRAME_HEAD_LEN = _PY_FRAME_HEAD_LEN
# Request meta trans_info keys; _constants.py is the single source.
cdef str DYEING_KEY = _PY_DYEING_KEY
cdef str EnvTransfer = _PY_ENV_TRANSFER
ctypedef unsigned int uint
ctypedef unsigned short ushort
ctypedef unsigned char uchar


cdef ushort endian_byte_swapper_ushort(ushort src):
    """Swap a 16-bit value between big and little endian.

    Unused here; packing uses ``htons`` / ``htonl``. Kept for historical
    Cython callers.
    """
    return  (src >> 8) | (src << 8)


cdef uint endian_byte_swapper_uint(uint src):
    """Swap a 32-bit value between big and little endian.

    Unused here; packing uses ``htons`` / ``htonl``. Kept for historical
    Cython callers.
    """
    return (src & 0x000000FFU) << 24 | (src & 0x0000FF00U) << 8 | (src & 0x00FF0000U) >> 8 | (src & 0xFF000000U) >> 24


cdef class CClientStreamCodec:
    """Client-side stream codec: INIT / DATA / CLOSE / FEEDBACK."""

    cpdef bytes encode_init_frame(self, TrpcMessage message, bytes body):
        """Pack a client INIT frame from ``message.stream_frame``.

        ``body`` is unused; the payload is serialized
        ``trpc_pb2.TrpcStreamInitMeta``. Reuses ``message.stream_frame`` when it
        is already that type; otherwise a fresh meta is created. Empty
        ``caller_service_name`` is filled with
        ``trpc.app.server.service``. ``callee_app`` / ``callee_method``
        are synced through ``set_*`` after the raw field is cleared.

        Args:
            message: Outbound stream state; request_meta is filled from
                caller / callee / func / trans_info / dyeing / env.
            body: Unused; kept so the four encode methods share a
                signature for :mod:`trpc_sdk.codec._codec` dispatch.

        Returns:
            Complete stream frame with ``head_len`` written as 0.

        Raises:
            ClientEncodeError: ``stream_frame`` is set but is not a
                ``trpc_pb2.TrpcStreamInitMeta``.
        """
        if message.stream_frame:
            if not isinstance(message.stream_frame, trpc_pb2.TrpcStreamInitMeta):
                raise ClientEncodeError("client encode stream frame type invalid")
            init_meta = message.stream_frame
        else:
            init_meta = trpc_pb2.TrpcStreamInitMeta()

        req = init_meta.request_meta

        if len(message.get_caller_service_name()) == 0:
            req.caller = b"trpc.app.server.service"
            # The setter parses the placeholder into app / server / service.
            message._set_caller_service_name("trpc.app.server.service")
        else:
            req.caller = message.get_caller_service_name().encode('utf8')

        # Set the callee service name.
        req.callee = message.get_callee_service_name().encode('utf8')
        # Set the downstream RPC method name; the client stub fills this.
        req.func = message.get_client_rpc_name().encode('utf8')
        # Set the downstream serialization type.
        init_meta.content_type = message.serialization_type
        # Set the downstream compression type.
        init_meta.content_encoding = message.compress_type
        # Clear first: the setters short-circuit when the value is unchanged,
        # so an empty derived field only refills after the name is reset.
        if not message.callee_app and message.get_callee_service_name():
            callee_service_name = message.get_callee_service_name()
            message._set_callee_service_name("")
            message._set_callee_service_name(callee_service_name)
        if not message.callee_method and message.get_client_rpc_name():
            client_rpc_name = message.get_client_rpc_name()
            message._set_client_rpc_name("")
            message._set_client_rpc_name(client_rpc_name)
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
        if len(str(message.env_transfer)) > 0:
            req.trans_info[EnvTransfer] = message.env_transfer.encode('utf8')

        stream_buf = init_meta.SerializeToString()
        return _common_pack(message.frame_head, stream_buf)

    cpdef bytes encode_data_frame(self, TrpcMessage message, bytes body):
        """Pack a DATA frame; ``body`` is the business payload.

        No stream meta is serialized. ``message.stream_frame`` is not
        read.

        Args:
            message: Supplies ``frame_head`` (type / state / stream id).
            body: Already serialized (and optionally compressed) body.

        Returns:
            Complete stream frame with ``head_len`` written as 0.
        """
        return _common_pack(message.frame_head, body)

    cpdef bytes encode_close_frame(self, TrpcMessage message, bytes body):
        """Pack a CLOSE frame from ``message.stream_frame``.

        ``body`` is unused; the payload is serialized
        ``trpc_pb2.TrpcStreamCloseMeta``.

        Args:
            message: Must already hold a ``trpc_pb2.TrpcStreamCloseMeta``.
            body: Unused; kept for dispatch-signature symmetry.

        Returns:
            Complete stream frame with ``head_len`` written as 0.

        Raises:
            ClientEncodeError: ``stream_frame`` is not a
                ``trpc_pb2.TrpcStreamCloseMeta``.
        """
        # close_frame type: trpc_pb2.TrpcStreamCloseMeta
        close_frame = message.stream_frame
        if not isinstance(message.stream_frame, trpc_pb2.TrpcStreamCloseMeta):
            raise ClientEncodeError("client encode stream frame type invalid")

        stream_buf = close_frame.SerializeToString()
        return _common_pack(message.frame_head, stream_buf)

    cpdef bytes encode_feedback_frame(self, TrpcMessage message, bytes body):
        """Pack a FEEDBACK frame from ``message.stream_frame``.

        ``body`` is unused; the payload is serialized
        ``trpc_pb2.TrpcStreamFeedBackMeta``.

        Args:
            message: Must already hold a ``trpc_pb2.TrpcStreamFeedBackMeta``.
            body: Unused; kept for dispatch-signature symmetry.

        Returns:
            Complete stream frame with ``head_len`` written as 0.

        Raises:
            ClientEncodeError: ``stream_frame`` is not a
                ``trpc_pb2.TrpcStreamFeedBackMeta``.
        """
        # feedback_frame type: trpc_pb2.TrpcStreamFeedBackMeta
        feedback_frame = message.stream_frame
        if not isinstance(message.stream_frame, trpc_pb2.TrpcStreamFeedBackMeta):
            raise ClientEncodeError("client encode stream frame type invalid")

        stream_buf = feedback_frame.SerializeToString()
        return _common_pack(message.frame_head, stream_buf)

    cpdef bytes decode_init_frame(self, TrpcMessage message, bytes body):
        """Parse a server INIT reply into compress / serialize / error.

        ``body`` is the complete frame. ``response_meta.ret != 0`` is
        stored as ``TrpcErrorType.CALLEE_FRAMEWORK`` on ``client_rsp_err``.
        Returns ``b''`` because INIT has no business body.

        Args:
            message: Client stream state to fill.
            body: Complete stream frame, including the 16-byte head.

        Returns:
            Empty bytes; the meta is on ``message.stream_frame``.
        """
        init_meta = trpc_pb2.TrpcStreamInitMeta()
        init_meta.ParseFromString(body[FRAME_HEAD_LEN:])

        message.set_compress_type(init_meta.content_encoding)
        message.set_serialization_type(init_meta.content_type)

        if init_meta.response_meta and init_meta.response_meta.ret != 0:
            message.set_client_rsp_err(TrpcException(TrpcErrorType.CALLEE_FRAMEWORK,
                                                     init_meta.response_meta.ret,
                                                     init_meta.response_meta.error_msg))

        message.set_stream_frame(init_meta)
        return b''

    cpdef bytes decode_data_frame(self, TrpcMessage message, bytes body):
        """Strip the 16-byte head and return the DATA payload.

        Also copies ``frame_head.stream_id`` onto the message.

        Args:
            message: Client stream state.
            body: Complete stream frame, including the 16-byte head.

        Returns:
            Business body after the frame head.
        """
        message.set_stream_id(message.frame_head.stream_id)
        return body[FRAME_HEAD_LEN:]

    cpdef bytes decode_close_frame(self, TrpcMessage message, bytes body):
        """Parse a CLOSE meta; RESET or nonzero ``ret`` becomes an error.

        ``close_type == trpc_pb2.TRPC_STREAM_RESET`` or ``ret != 0`` writes
        ``TrpcErrorType.CALLEE_FRAMEWORK`` onto ``client_rsp_err``. Returns
        ``b''``.

        Args:
            message: Client stream state to fill.
            body: Complete stream frame, including the 16-byte head.

        Returns:
            Empty bytes; the meta is on ``message.stream_frame``.
        """
        close = trpc_pb2.TrpcStreamCloseMeta()
        close.ParseFromString(body[FRAME_HEAD_LEN:])
        if close.close_type == trpc_pb2.TRPC_STREAM_RESET or close.ret != 0:
            message.set_client_rsp_err(TrpcException(TrpcErrorType.CALLEE_FRAMEWORK, close.ret, close.msg))

        message.set_stream_frame(close)
        return b''

    cpdef bytes decode_feedback_frame(self, TrpcMessage message, bytes body):
        """Parse a FEEDBACK meta and store it on ``message.stream_frame``.

        Args:
            message: Client stream state to fill.
            body: Complete stream frame, including the 16-byte head.

        Returns:
            Empty bytes; the meta is on ``message.stream_frame``.
        """
        feedback = trpc_pb2.TrpcStreamFeedBackMeta()
        feedback.ParseFromString(body[FRAME_HEAD_LEN:])
        message.set_stream_frame(feedback)
        return b''


cdef class CServerStreamCodec:
    """Server-side stream codec: INIT / DATA / CLOSE / FEEDBACK."""

    cpdef bytes encode_init_frame(self, TrpcMessage message, bytes body):
        """Pack a server INIT reply from ``message.stream_frame``.

        ``body`` is unused. ``server_rsp_err`` is mapped onto
        ``trpc_pb2.TrpcStreamInitResponseMeta``: ``TrpcErrorType.BUSINESS`` writes
        ``func_ret``; any other type writes ``ret``.

        Args:
            message: Inbound stream state plus the handler error.
            body: Unused; kept for dispatch-signature symmetry.

        Returns:
            Complete stream frame with ``head_len`` written as 0.

        Raises:
            ServerEncodeError: ``stream_frame`` is set but is not a
                ``trpc_pb2.TrpcStreamInitMeta``.
        """
        if message.stream_frame:
            if not isinstance(message.stream_frame, trpc_pb2.TrpcStreamInitMeta):
                raise ServerEncodeError("server encode stream frame type invalid")
            init_meta = message.stream_frame
        else:
            init_meta = trpc_pb2.TrpcStreamInitMeta()

        init_meta.content_type = message.serialization_type
        init_meta.content_encoding = message.compress_type
        rsp_meta = trpc_pb2.TrpcStreamInitResponseMeta()
                
        err = message.server_rsp_err
        rsp_meta.error_msg = err.msg.encode('utf8')
        if err.type == TrpcErrorType.BUSINESS:
            rsp_meta.func_ret = err.code
        else:
            rsp_meta.ret = err.code

        init_meta.response_meta.CopyFrom(rsp_meta)

        stream_buf = init_meta.SerializeToString()
        return _common_pack(message.frame_head, stream_buf)

    cpdef bytes encode_data_frame(self, TrpcMessage message, bytes body):
        """Pack a DATA frame; ``body`` is the business payload.

        Args:
            message: Supplies ``frame_head`` (type / state / stream id).
            body: Already serialized (and optionally compressed) body.

        Returns:
            Complete stream frame with ``head_len`` written as 0.
        """
        return _common_pack(message.frame_head, body)

    cpdef bytes encode_close_frame(self, TrpcMessage message, bytes body):
        """Pack a CLOSE frame from ``message.stream_frame``.

        ``body`` is unused. Raises ``ClientEncodeError`` (historical
        server-side naming) when ``stream_frame`` is not a
        ``trpc_pb2.TrpcStreamCloseMeta``.

        Args:
            message: Must already hold a ``trpc_pb2.TrpcStreamCloseMeta``.
            body: Unused; kept for dispatch-signature symmetry.

        Returns:
            Complete stream frame with ``head_len`` written as 0.

        Raises:
            ClientEncodeError: ``stream_frame`` is not a
                ``trpc_pb2.TrpcStreamCloseMeta``.
        """
        # close_frame type: trpc_pb2.TrpcStreamCloseMeta
        close_frame = message.stream_frame
        if not isinstance(message.stream_frame, trpc_pb2.TrpcStreamCloseMeta):
            raise ClientEncodeError("client encode stream frame type invalid")

        stream_buf = close_frame.SerializeToString()
        return _common_pack(message.frame_head, stream_buf)

    cpdef bytes encode_feedback_frame(self, TrpcMessage message, bytes body):
        """Pack a FEEDBACK frame from ``message.stream_frame``.

        ``body`` is unused. Raises ``ClientEncodeError`` (historical
        server-side naming) when ``stream_frame`` is not a
        ``trpc_pb2.TrpcStreamFeedBackMeta``.

        Args:
            message: Must already hold a ``trpc_pb2.TrpcStreamFeedBackMeta``.
            body: Unused; kept for dispatch-signature symmetry.

        Returns:
            Complete stream frame with ``head_len`` written as 0.

        Raises:
            ClientEncodeError: ``stream_frame`` is not a
                ``trpc_pb2.TrpcStreamFeedBackMeta``.
        """
        # feedback_frame type: trpc_pb2.TrpcStreamFeedBackMeta
        feedback_frame = message.stream_frame
        if not isinstance(message.stream_frame, trpc_pb2.TrpcStreamFeedBackMeta):
            raise ClientEncodeError("client encode stream frame type invalid")

        stream_buf = feedback_frame.SerializeToString()
        return _common_pack(message.frame_head, stream_buf)

    cpdef bytes decode_init_frame(self, TrpcMessage message, bytes body):
        """Parse a client INIT and fill caller / callee / func / dyeing.

        ``body`` is the complete frame. Stores the meta on
        ``message.stream_frame`` so
        :meth:`trpc_sdk.codec.ServerCodec.store_init_meta` can cache
        it by ``session_id + stream_id`` for later DATA / CLOSE.
        Returns ``b''``.

        Args:
            message: Fresh server stream state to fill.
            body: Complete stream frame, including the 16-byte head.

        Returns:
            Empty bytes; the meta is on ``message.stream_frame``.
        """
        init_meta = trpc_pb2.TrpcStreamInitMeta()
        init_meta.ParseFromString(body[FRAME_HEAD_LEN:])
        message.set_stream_id(message.frame_head.stream_id)

        req = init_meta.request_meta
        message.set_caller_service_name(req.caller.decode('utf8'))
        message.set_callee_service_name(req.callee.decode('utf8'))
        message.set_server_rpc_name(req.func.decode('utf8'))

        message.set_compress_type(init_meta.content_encoding)
        message.set_serialization_type(init_meta.content_type) 
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

        message.set_stream_frame(init_meta)
        return b''

    cpdef bytes decode_data_frame(self, TrpcMessage message, bytes body):
        """Strip the 16-byte head and return the DATA payload.

        Also copies ``frame_head.stream_id`` onto the message so the
        INIT cache lookup can find ``server_rpc_name``.

        Args:
            message: Server stream state.
            body: Complete stream frame, including the 16-byte head.

        Returns:
            Business body after the frame head.
        """
        message.set_stream_id(message.frame_head.stream_id)
        return body[FRAME_HEAD_LEN:]

    cpdef bytes decode_close_frame(self, TrpcMessage message, bytes body):
        """Parse a CLOSE meta; RESET or nonzero ``ret`` becomes an error.

        Writes ``TrpcErrorType.CALLEE_FRAMEWORK`` onto ``client_rsp_err``
        (historical field name on the server path). Returns ``b''``.

        Args:
            message: Server stream state to fill.
            body: Complete stream frame, including the 16-byte head.

        Returns:
            Empty bytes; the meta is on ``message.stream_frame``.
        """
        close = trpc_pb2.TrpcStreamCloseMeta()
        close.ParseFromString(body[FRAME_HEAD_LEN:])
        if close.close_type == trpc_pb2.TRPC_STREAM_RESET or close.ret != 0:
            message.set_client_rsp_err(TrpcException(TrpcErrorType.CALLEE_FRAMEWORK, close.ret, close.msg))

        message.set_stream_frame(close)
        return b''

    cpdef bytes decode_feedback_frame(self, TrpcMessage message, bytes body):
        """Parse a FEEDBACK meta and store it on ``message.stream_frame``.

        Args:
            message: Server stream state to fill.
            body: Complete stream frame, including the 16-byte head.

        Returns:
            Empty bytes; the meta is on ``message.stream_frame``.
        """
        feedback = trpc_pb2.TrpcStreamFeedBackMeta()
        feedback.ParseFromString(body[FRAME_HEAD_LEN:])
        message.set_stream_frame(feedback)
        return b''
 

cpdef bytes _common_pack(TrpcFrameHead frame_head, bytes body):
    """Hand-pack a stream frame: 16-byte head plus body, ``head_len`` = 0.

    Offset 8 is written as ``0``, not ``htons(len(protocol_head))``.
    Stream INIT / CLOSE / FEEDBACK meta is the body; there is no
    separate ``RequestProtocol`` / ``ResponseProtocol``. That is the
    difference from :func:`trpc_sdk.codec._cython._codec._common_pack`.
    ``PyMem_Malloc`` is not null-checked (historical).

    Args:
        frame_head: Host-endian type / state / stream id / reserved.
        body: Serialized stream meta, or the DATA business body.

    Returns:
        Complete stream frame bytes.
    """
    cdef uint body_len = len(body)
    cdef uint total_len = FRAME_HEAD_LEN + body_len

    cdef char* to_prt = <char *> PyMem_Malloc(total_len)
    cdef ushort magic = trpc_pb2.TRPC_MAGIC_VALUE
    (<ushort *>(to_prt))[0] = htons(magic)
    (<uchar *>(to_prt+2))[0] = frame_head.frame_type
    (<uchar *>(to_prt+3))[0] = frame_head.frame_state
    (<uint *>(to_prt+4))[0] = htonl(total_len)
    (<ushort *>(to_prt+8))[0] = 0
    (<uint *>(to_prt+10))[0] = htonl(frame_head.stream_id)
    (<ushort *>(to_prt+14))[0] = htons(frame_head.frame_reserved)

    cdef char* body_ptr = body
    memcpy(to_prt+FRAME_HEAD_LEN, body_ptr, body_len)

    results = bytes(to_prt[:total_len])

    PyMem_Free(to_prt)

    return results
