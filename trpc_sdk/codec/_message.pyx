# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

#cython: language_level=3, boundscheck=False, wraparound=False

"""In-memory tRPC frame head and per-RPC message used by codec encode/decode.

``CFrameHead`` is the public 16-byte tRPC wire head, not the 6-byte
transport IPC envelope. ``Message`` hangs off ``TrpcContext``: the server
entry uses :func:`create_message`, client stubs use :func:`clone_message`,
and stream frames use :func:`clone_context_and_message`. Assigning
fields directly does not run the derived-field setters.
"""

import copy
from typing import Tuple
from cython.operator cimport dereference

from trpc_sdk.abc._cython cimport TrpcContext
from trpc_sdk.exceptions import TrpcErrorCode
from trpc_sdk.exceptions import create_frame_error
from ._types import CompressType
from ._types import SerializationType


cdef ushort endian_byte_swapper_ushort(ushort src):
    """Swap a 16-bit value between big and little endian."""
    return  (src >> 8) | (src << 8)


cdef uint endian_byte_swapper_uint(uint src):
    """Swap a 32-bit value between big and little endian."""
    return (src & 0x000000FFU) << 24 | (src & 0x0000FF00U) << 8 | (src & 0x00FF0000U) >> 8 | (src & 0xFF000000U) >> 24


cdef class CFrameHead:
    """In-memory tRPC wire frame head (16 bytes on the wire).

    Layout on the wire, big-endian for multi-byte fields::

        [stx:2][frame_type:1][frame_state:1][package_len:4]
        [head_len:2][stream_id:4][frame_reserved:2]

    Fields here are host-endian integers. ``parse_frame_head`` and the
    encode paths convert with ``ntohs`` / ``ntohl`` / ``htons`` /
    ``htonl``. This is the public tRPC head, not the 6-byte transport
    IPC envelope.

    ``cpdef`` setters exist so Python and Cython callers write the same
    fields; they store the host value as-is and do not swap endianness.
    """

    def __init__(self):
        """Zero-fill every field.

        Encode paths overwrite ``stx``, ``package_len``, and ``head_len``
        when the frame is packed. Unary frames leave ``stream_id`` and
        ``frame_state`` at 0.
        """
        self.stx = 0              # 2-byte magic; encode writes TRPC_MAGIC_VALUE
        self.frame_type = 0       # unary vs stream (TrpcDataFrameType)
        self.frame_state = 0      # stream sub-type; stays 0 on unary
        self.package_len = 0      # total frame size, including this 16-byte head
        self.head_len = 0         # protocol-header size after the frame head
        self.stream_id = 0        # stream identifier; stays 0 on unary
        self.frame_reserved = 0   # unused reserved field

    cpdef void set_stx(self, ushort stx):
        """Set the 2-byte magic (``TRPC_MAGIC_VALUE`` on a valid frame).

        A mismatch on decode closes the connection.

        Args:
            stx: Host-endian magic.
        """
        self.stx = stx

    cpdef void set_frame_type(self, uchar frame_type):
        """Set unary vs stream (``TRPC_UNARY_FRAME`` / ``TRPC_STREAM_FRAME``).

        Stream frames then use ``frame_state`` for INIT / DATA / CLOSE /
        FEEDBACK.

        Args:
            frame_type: ``TrpcDataFrameType`` value.
        """
        self.frame_type = frame_type

    cpdef void set_frame_state(self, uchar frame_state):
        """Set the stream sub-type; unused (0) on unary frames.

        Args:
            frame_state: ``TrpcStreamFrameType`` value, or 0.
        """
        self.frame_state = frame_state

    cpdef void set_stream_id(self, uint stream_id):
        """Set the stream id; 0 on unary frames.

        Args:
            stream_id: Host-endian stream identifier.
        """
        self.stream_id = stream_id

    cpdef void set_package_len(self, uint package_len):
        """Set the total frame length, including this 16-byte head.

        Parsers read 16 bytes first, then ``package_len - 16`` more.

        Args:
            package_len: Host-endian total size in bytes.
        """
        self.package_len = package_len

    cpdef void set_head_len(self, ushort head_len):
        """Set the protocol-header length, not the 16-byte frame head.

        This is the size of ``RequestProtocol`` / ``ResponseProtocol`` /
        stream meta that follows the frame head.

        Args:
            head_len: Host-endian protocol-header size in bytes.
        """
        self.head_len = head_len

    cpdef void set_frame_reserved(self, ushort frame_reserved):
        """Set the unused reserved field (normally 0).

        Args:
            frame_reserved: Host-endian reserved value.
        """
        self.frame_reserved = frame_reserved


cpdef CFrameHead create_frame_head(uchar frame_type, uchar frame_state, uint stream_id):
    """Build a frame head with type / state / stream id only.

    ``stx``, ``package_len``, and ``head_len`` stay 0 until encode fills
    them.

    Args:
        frame_type: ``TrpcDataFrameType`` value.
        frame_state: ``TrpcStreamFrameType`` value, or 0 on unary.
        stream_id: Stream identifier; 0 on unary.

    Returns:
        A zeroed ``CFrameHead`` with those three fields set.
    """
    frame_head = CFrameHead()
    frame_head.frame_type = frame_type
    frame_head.frame_state = frame_state
    frame_head.stream_id = stream_id
    return frame_head


cpdef CFrameHead parse_frame_head(bytes data):
    """Decode the first 16 bytes of a tRPC frame into host-endian fields.

    Callers must pass at least 16 bytes; this function does not check
    length. Multi-byte fields are converted with ``ntohs`` / ``ntohl``.

    Args:
        data: Frame bytes; only ``data[:16]`` is read.

    Returns:
        A ``CFrameHead`` with host-endian integers.
    """
    frame_head = CFrameHead()
    cdef char* from_prt = data
    frame_head.stx = ntohs(dereference(<ushort *>(from_prt)))
    frame_head.frame_type = dereference(<uchar *>(from_prt+2))
    frame_head.frame_state = dereference(<uchar *>(from_prt+3))
    frame_head.package_len = ntohl(dereference(<uint *>(from_prt+4)))
    frame_head.head_len = ntohs(dereference(<ushort *>(from_prt+8)))
    frame_head.stream_id = ntohl(dereference(<uint *>(from_prt+10)))
    frame_head.frame_reserved = ntohs(dereference(<ushort *>(from_prt+14)))

    return frame_head


class FrameHead(CFrameHead):
    """Historical Python alias of ``CFrameHead``; behavior is identical."""
    pass

def get_app_server_service(service_name: str):
    """Split a service name into app, server, and service parts.

    The canonical form is ``trpc.{app}.{server}.{service}``. More than
    three dotted segments drop the ``trpc`` prefix and join the rest as
    the service. One segment returns ``(service_name, "", "")``. Two or
    three segments index the string itself (``service_name[0]``), not
    the split parts.

    Args:
        service_name: Fully qualified or short service name.

    Returns:
        A ``(app, server, service)`` tuple.
    """
    all_names = service_name.split(".")
    len_names = len(all_names)
    if len_names > 3:
        return all_names[1], all_names[2], '.'.join(all_names[3:])
    if len_names == 1:
        return service_name, "", ""
    if len_names == 2:
        return service_name[0], service_name[1], ""
    if len_names == 3:
        return service_name[0], service_name[1], service_name[2]

def get_method_from_rpc_func(rpc_name: str):
    """Extract the method from a canonical RPC name.

    The canonical form is ``/trpc.{app}.{server}.{service}/{method}``.
    Anything else is returned unchanged.

    Args:
        rpc_name: RPC name, usually from ``server_rpc_name`` or
            ``client_rpc_name``.

    Returns:
        The method segment, or ``rpc_name`` if the form does not match.
    """
    all_names = rpc_name.split("/")
    len_names = len(all_names)
    if rpc_name[0] != "/" or len_names != 3 or all_names[1].count(".") != 3:
        return rpc_name
    return all_names[2]

cdef class Message:
    """Per-RPC codec state hung off ``TrpcContext``.

    Server receive uses :func:`create_message`. Each outbound client call
    uses :func:`clone_message` (callee becomes the new caller). Stream
    frames use :func:`clone_context_and_message` and keep caller/callee.
    Writing fields directly skips the derived-field sync that the
    ``set_*`` methods perform.
    """
    def __init__(self):
        """Zero the message and install default serializer / compressor.

        ``server_rsp_err`` starts as a success ``create_frame_error`` so an
        untouched server path still encodes a valid response status.
        """
        self.context = None  # filled by create_message / clone_* / get_message
        self.frame_head = CFrameHead()
        self.timeout = 0  # milliseconds
        self.serialization_type = SerializationType.PB
        self.compress_type = CompressType.Noop
        self.stream_id = 0
        self.dyeing = False
        self.dyeing_key = ""
        self.server_rpc_name = ""
        self.client_rpc_name = ""
        self.server_meta_data = {}
        self.client_meta_data = {}
        self.caller_service_name = ""
        self.callee_service_name = ""
        self.server_rsp_err = create_frame_error(TrpcErrorCode.TRPC_INVOKE_SUCCESS, "")
        self.client_rsp_err = None
        self.server_req_head = None
        self.server_rsp_head = None
        self.client_req_head = None
        self.client_rsp_head = None
        self.local_addr = ""
        self.remote_addr = ""
        self.logger = ""
        self.caller_app = ""
        self.caller_server = ""
        self.caller_service = ""
        self.caller_method = ""
        self.callee_app = ""
        self.callee_server = ""
        self.callee_service = ""
        self.callee_method = ""
        self.namespace = ""
        self.env_name = ""
        self.set_name = ""
        self.env_transfer = ""
        self.callee_container_name = ""
        self.callee_set_name = ""
        self.request_id = 0

        self.stream_frame = None
        self.session_id = 0
        self.request_attachment = b""
        self.response_attachment = b""

    cpdef void set_frame_head(self, CFrameHead frame_head):
        """Replace the 16-byte tRPC frame head.

        Args:
            frame_head: Host-endian ``CFrameHead``.
        """
        self.frame_head = frame_head

    cpdef void set_timeout(self, int timeout):
        """Store the request timeout in milliseconds.

        Args:
            timeout: Timeout in milliseconds.
        """
        self.timeout = timeout

    cpdef void set_serialization_type(self, int serialization_type):
        """Store the body serializer id (``SerializationType``).

        Args:
            serialization_type: Serializer identifier.
        """
        self.serialization_type = serialization_type

    cpdef void set_compress_type(self, int compress_type):
        """Store the body compressor id (``CompressType``).

        Args:
            compress_type: Compressor identifier.
        """
        self.compress_type = compress_type

    cpdef void set_stream_id(self, uint stream_id):
        """Store the stream id; 0 on unary frames.

        Args:
            stream_id: Stream identifier.
        """
        self.stream_id = stream_id

    cpdef void set_dyeing(self, bint dyeing):
        """Enable or disable traffic dyeing for this RPC.

        Args:
            dyeing: True when the request is dyed.
        """
        self.dyeing = dyeing

    cpdef void set_dyeing_key(self, str dyeing_key):
        """Store the routing key used for dyed traffic.

        Args:
            dyeing_key: Dyeing key string.
        """
        self.dyeing_key = dyeing_key

    cpdef void set_server_rpc_name(self, str server_rpc_name):
        """Store the inbound RPC name and sync ``callee_method``.

        A matching value is a no-op. ``callee_method`` comes from
        :func:`get_method_from_rpc_func`.

        Args:
            server_rpc_name: RPC name observed by the server.
        """
        if self.server_rpc_name == server_rpc_name:
            return
        self.server_rpc_name = server_rpc_name
        cdef str callee_method = get_method_from_rpc_func(server_rpc_name)
        self.callee_method = callee_method

    cpdef void set_client_rpc_name(self, str client_rpc_name):
        """Store the outbound RPC name and sync ``callee_method``.

        Called from the client stub. A matching value is a no-op.

        Args:
            client_rpc_name: RPC name selected for the downstream call.
        """
        if self.client_rpc_name == client_rpc_name:
            return
        self.client_rpc_name = client_rpc_name
        self.callee_method = get_method_from_rpc_func(client_rpc_name)

    cpdef void set_server_meta_data(self, object server_meta_data):
        """Replace inbound / server-owned transmissive metadata.

        Args:
            server_meta_data: Protocol transmissive map, usually a dict.
        """
        self.server_meta_data = server_meta_data

    cpdef void set_client_meta_data(self, object client_meta_data):
        """Replace outbound / client-owned transmissive metadata.

        Args:
            client_meta_data: Protocol transmissive map, usually a dict.
        """
        self.client_meta_data = client_meta_data

    cpdef void set_caller_service_name(self, str caller_service_name):
        """Store the caller service name and parse app / server / service.

        A matching value is a no-op. ``"*"`` is stored as-is and does
        not update the parsed parts.

        Args:
            caller_service_name: Fully qualified upstream service name.
        """
        if caller_service_name == self.caller_service_name:
            return
        self.caller_service_name = caller_service_name
        if caller_service_name == "*":
            return
        app, server, service = get_app_server_service(caller_service_name)
        self.caller_app = app
        self.caller_server = server
        self.caller_service = service

    cpdef void set_callee_service_name(self, str callee_service_name):
        """Store the callee service name and parse app / server / service.

        A matching value is a no-op. ``"*"`` is stored as-is and does
        not update the parsed parts.

        Args:
            callee_service_name: Fully qualified downstream service name.
        """
        if callee_service_name == self.callee_service_name:
            return
        self.callee_service_name = callee_service_name
        if callee_service_name == "*":
            return
        app, server, service = get_app_server_service(callee_service_name)
        self.callee_app = app
        self.callee_server = server
        self.callee_service = service

    cpdef void set_server_rsp_err(self, object server_rsp_err):
        """Store the error encoded into the server response.

        Args:
            server_rsp_err: Usually a ``create_frame_error`` result.
        """
        self.server_rsp_err = server_rsp_err

    cpdef void set_client_rsp_err(self, object client_rsp_err):
        """Store the error returned by a downstream call.

        Typically set while the client decodes the response.

        Args:
            client_rsp_err: Downstream error, or None.
        """
        self.client_rsp_err = client_rsp_err

    cpdef void set_server_req_head(self, object server_req_head):
        """Store the protocol-specific inbound request header.

        Args:
            server_req_head: Decoded request protocol head.
        """
        self.server_req_head = server_req_head

    cpdef void set_server_rsp_head(self, object server_rsp_head):
        """Store the protocol-specific outbound response header.

        Args:
            server_rsp_head: Response protocol head sent upstream.
        """
        self.server_rsp_head = server_rsp_head

    cpdef void set_client_req_head(self, object client_req_head):
        """Store the protocol-specific downstream request header.

        Args:
            client_req_head: Request protocol head sent downstream.
        """
        self.client_req_head = client_req_head

    cpdef void set_client_rsp_head(self, object client_rsp_head):
        """Store the protocol-specific downstream response header.

        Args:
            client_rsp_head: Response protocol head from the callee.
        """
        self.client_rsp_head = client_rsp_head

    cpdef void set_local_addr(self, str local_addr):
        """Store the local network endpoint for this RPC.

        Args:
            local_addr: Local address string.
        """
        self.local_addr = local_addr

    cpdef void set_remote_addr(self, str remote_addr):
        """Store the peer network endpoint for this RPC.

        Args:
            remote_addr: Remote address string.
        """
        self.remote_addr = remote_addr

    cpdef void set_logger(self, str logger):
        """Store the logger category or identifier.

        Args:
            logger: Logger name.
        """
        self.logger = logger

    cpdef void set_caller_app(self, str caller_app):
        """Overwrite the parsed caller app without touching the full name.

        Args:
            caller_app: Caller application name.
        """
        self.caller_app = caller_app

    cpdef void set_caller_server(self, str caller_server):
        """Overwrite the parsed caller server without touching the full name.

        Args:
            caller_server: Caller server name.
        """
        self.caller_server = caller_server

    cpdef void set_caller_service(self, str caller_service):
        """Overwrite the parsed caller service without touching the full name.

        Args:
            caller_service: Caller service name.
        """
        self.caller_service = caller_service

    cpdef void set_caller_method(self, str caller_method):
        """Overwrite the caller method without touching the RPC name.

        Args:
            caller_method: Caller method name.
        """
        self.caller_method = caller_method

    cpdef void set_callee_app(self, str callee_app):
        """Overwrite the parsed callee app without touching the full name.

        Args:
            callee_app: Callee application name.
        """
        self.callee_app = callee_app

    cpdef void set_callee_server(self, str callee_server):
        """Overwrite the parsed callee server without touching the full name.

        Args:
            callee_server: Callee server name.
        """
        self.callee_server = callee_server

    cpdef void set_callee_service(self, str callee_service):
        """Overwrite the parsed callee service without touching the full name.

        Args:
            callee_service: Callee service name.
        """
        self.callee_service = callee_service

    cpdef void set_callee_method(self, str callee_method):
        """Overwrite the callee method without touching the RPC name.

        Args:
            callee_method: Callee method name.
        """
        self.callee_method = callee_method

    cpdef void set_namespace(self, str namespace):
        """Store the deployment namespace.

        Args:
            namespace: Namespace string.
        """
        self.namespace = namespace

    cpdef void set_env_name(self, str env_name):
        """Store the deployment environment name.

        Args:
            env_name: Environment name.
        """
        self.env_name = env_name

    cpdef void set_setname(self, str set_name):
        """Store the logical deployment set name on ``set_name``.

        Args:
            set_name: Set name written to ``self.set_name``.
        """
        self.set_name = set_name

    cpdef void set_env_transfer(self, str env_transfer):
        """Store the environment-transfer routing value.

        Args:
            env_transfer: Environment-transfer string.
        """
        self.env_transfer = env_transfer

    cpdef void set_callee_container_name(self, str callee_container_name):
        """Store the target container name.

        Args:
            callee_container_name: Container name.
        """
        self.callee_container_name = callee_container_name

    cpdef void set_callee_set_name(self, str callee_set_name):
        """Store the target deployment set name.

        Args:
            callee_set_name: Callee set name.
        """
        self.callee_set_name = callee_set_name

    cpdef void set_stream_frame(self, object stream_frame):
        """Store the protocol-specific streaming frame payload.

        Args:
            stream_frame: Stream frame object, or None on unary.
        """
        self.stream_frame = stream_frame

    cpdef void set_request_id(self, uint request_id):
        """Store the RPC request correlation id.

        Args:
            request_id: Request identifier.
        """
        self.request_id = request_id

    cpdef void set_session_id(self, int session_id):
        """Store the transport session id, not ``request_id``.

        Args:
            session_id: Transport session identifier.
        """
        self.session_id = session_id

    cpdef void set_request_attachment(self, bytes attachment):
        """Store the opaque request attachment bytes.

        Args:
            attachment: Request attachment.
        """
        self.request_attachment = attachment

    cpdef void set_response_attachment(self, bytes attachment):
        """Store the opaque response attachment bytes.

        Args:
            attachment: Response attachment.
        """
        self.response_attachment = attachment

    def __repr__(self):
        """Return the callee service name and downstream response head."""
        return "%s %s" % (self.callee_service_name, self.client_rsp_head)

def clone_context_and_message(TrpcContext ctx) -> Tuple[TrpcContext, Message]:
    """Clone context and message for a stream frame.

    Copies common fields and keeps caller/callee identity via
    :func:`copy_server_to_server_message`. If ``ctx.msg`` is not a
    ``Message``, the new pair is empty.

    Args:
        ctx: Source context.

    Returns:
        A shallow-copied context paired with a new ``Message``.
    """
    new_ctx = copy.copy(ctx)
    new_msg = Message()
    new_ctx.msg = new_msg
    new_msg.context = new_ctx

    if not isinstance(ctx.msg, Message):
        return new_ctx, new_msg

    copy_common_message(ctx.msg, new_msg)
    copy_server_to_server_message(ctx.msg, new_msg)
    return new_ctx, new_msg


cpdef clone_message(TrpcContext ctx):
    """Clone context and message for an outbound client RPC.

    Each client-stub call needs a new message. Common fields are copied,
    then :func:`copy_server_to_client_message` turns the old callee into
    the new caller. If ``ctx.msg`` is not a ``Message``, the new message
    still points ``context`` at the original ``ctx``.

    Args:
        ctx: Source context.

    Returns:
        A shallow-copied context paired with a new ``Message``.
    """
    new_ctx = copy.copy(ctx)
    new_msg = Message()
    if not isinstance(ctx.msg, Message):
        new_ctx.msg = new_msg
        new_msg.context = ctx
        return new_ctx, new_msg

    new_ctx.msg = new_msg
    new_msg.context = new_ctx
    copy_common_message(ctx.msg, new_msg)
    copy_server_to_client_message(ctx.msg, new_msg)
    return new_ctx, new_msg


def copy_common_message(Message msg, Message new_msg):
    """Copy timeout, names, dyeing, env, attachments, and server heads.

    Does not copy caller/callee identity, client metadata, or errors.

    Args:
        msg: Source message.
        new_msg: Destination message.
    """
    new_msg.frame_head = msg.frame_head
    new_msg.timeout = msg.timeout
    new_msg.serialization_type = msg.serialization_type
    new_msg.server_rpc_name = msg.server_rpc_name
    new_msg.client_rpc_name = msg.client_rpc_name
    new_msg.server_req_head = msg.server_req_head
    new_msg.server_rsp_head = msg.server_rsp_head
    new_msg.dyeing = msg.dyeing
    new_msg.dyeing_key = msg.dyeing_key
    new_msg.server_meta_data = msg.server_meta_data
    new_msg.logger = msg.logger
    new_msg.namespace = msg.namespace
    new_msg.env_name = msg.env_name
    new_msg.set_name = msg.set_name
    new_msg.env_transfer = msg.env_transfer
    new_msg.session_id = msg.session_id
    new_msg.request_attachment = msg.request_attachment
    new_msg.response_attachment = msg.response_attachment


def copy_server_to_client_message(msg: Message, new_msg: Message):
    """Flip identity for an outbound call: old callee becomes new caller.

    Also copies ``server_meta_data`` into ``client_meta_data``.

    Args:
        msg: Inbound / current message.
        new_msg: Outbound message being prepared.
    """
    new_msg.client_meta_data = msg.server_meta_data.copy()
    # Old callee becomes the new caller of the downstream RPC.
    new_msg.caller_service_name = msg.callee_service_name
    new_msg.caller_app = msg.callee_app
    new_msg.caller_server = msg.callee_server
    new_msg.caller_service = msg.callee_service
    new_msg.caller_method = msg.callee_method


def copy_server_to_server_message(msg: Message, new_msg: Message):
    """Copy caller and callee identity without flipping them.

    Used by stream clones so successive frames keep the same names.

    Args:
        msg: Source message.
        new_msg: Destination message.
    """
    new_msg.caller_service_name = msg.caller_service_name
    new_msg.caller_app = msg.caller_app
    new_msg.caller_server = msg.caller_server
    new_msg.caller_service = msg.caller_service
    new_msg.caller_method = msg.caller_method

    new_msg.callee_service_name = msg.callee_service_name
    new_msg.callee_app = msg.callee_app
    new_msg.callee_server = msg.callee_server
    new_msg.callee_service = msg.callee_service
    new_msg.callee_method = msg.callee_method


cpdef clone_client_message(ctx: TrpcContext):
    """Thin wrapper around :func:`clone_message` for client stubs.

    Args:
        ctx: Source context.

    Returns:
        The same pair as :func:`clone_message`.
    """
    new_ctx, new_msg = clone_message(ctx)
    return new_ctx, new_msg


cpdef Message get_message(ctx: TrpcContext):
    """Return ``ctx.msg``, creating a ``Message`` if it is missing.

    Args:
        ctx: TrpcContext that should own the message.

    Returns:
        The existing or newly created ``Message``.
    """
    if not isinstance(ctx.msg, Message):
        ctx.msg = Message()
        ctx.msg.context = ctx
    return ctx.msg

def create_message(TrpcContext ctx):
    """Attach a fresh empty message to ``ctx`` (server receive entry).

    Always overwrites ``ctx.msg``.

    Args:
        ctx: Server request context.

    Returns:
        The same context paired with the new ``Message``.
    """
    message = Message()
    ctx.msg = message
    message.context = ctx
    return ctx, message
