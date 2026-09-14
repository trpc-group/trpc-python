# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Type hints for the codec Message and 16-byte tRPC frame head.

``CFrameHead`` is the public tRPC wire head, not the transport IPC
envelope. ``Message`` holds a ``TrpcContext`` back-reference. Server
receive uses :func:`create_message`; client stubs use :func:`clone_message`;
stream frames use :func:`clone_context_and_message`.
"""

from __future__ import annotations

from typing import Any
from typing import Tuple

from trpc_sdk.abc import TrpcContext


class CFrameHead:
    """In-memory tRPC wire frame head (16 bytes on the wire).

    Fields are host-endian. ``parse_frame_head`` and encode paths convert
    with ``ntohs`` / ``ntohl`` / ``htons`` / ``htonl``.
    """

    stx: int
    frame_type: int
    frame_state: int
    package_len: int
    head_len: int
    stream_id: int
    frame_reserved: int

    def __init__(self) -> None:
        """Zero-fill every field."""
        ...

    def set_stx(self, stx: int) -> None:
        """Store the 2-byte magic (``TRPC_MAGIC_VALUE`` on a valid frame)."""
        ...

    def set_frame_type(self, frame_type: int) -> None:
        """Store unary vs stream (``TRPC_UNARY_FRAME`` / ``TRPC_STREAM_FRAME``)."""
        ...

    def set_frame_state(self, frame_state: int) -> None:
        """Store the stream sub-type; unused (0) on unary frames."""
        ...

    def set_package_len(self, package_len: int) -> None:
        """Store the total frame length, including this 16-byte head."""
        ...

    def set_head_len(self, head_len: int) -> None:
        """Store the protocol-header length, not the 16-byte frame head."""
        ...

    def set_stream_id(self, stream_id: int) -> None:
        """Store the stream id; 0 on unary frames."""
        ...

    def set_frame_reserved(self, frame_reserved: int) -> None:
        """Store the unused reserved field (normally 0)."""
        ...


class FrameHead(CFrameHead):
    """Historical Python alias of ``CFrameHead``; behavior is identical."""
    ...


def create_frame_head(frame_type: int, frame_state: int, stream_id: int) -> CFrameHead:
    """Build a frame head with type / state / stream id only.

    ``stx``, ``package_len``, and ``head_len`` stay 0 until encode fills them.
    """
    ...


def parse_frame_head(data: bytes) -> CFrameHead:
    """Decode the first 16 bytes of a tRPC frame into host-endian fields.

    Callers must pass at least 16 bytes; length is not checked here.
    """
    ...


def get_app_server_service(service_name: str) -> Tuple[str, str, str]:
    """Split a service name into ``(app, server, service)``.

    Canonical form is ``trpc.{app}.{server}.{service}``.
    """
    ...


def get_method_from_rpc_func(rpc_name: str) -> str:
    """Extract the method from ``/trpc.{app}.{server}.{service}/{method}``.

    Non-canonical names are returned unchanged.
    """
    ...


class Message:
    """Per-RPC codec state hung off ``TrpcContext``.

    Writing fields directly skips the derived-field sync that the
    ``set_*`` methods perform.
    """

    context: Any  # trpc_sdk.abc.TrpcContext; back-ref, TrpcContext.msg points here
    frame_head: CFrameHead
    timeout: int  # milliseconds
    serialization_type: int
    compress_type: int
    stream_id: int
    dyeing: bool
    dyeing_key: str
    server_rpc_name: str
    client_rpc_name: str
    server_meta_data: Any
    client_meta_data: Any
    caller_service_name: str
    callee_service_name: str
    server_rsp_err: Any
    client_rsp_err: Any
    server_req_head: Any
    server_rsp_head: Any
    client_req_head: Any
    client_rsp_head: Any
    local_addr: str
    remote_addr: str
    logger: str
    caller_app: str
    caller_server: str
    caller_service: str
    caller_method: str
    callee_app: str
    callee_server: str
    callee_service: str
    callee_method: str
    namespace: str
    env_name: str
    set_name: str
    env_transfer: str
    callee_container_name: str
    callee_set_name: str
    stream_frame: Any
    request_id: int
    session_id: int  # transport session, not request_id
    request_attachment: bytes
    response_attachment: bytes

    def __init__(self) -> None:
        """Zero the message and install default serializer / compressor."""
        ...

    def set_frame_head(self, frame_head: CFrameHead) -> None: ...

    def set_timeout(self, timeout: int) -> None:
        """Store the request timeout in milliseconds."""
        ...

    def set_serialization_type(self, serialization_type: int) -> None: ...

    def set_compress_type(self, compress_type: int) -> None: ...

    def set_stream_id(self, stream_id: int) -> None: ...

    def set_dyeing(self, dyeing: bool) -> None: ...

    def set_dyeing_key(self, dyeing_key: str) -> None: ...

    def set_server_rpc_name(self, server_rpc_name: str) -> None:
        """Store the inbound RPC name and sync ``callee_method``."""
        ...

    def set_client_rpc_name(self, client_rpc_name: str) -> None:
        """Store the outbound RPC name and sync ``callee_method``."""
        ...

    def set_server_meta_data(self, server_meta_data: Any) -> None: ...

    def set_client_meta_data(self, client_meta_data: Any) -> None: ...

    def set_caller_service_name(self, caller_service_name: str) -> None:
        """Store the caller name and parse app / server / service unless ``*``."""
        ...

    def set_callee_service_name(self, callee_service_name: str) -> None:
        """Store the callee name and parse app / server / service unless ``*``."""
        ...

    def set_server_rsp_err(self, server_rsp_err: Any) -> None: ...

    def set_client_rsp_err(self, client_rsp_err: Any) -> None: ...

    def set_server_req_head(self, server_req_head: Any) -> None: ...

    def set_server_rsp_head(self, server_rsp_head: Any) -> None: ...

    def set_client_req_head(self, client_req_head: Any) -> None: ...

    def set_client_rsp_head(self, client_rsp_head: Any) -> None: ...

    def set_local_addr(self, local_addr: str) -> None: ...

    def set_remote_addr(self, remote_addr: str) -> None: ...

    def set_logger(self, logger: str) -> None: ...

    def set_caller_app(self, caller_app: str) -> None: ...

    def set_caller_server(self, caller_server: str) -> None: ...

    def set_caller_service(self, caller_service: str) -> None: ...

    def set_caller_method(self, caller_method: str) -> None: ...

    def set_callee_app(self, callee_app: str) -> None: ...

    def set_callee_server(self, callee_server: str) -> None: ...

    def set_callee_service(self, callee_service: str) -> None: ...

    def set_callee_method(self, callee_method: str) -> None: ...

    def set_namespace(self, namespace: str) -> None: ...

    def set_env_name(self, env_name: str) -> None: ...

    def set_setname(self, set_name: str) -> None:
        """Store the logical deployment set name on ``set_name``."""
        ...

    def set_env_transfer(self, env_transfer: str) -> None: ...

    def set_callee_container_name(self, callee_container_name: str) -> None: ...

    def set_callee_set_name(self, callee_set_name: str) -> None: ...

    def set_stream_frame(self, stream_frame: Any) -> None: ...

    def set_request_id(self, request_id: int) -> None: ...

    def set_session_id(self, session_id: int) -> None:
        """Store the transport session id, not ``request_id``."""
        ...

    def set_request_attachment(self, attachment: bytes) -> None: ...

    def set_response_attachment(self, attachment: bytes) -> None: ...

    def __repr__(self) -> str: ...


def clone_context_and_message(ctx: TrpcContext) -> Tuple[TrpcContext, Message]:
    """Clone context and message for a stream frame.

    Keeps caller/callee identity. Used by stream client and server.
    """
    ...


def clone_message(ctx: TrpcContext) -> Tuple[TrpcContext, Message]:
    """Clone context and message for an outbound client RPC.

    Turns the old callee into the new caller. Each client-stub call
    needs a new message.
    """
    ...


def copy_common_message(msg: Message, new_msg: Message) -> None:
    """Copy timeout, names, dyeing, env, attachments, and server heads."""
    ...


def copy_server_to_client_message(msg: Message, new_msg: Message) -> None:
    """Flip identity for an outbound call: old callee becomes new caller."""
    ...


def copy_server_to_server_message(msg: Message, new_msg: Message) -> None:
    """Copy caller and callee identity without flipping them."""
    ...


def clone_client_message(ctx: TrpcContext) -> Tuple[TrpcContext, Message]:
    """Thin wrapper around :func:`clone_message` for client stubs."""
    ...


def get_message(ctx: TrpcContext) -> Message:
    """Return ``ctx.msg``, creating a ``Message`` if it is missing."""
    ...


def create_message(ctx: TrpcContext) -> Tuple[TrpcContext, Message]:
    """Attach a fresh empty message to ``ctx`` (server receive entry).

    Always overwrites ``ctx.msg``.

    Args:
        ctx: Server request context.

    Returns:
        The same context paired with the new ``Message``.
    """
    ...
