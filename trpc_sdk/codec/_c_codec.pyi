# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Type hints for the unary Cython client / server codec.

Python :class:`~trpc_sdk.codec.ClientCodec` / ``ServerCodec`` dispatch
unary frames here. Stream frames use :mod:`trpc_sdk.codec._c_codec_stream`.
"""

from __future__ import annotations

from ._message import CFrameHead
from ._message import Message


class CClientCodec:
    """Client-side unary codec: request encode and response decode.

    ``request_id`` is a per-instance counter incremented on each
    ``encode``. It wraps at ``UINT32_MAX_VALUE`` and is not unique
    across processes or codec instances.
    """

    request_id: int

    def encode(self, message: Message, body: bytes) -> bytes:
        """Pack a client request body into a unary tRPC frame.

        If ``message.client_req_head`` is empty, a deepcopy of
        ``server_req_head`` is used when it is a ``RequestProtocol``.

        Args:
            message: Outbound RPC state; fields are copied into the
                request head.
            body: Already serialized (and optionally compressed) body.

        Returns:
            Complete unary frame including the 16-byte head.

        Raises:
            ClientEncodeError: ``client_req_head`` is set but is not a
                ``RequestProtocol``.
        """
        ...

    def decode(self, message: Message, buffer: bytes) -> bytes:
        """Unpack a unary response frame into the message and body.

        Args:
            message: Client RPC state to fill from the response.
            buffer: Complete unary frame, including the 16-byte head.

        Returns:
            Response body between the protocol head and the attachment.

        Raises:
            ClientDecodeError: Frame shorter than 16 bytes, or an
                invalid ``client_rsp_head`` type.
        """
        ...

    def _unpack(self, rsp: object, message: Message, buffer: bytes) -> bytes:
        """Parse the frame head, ``ResponseProtocol``, body, and attachment.

        Both ``rsp.ret`` and ``rsp.func_ret`` become
        ``TrpcErrorType.BUSINESS`` on ``client_rsp_err``.

        Args:
            rsp: ``ResponseProtocol`` instance filled in place.
            message: Client RPC state updated with codec fields.
            buffer: Complete unary frame, including the 16-byte head.
        """
        ...

    def update_msg_by_msg(self, origin_message: Message, target_message: Message) -> None:
        """Copy a decoded response from a temporary message onto the request.

        Equivalent to the second half of ``_unpack`` without touching
        the body. Used on the multiplexed receive path.

        Args:
            origin_message: Temporary message that already ran decode.
            target_message: The in-flight request message to update.
        """
        ...


class CServerCodec:
    """Server-side unary codec: request decode and response encode."""

    def encode(self, message: Message, body: bytes) -> bytes:
        """Pack a server response body into a unary tRPC frame.

        ``TrpcErrorType.BUSINESS`` writes ``func_ret``; any other type
        writes ``ret``.

        Args:
            message: Inbound RPC state plus the handler error.
            body: Already serialized (and optionally compressed) body.
        """
        ...

    def decode(self, message: Message, buffer: bytes) -> bytes:
        """Unpack a unary request frame and seed the response head.

        Copies ``version`` / ``call_type`` / ``request_id`` /
        ``message_type`` / content types from the request so encode
        can reply with matching fields.

        Args:
            message: Fresh server RPC state to fill.
            buffer: Complete unary frame, including the 16-byte head.

        Returns:
            Request body between the protocol head and the attachment.
        """
        ...

    def _unpack(self, req: object, message: Message, buffer: bytes) -> bytes:
        """Parse the frame head, ``RequestProtocol``, body, and attachment.

        Args:
            req: ``RequestProtocol`` instance filled in place.
            message: Server RPC state; receives the frame head and
                request attachment.
            buffer: Complete unary frame, including the 16-byte head.
        """
        ...


def _common_pack(
    req_head: bytes,
    frame_head: CFrameHead,
    body: bytes,
    attachment: bytes = b"",
) -> bytes:
    """Hand-pack a unary frame: 16-byte head + protocol head + body + attachment.

    Multi-byte fields are written big-endian. Attachment is copied only
    when non-empty.

    Args:
        req_head: Serialized ``RequestProtocol`` or ``ResponseProtocol``.
        frame_head: Host-endian type / state / stream id / reserved.
        body: Serialized (and optionally compressed) RPC body.
        attachment: Optional trailing attachment; default empty.
    """
    ...
