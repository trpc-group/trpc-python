# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Type hints for the stream-frame Cython client / server codec.

Python :class:`~trpc_sdk.codec.ClientCodec` / ``ServerCodec`` dispatch
INIT / DATA / CLOSE / FEEDBACK here. Unary frames use
:mod:`trpc_sdk.codec._c_codec`.
"""

from __future__ import annotations

from ._message import CFrameHead
from ._message import Message


class CClientStreamCodec:
    """Client-side stream codec: INIT / DATA / CLOSE / FEEDBACK."""

    def encode_init_frame(self, message: Message, body: bytes) -> bytes:
        """Pack a client INIT frame from ``message.stream_frame``.

        ``body`` is unused; the payload is serialized
        ``TrpcStreamInitMeta``.

        Args:
            message: Outbound stream state; request_meta is filled from
                caller / callee / func / trans_info / dyeing / env.
            body: Unused; kept so the four encode methods share a
                signature for :mod:`trpc_sdk.codec._codec` dispatch.
        """
        ...

    def encode_data_frame(self, message: Message, body: bytes) -> bytes:
        """Pack a DATA frame; ``body`` is the business payload.

        Args:
            message: Supplies ``frame_head`` (type / state / stream id).
            body: Already serialized (and optionally compressed) body.
        """
        ...

    def encode_close_frame(self, message: Message, body: bytes) -> bytes:
        """Pack a CLOSE frame from ``message.stream_frame``.

        ``body`` is unused; the payload is serialized
        ``TrpcStreamCloseMeta``.

        Args:
            message: Must already hold a ``TrpcStreamCloseMeta``.
            body: Unused; kept for dispatch-signature symmetry.
        """
        ...

    def encode_feedback_frame(self, message: Message, body: bytes) -> bytes:
        """Pack a FEEDBACK frame from ``message.stream_frame``.

        ``body`` is unused; the payload is serialized
        ``TrpcStreamFeedBackMeta``.

        Args:
            message: Must already hold a ``TrpcStreamFeedBackMeta``.
            body: Unused; kept for dispatch-signature symmetry.
        """
        ...

    def decode_init_frame(self, message: Message, body: bytes) -> bytes:
        """Parse a server INIT reply into compress / serialize / error.

        ``body`` is the complete frame. Nonzero ``response_meta.ret``
        becomes ``TrpcErrorType.CALLEE_FRAMEWORK``. Returns ``b''``.

        Args:
            message: Client stream state to fill.
            body: Complete stream frame, including the 16-byte head.
        """
        ...

    def decode_data_frame(self, message: Message, body: bytes) -> bytes:
        """Strip the 16-byte head and return the DATA payload.

        Args:
            message: Client stream state.
            body: Complete stream frame, including the 16-byte head.
        """
        ...

    def decode_close_frame(self, message: Message, body: bytes) -> bytes:
        """Parse a CLOSE meta; RESET or nonzero ``ret`` becomes an error.

        Args:
            message: Client stream state to fill.
            body: Complete stream frame, including the 16-byte head.
        """
        ...

    def decode_feedback_frame(self, message: Message, body: bytes) -> bytes:
        """Parse a FEEDBACK meta and store it on ``message.stream_frame``.

        Args:
            message: Client stream state to fill.
            body: Complete stream frame, including the 16-byte head.
        """
        ...


class CServerStreamCodec:
    """Server-side stream codec: INIT / DATA / CLOSE / FEEDBACK."""

    def encode_init_frame(self, message: Message, body: bytes) -> bytes:
        """Pack a server INIT reply from ``message.stream_frame``.

        ``body`` is unused. ``TrpcErrorType.BUSINESS`` writes ``func_ret``;
        any other type writes ``ret``.

        Args:
            message: Inbound stream state plus the handler error.
            body: Unused; kept for dispatch-signature symmetry.
        """
        ...

    def encode_data_frame(self, message: Message, body: bytes) -> bytes:
        """Pack a DATA frame; ``body`` is the business payload.

        Args:
            message: Supplies ``frame_head`` (type / state / stream id).
            body: Already serialized (and optionally compressed) body.
        """
        ...

    def encode_close_frame(self, message: Message, body: bytes) -> bytes:
        """Pack a CLOSE frame from ``message.stream_frame``.

        Raises ``ClientEncodeError`` (historical server-side naming)
        when ``stream_frame`` is not a ``TrpcStreamCloseMeta``.

        Args:
            message: Must already hold a ``TrpcStreamCloseMeta``.
            body: Unused; kept for dispatch-signature symmetry.
        """
        ...

    def encode_feedback_frame(self, message: Message, body: bytes) -> bytes:
        """Pack a FEEDBACK frame from ``message.stream_frame``.

        Raises ``ClientEncodeError`` (historical server-side naming)
        when ``stream_frame`` is not a ``TrpcStreamFeedBackMeta``.

        Args:
            message: Must already hold a ``TrpcStreamFeedBackMeta``.
            body: Unused; kept for dispatch-signature symmetry.
        """
        ...

    def decode_init_frame(self, message: Message, body: bytes) -> bytes:
        """Parse a client INIT and fill caller / callee / func / dyeing.

        Later DATA / CLOSE frames recover ``server_rpc_name`` from the
        INIT cache in :meth:`trpc_sdk.codec.ServerCodec.set_init_meta`.

        Args:
            message: Fresh server stream state to fill.
            body: Complete stream frame, including the 16-byte head.
        """
        ...

    def decode_data_frame(self, message: Message, body: bytes) -> bytes:
        """Strip the 16-byte head and return the DATA payload.

        Args:
            message: Server stream state.
            body: Complete stream frame, including the 16-byte head.
        """
        ...

    def decode_close_frame(self, message: Message, body: bytes) -> bytes:
        """Parse a CLOSE meta; RESET or nonzero ``ret`` becomes an error.

        Writes onto ``client_rsp_err`` (historical field name on the
        server path).

        Args:
            message: Server stream state to fill.
            body: Complete stream frame, including the 16-byte head.
        """
        ...

    def decode_feedback_frame(self, message: Message, body: bytes) -> bytes:
        """Parse a FEEDBACK meta and store it on ``message.stream_frame``.

        Args:
            message: Server stream state to fill.
            body: Complete stream frame, including the 16-byte head.
        """
        ...


def _common_pack(frame_head: CFrameHead, body: bytes) -> bytes:
    """Hand-pack a stream frame: 16-byte head plus body, ``head_len`` = 0.

    Stream meta lives in the body slot. That is the difference from
    :func:`trpc_sdk.codec._c_codec._common_pack`.

    Args:
        frame_head: Host-endian type / state / stream id / reserved.
        body: Serialized stream meta, or the DATA business body.
    """
    ...
