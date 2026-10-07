# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Unary/stream dispatch layer for the ``"trpc"`` codec plugins.

``TrpcClientCodec`` and ``TrpcServerCodec`` own no packing logic of their own.
Each subclasses :class:`~trpc_sdk.abc.BaseCodec` only and holds two Cython
codecs by composition: ``self.codec`` for unary frames
(:mod:`trpc_sdk.codec._cython._codec`) and ``self.stream_codec`` for stream
frames (:mod:`trpc_sdk.codec._cython._codec_stream`). This layer reads
``frame_head.frame_type`` to choose between them; the stream codec branches
on ``frame_state`` itself. The server INIT meta cache lives here.

Protocol checkers and the ``get_*`` plugin lookups live in
:mod:`trpc_sdk.codec._factory`.
"""

from typing import Dict

from trpc_sdk._inner import trpc_pb2
from trpc_sdk.abc import BaseCodec
from trpc_sdk.abc import PluginInitPos
from trpc_sdk.abc import PluginType
from trpc_sdk.abc import TrpcFrameHead
from trpc_sdk.abc import TrpcMessage
from trpc_sdk.abc import parse_frame_head
from trpc_sdk.exceptions import ClientDecodeError
from trpc_sdk.exceptions import ServerDecodeError
from trpc_sdk.exceptions import ServerSystemError
from trpc_sdk.plugin import register_plugin
from ._constants import FRAME_HEAD_LEN
from ._cython import _TrpcClientCodec
from ._cython import _TrpcClientStreamCodec
from ._cython import _TrpcServerCodec
from ._cython import _TrpcServerStreamCodec
from ._types import StreamInitMeta


@register_plugin(PluginType.CLIENT_CODEC, "trpc", init_pos=PluginInitPos.WORKER)
class TrpcClientCodec(BaseCodec):
    """Client-side ``"trpc"`` codec: unary vs stream dispatch.

    Owns no packing logic. ``frame_head.frame_type`` selects
    ``self.codec`` (:class:`_TrpcClientCodec`, unary) or ``self.stream_codec``
    (:class:`_TrpcClientStreamCodec`, INIT / DATA / CLOSE / FEEDBACK).
    ``request_id`` lives on ``self.codec``, not on this plugin.

    :meth:`encode` / :meth:`decode` stay message-first. Callers use the
    :class:`BaseCodec` ``encode_data`` / ``decode_data`` entry points,
    which pull the message from the context.
    """

    def __init__(self):
        super().__init__()
        self.codec = _TrpcClientCodec()
        self.stream_codec = _TrpcClientStreamCodec()

    def encode(self, message: TrpcMessage, body: bytes) -> bytes:
        """Dispatch unary or stream encode for an outbound client frame.

        Unary frames go to ``self.codec``. Stream frames go to
        :meth:`encode_stream`.

        Args:
            message: RPC state; ``frame_head.frame_type`` selects the path.
            body: Already serialized (and optionally compressed) body.

        Returns:
            Complete tRPC frame including the 16-byte head.

        Raises:
            ValueError: Stream ``frame_state`` is not INIT / DATA / CLOSE /
                FEEDBACK.
        """
        if message.frame_head.frame_type != trpc_pb2.TRPC_UNARY_FRAME:
            return self.encode_stream(message, body)

        return self.codec.encode(message, body)

    def encode_stream(self, message: TrpcMessage, body: bytes) -> bytes:
        """Pack one client stream frame via ``self.stream_codec``.

        Args:
            message: RPC state; ``frame_head.frame_state`` selects INIT /
                DATA / CLOSE / FEEDBACK.
            body: Used for DATA; INIT / CLOSE / FEEDBACK read
                ``message.stream_frame`` instead.

        Returns:
            Complete stream frame including the 16-byte head.

        Raises:
            ValueError: ``frame_state`` is not a known stream sub-type.
        """
        return self.stream_codec.encode_stream(message, body)

    def decode(self, message: TrpcMessage, body: bytes) -> bytes:
        """Dispatch unary or stream decode for an inbound client frame.

        The first 16 bytes are parsed so a stream reply can set
        ``frame_head`` / ``stream_id`` before :meth:`decode_stream`.
        Unary frames go to ``self.codec``.

        Args:
            message: RPC state updated from the frame head and protocol.
            body: Complete frame bytes, head included.

        Returns:
            Decoded application body (unary) or stream payload.

        Raises:
            ClientDecodeError: Frame shorter than 16 bytes.
            ValueError: Stream ``frame_state`` is not INIT / DATA / CLOSE /
                FEEDBACK.
        """
        if len(body) < FRAME_HEAD_LEN:
            raise ClientDecodeError("client decode rsp buf len invalid")

        frame_head: TrpcFrameHead = parse_frame_head(body[:16])
        if frame_head.frame_type != trpc_pb2.TRPC_UNARY_FRAME:
            message.set_frame_head(frame_head)
            message.set_stream_id(frame_head.stream_id)
            return self.decode_stream(message, body)
        return self.codec.decode(message, body)

    def decode_stream(self, message: TrpcMessage, body: bytes) -> bytes:
        """Unpack one client stream frame via ``self.stream_codec``.

        Args:
            message: RPC state; ``frame_head.frame_state`` must already
                be set (see :meth:`decode`).
            body: Complete stream frame, head included.

        Returns:
            Stream payload after the 16-byte head, or empty bytes for
            INIT / CLOSE / FEEDBACK after meta is stored on ``message``.

        Raises:
            ValueError: ``frame_state`` is not a known stream sub-type.
        """
        return self.stream_codec.decode_stream(message, body)


@register_plugin(PluginType.SERVER_CODEC, "trpc", init_pos=PluginInitPos.WORKER)
class TrpcServerCodec(BaseCodec):
    """Server-side ``"trpc"`` codec: unary vs stream dispatch.

    Owns no packing logic. Unary frames go to ``self.codec``
    (:class:`_TrpcServerCodec`); stream frames go to ``self.stream_codec``
    (:class:`_TrpcServerStreamCodec`). INIT meta is cached here per
    ``session_id`` / ``stream_id`` so later DATA / CLOSE / FEEDBACK
    frames can restore ``server_rpc_name``. A successful CLOSE encode
    drops that cache entry.

    :meth:`encode` / :meth:`decode` stay message-first, as do the
    meta-cache helpers. Callers use the :class:`BaseCodec`
    ``encode_data`` / ``decode_data`` entry points.
    """

    def __init__(self):
        super().__init__()
        self.codec = _TrpcServerCodec()
        self.stream_codec = _TrpcServerStreamCodec()
        self.init_metas: Dict[int, Dict[int, StreamInitMeta]] = {}

    def store_init_meta(self, msg: TrpcMessage, init_meta: StreamInitMeta):
        """Cache INIT meta for later DATA / CLOSE / FEEDBACK frames.

        Nested as ``init_metas[session_id][stream_id]``. Created on
        inbound INIT decode.

        Args:
            msg: Supplies ``session_id`` and ``stream_id``.
            init_meta: Parsed ``TrpcStreamInitMeta`` from the INIT frame.
        """
        stream_id = msg.stream_id
        session_id = msg.session_id
        if session_id not in self.init_metas:
            self.init_metas[session_id] = {}

        self.init_metas[session_id][stream_id] = init_meta

    def delete_init_meta(self, msg: TrpcMessage):
        """Drop the INIT cache entry for this session/stream.

        No-op when the session is already gone. An empty session map
        is removed so closed connections do not leak keys.

        Args:
            msg: Supplies ``session_id`` and ``stream_id``.
        """
        stream_id = msg.stream_id
        session_id = msg.session_id
        if session_id not in self.init_metas:
            return

        self.init_metas[session_id].pop(stream_id)
        if len(self.init_metas[session_id]) == 0:
            self.init_metas.pop(session_id)

    def set_init_meta(self, msg: TrpcMessage):
        """Restore ``server_rpc_name`` from the cached INIT meta.

        Args:
            msg: Supplies ``session_id`` / ``stream_id``; ``server_rpc_name``
                is overwritten from ``request_meta.func``.

        Raises:
            ServerSystemError: Session is gone (stream already closed)
                or this ``stream_id`` was never cached.
        """
        stream_id = msg.stream_id
        session_id = msg.session_id
        if session_id not in self.init_metas:
            raise ServerSystemError(f"codec stream closed, session_id: {session_id} stream_id: {stream_id}")

        if stream_id not in self.init_metas[session_id]:
            raise ServerSystemError(f"codec invalid stream session_id: {session_id} stream_id: {stream_id}")

        init_meta: StreamInitMeta = self.init_metas[session_id][stream_id]
        msg.server_rpc_name = init_meta.request_meta.func.decode('utf8')

    def encode(self, message: TrpcMessage, body: bytes) -> bytes:
        """Dispatch unary or stream encode for an outbound server frame.

        Unary frames go to ``self.codec``. Stream frames go to
        :meth:`encode_stream`.

        Args:
            message: RPC state; ``frame_head.frame_type`` selects the path.
            body: Already serialized (and optionally compressed) body.

        Returns:
            Complete tRPC frame including the 16-byte head.

        Raises:
            ValueError: Stream ``frame_state`` is not INIT / DATA / CLOSE /
                FEEDBACK.
        """
        if message.frame_head.frame_type != trpc_pb2.TRPC_UNARY_FRAME:
            return self.encode_stream(message, body)

        return self.codec.encode(message, body)

    def encode_stream(self, message: TrpcMessage, body: bytes) -> bytes:
        """Pack one server stream frame via ``self.stream_codec``.

        A successful CLOSE pack also calls :meth:`delete_init_meta`.

        Args:
            message: RPC state; ``frame_head.frame_state`` selects INIT /
                DATA / CLOSE / FEEDBACK.
            body: Used for DATA; INIT / CLOSE / FEEDBACK read
                ``message.stream_frame`` instead.

        Returns:
            Complete stream frame including the 16-byte head.

        Raises:
            ValueError: ``frame_state`` is not a known stream sub-type.
        """
        rsp = self.stream_codec.encode_stream(message, body)
        if message.frame_head.frame_state == trpc_pb2.TRPC_STREAM_FRAME_CLOSE:
            self.delete_init_meta(message)
        return rsp

    async def decode(self, message: TrpcMessage, body: bytes) -> bytes:
        """Dispatch unary or stream decode for an inbound server frame.

        Declared ``async`` to match the server codec plugin contract;
        the body is already a complete frame, so nothing is awaited.
        Stream frames set ``frame_head`` / ``stream_id`` then call
        :meth:`decode_stream`.

        Args:
            message: RPC state updated from the frame head and protocol.
            body: Complete frame bytes, head included.

        Returns:
            Decoded application body (unary) or stream payload.

        Raises:
            ServerDecodeError: Frame shorter than 16 bytes.
            ValueError: Stream ``frame_state`` is not INIT / DATA / CLOSE /
                FEEDBACK.
        """
        if len(body) < FRAME_HEAD_LEN:
            raise ServerDecodeError("server decode req buf len invalid")

        frame_head: TrpcFrameHead = parse_frame_head(body[:16])
        if frame_head.frame_type != trpc_pb2.TRPC_UNARY_FRAME:
            message.set_frame_head(frame_head)
            message.set_stream_id(frame_head.stream_id)
            return self.decode_stream(message, body)

        return self.codec.decode(message, body)

    def decode_stream(self, message: TrpcMessage, body: bytes) -> bytes:
        """Unpack one server stream frame via ``self.stream_codec``.

        INIT stores ``message.stream_frame`` in the cache. DATA / CLOSE /
        FEEDBACK restore ``server_rpc_name`` via :meth:`set_init_meta`.

        Args:
            message: RPC state; ``frame_head.frame_state`` must already
                be set (see :meth:`decode`).
            body: Complete stream frame, head included.

        Returns:
            Stream payload after the 16-byte head, or empty bytes for
            INIT / CLOSE / FEEDBACK after meta is applied.

        Raises:
            ServerSystemError: DATA / CLOSE / FEEDBACK with no cached INIT.
            ValueError: ``frame_state`` is not a known stream sub-type.
        """
        rsp = self.stream_codec.decode_stream(message, body)
        if message.frame_head.frame_state == trpc_pb2.TRPC_STREAM_FRAME_INIT:
            self.store_init_meta(message, message.stream_frame)
        else:
            self.set_init_meta(message)
        return rsp
