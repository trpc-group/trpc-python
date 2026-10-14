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

Protocol parsers and the ``get_*`` plugin lookups live in
:mod:`trpc_sdk.codec._factory`.
"""

import threading
from typing import Dict

from trpc_sdk._inner import trpc_pb2
from trpc_sdk.abc import BaseCodec
from trpc_sdk.abc import PluginInitPos
from trpc_sdk.abc import PluginType
from trpc_sdk.abc import TrpcContext
from trpc_sdk.abc import TrpcFrameHead
from trpc_sdk.abc import TrpcMessage
from trpc_sdk.abc import get_message
from trpc_sdk.abc import parse_frame_head
from trpc_sdk.exceptions import ClientDecodeError
from trpc_sdk.exceptions import ServerDecodeError
from trpc_sdk.exceptions import ServerSystemError
from trpc_sdk.plugin import register_plugin
from trpc_sdk.telemetry import report_attr
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
    """

    def __init__(self):
        super().__init__()
        self.codec = _TrpcClientCodec()
        self.stream_codec = _TrpcClientStreamCodec()

    def encode_data(self, context: TrpcContext, body: bytes) -> bytes:
        """Dispatch unary or stream encode for an outbound client frame.

        Unary frames go to ``self.codec``. Stream frames go to
        :meth:`encode_stream`.

        Args:
            context: Owns the message whose ``frame_head.frame_type``
                selects the path.
            body: Already serialized (and optionally compressed) body.

        Returns:
            Complete tRPC frame including the 16-byte head.

        Raises:
            ValueError: Stream ``frame_state`` is not INIT / DATA / CLOSE /
                FEEDBACK.
        """
        if get_message(context).frame_head.frame_type != trpc_pb2.TRPC_UNARY_FRAME:
            return self.encode_stream(context, body)

        return self.codec.encode_data(context, body)

    def encode_stream(self, context: TrpcContext, body: bytes) -> bytes:
        """Pack one client stream frame via ``self.stream_codec``.

        Args:
            context: Owns the message whose ``frame_head.frame_state``
                selects INIT / DATA / CLOSE / FEEDBACK.
            body: Used for DATA; INIT / CLOSE / FEEDBACK read
                ``message.stream_frame`` instead.

        Returns:
            Complete stream frame including the 16-byte head.

        Raises:
            ValueError: ``frame_state`` is not a known stream sub-type.
        """
        return self.stream_codec.encode_stream(context, body)

    def decode_data(self, context: TrpcContext, body: bytes) -> bytes:
        """Dispatch unary or stream decode for an inbound client frame.

        The first 16 bytes are parsed so a stream reply can set
        ``frame_head`` / ``stream_id`` before :meth:`decode_stream`.
        Unary frames go to ``self.codec``.

        Args:
            context: Owns the message updated from the frame head and
                protocol.
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
            message = get_message(context)
            message.set_frame_head(frame_head)
            message.set_stream_id(frame_head.stream_id)
            return self.decode_stream(context, body)
        return self.codec.decode_data(context, body)

    def decode_stream(self, context: TrpcContext, body: bytes) -> bytes:
        """Unpack one client stream frame via ``self.stream_codec``.

        Args:
            context: Owns the message whose ``frame_head.frame_state``
                must already be set (see :meth:`decode_data`).
            body: Complete stream frame, head included.

        Returns:
            Stream payload after the 16-byte head, or empty bytes for
            INIT / CLOSE / FEEDBACK after meta is stored on the message.

        Raises:
            ValueError: ``frame_state`` is not a known stream sub-type.
        """
        return self.stream_codec.decode_stream(context, body)


@register_plugin(PluginType.SERVER_CODEC, "trpc", init_pos=PluginInitPos.WORKER)
class TrpcServerCodec(BaseCodec):
    """Server-side ``"trpc"`` codec: unary vs stream dispatch.

    Owns no packing logic. Unary frames go to ``self.codec``
    (:class:`_TrpcServerCodec`); stream frames go to ``self.stream_codec``
    (:class:`_TrpcServerStreamCodec`). INIT meta is cached here per
    ``session_id`` / ``stream_id`` so later DATA / CLOSE / FEEDBACK
    frames can restore ``server_rpc_name``. The stream dispatcher drops an
    entry once both sides have closed the stream, and a whole session with
    :meth:`drop_session` when its connection goes away; encoding the
    server's own CLOSE keeps it, since the client's CLOSE still has to be
    decoded.

    The cache is one map per thread (per serving loop), like the stream
    dispatcher's map: all frames of a stream are decoded on one loop.

    The meta-cache helpers stay message-first; only the four codec hooks
    take the context.
    """

    def __init__(self):
        super().__init__()
        self.codec = _TrpcServerCodec()
        self.stream_codec = _TrpcServerStreamCodec()
        self._local = threading.local()

    @property
    def init_metas(self) -> Dict[int, Dict[int, StreamInitMeta]]:
        """``session_id -> stream_id -> INIT meta`` of the calling thread; created on first use."""
        metas = getattr(self._local, "init_metas", None)
        if metas is None:
            metas = {}
            self._local.init_metas = metas
        return metas

    @init_metas.setter
    def init_metas(self, value: Dict[int, Dict[int, StreamInitMeta]]) -> None:
        self._local.init_metas = value

    def store_init_meta(self, msg: TrpcMessage, init_meta: StreamInitMeta):
        """Cache INIT meta for later DATA / CLOSE / FEEDBACK frames.

        Nested as ``init_metas[session_id][stream_id]``. Created on
        inbound INIT decode.

        Args:
            msg: Supplies ``session_id`` and ``stream_id``.
            init_meta: Parsed ``TrpcStreamInitMeta`` from the INIT frame.
        """
        self.init_metas.setdefault(msg.session_id, {})[msg.stream_id] = init_meta

    def delete_init_meta(self, msg: TrpcMessage):
        """Drop the INIT cache entry for this session/stream.

        No-op when the entry is already gone. An empty session map
        is removed so closed connections do not leak keys.

        Args:
            msg: Supplies ``session_id`` and ``stream_id``.
        """
        init_metas = self.init_metas
        session = init_metas.get(msg.session_id)
        if session is None:
            return

        session.pop(msg.stream_id, None)
        if not session:
            init_metas.pop(msg.session_id)

    def drop_session(self, session_id: int) -> None:
        """Drop every INIT cache entry of one connection on the calling thread.

        Args:
            session_id: Connection id whose streams are gone.
        """
        self.init_metas.pop(session_id, None)

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
        session = self.init_metas.get(session_id)
        if session is None:
            # Expected for frames still in flight when the connection was aborted.
            report_attr("trpc_stream_frame_not_found", 1)
            raise ServerSystemError(f"codec stream closed, session_id: {session_id} stream_id: {stream_id}")

        if stream_id not in session:
            report_attr("trpc_stream_frame_not_found", 1)
            raise ServerSystemError(f"codec invalid stream session_id: {session_id} stream_id: {stream_id}")

        init_meta: StreamInitMeta = session[stream_id]
        msg.server_rpc_name = init_meta.request_meta.func.decode('utf8')

    def encode_data(self, context: TrpcContext, body: bytes) -> bytes:
        """Dispatch unary or stream encode for an outbound server frame.

        Unary frames go to ``self.codec``. Stream frames go to
        :meth:`encode_stream`.

        Args:
            context: Owns the message whose ``frame_head.frame_type``
                selects the path.
            body: Already serialized (and optionally compressed) body.

        Returns:
            Complete tRPC frame including the 16-byte head.

        Raises:
            ValueError: Stream ``frame_state`` is not INIT / DATA / CLOSE /
                FEEDBACK.
        """
        if get_message(context).frame_head.frame_type != trpc_pb2.TRPC_UNARY_FRAME:
            return self.encode_stream(context, body)

        return self.codec.encode_data(context, body)

    def encode_stream(self, context: TrpcContext, body: bytes) -> bytes:
        """Pack one server stream frame via ``self.stream_codec``.

        Args:
            context: Owns the message whose ``frame_head.frame_state``
                selects INIT / DATA / CLOSE / FEEDBACK.
            body: Used for DATA; INIT / CLOSE / FEEDBACK read
                ``message.stream_frame`` instead.

        Returns:
            Complete stream frame including the 16-byte head.

        Raises:
            ValueError: ``frame_state`` is not a known stream sub-type.
        """
        return self.stream_codec.encode_stream(context, body)

    async def decode_data(self, context: TrpcContext, body: bytes) -> bytes:
        """Dispatch unary or stream decode for an inbound server frame.

        Declared ``async`` to match the server codec plugin contract;
        the body is already a complete frame, so nothing is awaited.
        Stream frames set ``frame_head`` / ``stream_id`` then call
        :meth:`decode_stream`.

        Args:
            context: Owns the message updated from the frame head and
                protocol.
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
            message = get_message(context)
            message.set_frame_head(frame_head)
            message.set_stream_id(frame_head.stream_id)
            return self.decode_stream(context, body)

        return self.codec.decode_data(context, body)

    def decode_stream(self, context: TrpcContext, body: bytes) -> bytes:
        """Unpack one server stream frame via ``self.stream_codec``.

        INIT stores ``message.stream_frame`` in the cache. DATA / CLOSE /
        FEEDBACK restore ``server_rpc_name`` via :meth:`set_init_meta`.

        Args:
            context: Owns the message whose ``frame_head.frame_state``
                must already be set (see :meth:`decode_data`).
            body: Complete stream frame, head included.

        Returns:
            Stream payload after the 16-byte head, or empty bytes for
            INIT / CLOSE / FEEDBACK after meta is applied.

        Raises:
            ServerSystemError: DATA / CLOSE / FEEDBACK with no cached INIT.
            ValueError: ``frame_state`` is not a known stream sub-type.
        """
        rsp = self.stream_codec.decode_stream(context, body)
        message = get_message(context)
        if message.frame_head.frame_state == trpc_pb2.TRPC_STREAM_FRAME_INIT:
            self.store_init_meta(message, message.stream_frame)
        else:
            self.set_init_meta(message)
        return rsp
