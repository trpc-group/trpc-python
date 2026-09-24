# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Inner task-stream encode / decode between a worker and a task child.

This is the process-internal stream protocol registered as
``inner_task_stream``, used over the Unix-domain socket that pairs a
worker with a :class:`~trpc_sdk.server._task.TaskProcess`. It is **not**
the public TCP stream path in :mod:`trpc_sdk.codec._cython._codec_stream`.

The on-wire layout is still the 16-byte tRPC frame head, and the magic
equals ``trpc_pb2.TRPC_MAGIC_VALUE`` (aliased here as ``TASK_STREAM_MAGIC_VALUE``).
Four ``frame_state`` values after a stream ``frame_type``:

* INIT carries ``trpc_pb2.TrpcStreamInitMeta`` (func / content_type / encoding).
* DATA carries the business body only.
* CLOSE carries ``trpc_pb2.TrpcStreamCloseMeta``.
* FEEDBACK carries ``trpc_pb2.TrpcStreamFeedBackMeta``.

Packing is hand-rolled Python. Like
:func:`trpc_sdk.codec._cython._codec_stream._common_pack`, the ``head_len`` slot at
offset 8 is left unwritten: stream meta lives in the body slot, with no
separate ``RequestProtocol`` / ``ResponseProtocol``.

Codecs interpret frames. Parsers only cut complete frames: the client
parser reads asynchronously from a
:class:`~trpc_sdk.transport.PooledConnection`;
the server parser scans a buffer for magic + ``package_len``.

The server codec caches INIT meta by ``session_id + stream_id`` so later
DATA / CLOSE / FEEDBACK frames can recover ``server_rpc_name``. That
cache is dropped when the server encodes CLOSE.
"""

from __future__ import annotations

__all__ = ['TaskStreamClientCodec', 'TaskStreamServerCodec', 'TaskStreamClientParser', 'TaskStreamServerParser']

import socket
import struct
import sys
from abc import abstractmethod
from collections import defaultdict
from typing import TYPE_CHECKING
from typing import Callable
from typing import Dict
from typing import List

from google.protobuf.message import Message as PbMessage

from trpc_sdk._inner import trpc_pb2
from trpc_sdk.abc import BaseClientProtocolChecker
from trpc_sdk.abc import BaseCodec
from trpc_sdk.abc import PluginInitPos
from trpc_sdk.abc import PluginType
from trpc_sdk.abc import TrpcFrameHead
from trpc_sdk.abc import TrpcMessage
from trpc_sdk.abc import parse_frame_head
from trpc_sdk.plugin import register_plugin
from ._constants import FRAME_HEAD_LEN
from ._constants import INNER_TASK_STREAM
from ._constants import TASK_STREAM_MAGIC_VALUE
from ._exceptions import TaskStreamClientDecodeError
from ._exceptions import TaskStreamClientEncodeError
from ._exceptions import TaskStreamClientReadFrameError
from ._exceptions import TaskStreamServerDecodeError
from ._exceptions import TaskStreamServerEncodeError
from ._types import StreamInitMeta
from ._types import TaskStreamCodecRole

if TYPE_CHECKING:
    from trpc_sdk.transport import PooledConnection


def _fill_field(src: bytes, dst: bytearray, index: int) -> int:
    """Copy ``src`` into ``dst`` starting at ``index``.

    ``_common_pack`` pre-allocates a padded buffer and writes each head
    field in place. Returning the next write index lets callers chain
    fills without tracking offsets themselves.

    Args:
        src: Field bytes already in on-wire order.
        dst: Destination buffer; must be large enough for ``index + len(src)``.
        index: First byte to overwrite.

    Returns:
        The index immediately after the last written byte.
    """
    offset = 0
    for data in src:
        dst[index + offset] = data
        offset = offset + 1
    return offset + index


def _common_pack(frame_head: TrpcFrameHead, body: bytes):
    """Hand-pack a stream frame: 16-byte head plus body.

    ``zfill(total_len)`` prepends padding so the body sits at offset 16
    and the head can be written over the prefix. Python ``zfill`` pads
    with ASCII ``'0'`` (``0x30``), not NUL. The skipped ``head_len``
    slot therefore stays ``0x30``. Stream decode never reads
    ``head_len``; it always slices ``body[FRAME_HEAD_LEN:]``.

    ``htons`` / ``htonl`` produce a network-endian integer; writing that
    integer with ``to_bytes(..., sys.byteorder)`` then yields big-endian
    on the wire on both little-endian and big-endian hosts.

    Offset 8 (2 bytes) is the tRPC ``head_len`` slot. It is skipped so
    the remaining fields stay aligned with the 16-byte frame head, the
    same layout as :func:`trpc_sdk.codec._cython._codec_stream._common_pack`.

    Args:
        frame_head: Host-endian type / state / stream id / reserved.
        body: Serialized stream meta, or the DATA business body. A
            ``str`` is encoded as UTF-8 for historical callers.

    Returns:
        Complete stream frame bytes.

    Raises:
        TypeError: ``body`` is neither ``bytes`` nor ``str``.
    """
    total_len = FRAME_HEAD_LEN + len(body)
    if isinstance(body, bytes):
        results = bytearray(body).zfill(total_len)
    elif isinstance(body, str):
        results = bytearray(body, 'utf-8').zfill(total_len)
    else:
        raise TypeError("encode stream body is not 'bytes'")
    magic = socket.htons(TASK_STREAM_MAGIC_VALUE).to_bytes(2, sys.byteorder)
    index = _fill_field(magic, results, 0)
    index = _fill_field(frame_head.frame_type.to_bytes(1, sys.byteorder), results, index)
    index = _fill_field(frame_head.frame_state.to_bytes(1, sys.byteorder), results, index)
    total_len = socket.htonl(total_len).to_bytes(4, sys.byteorder)
    index = _fill_field(total_len, results, index)
    # Skip the 2-byte head_len slot so stream_id stays at offset 10,
    # matching the tRPC 16-byte frame head. Left unwritten (zfill padding).
    index = index + 2
    stream_id = socket.htonl(frame_head.stream_id).to_bytes(4, sys.byteorder)
    index = _fill_field(stream_id, results, index)
    if frame_head.frame_reserved:
        # Write reserved only when non-zero; the two bytes are already
        # the zfill padding when the value is the default 0.
        frame_reserved = socket.htonl(frame_head.frame_reserved).to_bytes(2, sys.byteorder)
        _fill_field(frame_reserved, results, index)
    return bytes(results)


class _TaskStreamCodec(BaseCodec):
    """Private shared INIT / DATA / CLOSE / FEEDBACK encode and decode.

    A :class:`~trpc_sdk.abc.BaseCodec`. :meth:`encode` / :meth:`decode`
    and the eight ``_encode_*`` / ``_decode_*`` helpers stay
    message-first; the context-first entry points are the base class
    ``encode_data`` / ``decode_data``. Not
    :class:`~trpc_sdk.abc.TaskCodec` (task body pickle). Callers
    construct :class:`TaskStreamClientCodec` or
    :class:`TaskStreamServerCodec`; this base only shares the dispatch
    maps. INIT encode is abstract: the client fills ``request_meta``,
    the server fills ``response_meta``.

    ``_encode_err_cls`` / ``_decode_err_cls`` / ``_role`` default to the
    client side so shared methods can format error text with
    ``role.name``. The server subclass overwrites all three.

    ``_init_metas[session_id][stream_id]`` is populated only on server
    INIT decode. DATA / CLOSE / FEEDBACK recover ``server_rpc_name``
    from that cache; CLOSE encode drops the entry.
    """

    def __init__(self):
        # BaseCodec.__init__ reaches BasePlugin and sets ``_name``.
        super().__init__()
        self._encode_map = {
            trpc_pb2.TRPC_STREAM_FRAME_INIT: self._encode_init_frame,
            trpc_pb2.TRPC_STREAM_FRAME_DATA: self._encode_data_frame,
            trpc_pb2.TRPC_STREAM_FRAME_CLOSE: self._encode_close_frame,
            trpc_pb2.TRPC_STREAM_FRAME_FEEDBACK: self._encode_feedback_frame
        }
        self._decode_map = {
            trpc_pb2.TRPC_STREAM_FRAME_INIT: self._decode_init_frame,
            trpc_pb2.TRPC_STREAM_FRAME_DATA: self._decode_data_frame,
            trpc_pb2.TRPC_STREAM_FRAME_CLOSE: self._decode_close_frame,
            trpc_pb2.TRPC_STREAM_FRAME_FEEDBACK: self._decode_feedback_frame
        }
        self._encode_err_cls: Callable[[str], Exception] = TaskStreamClientEncodeError
        self._decode_err_cls: Callable[[str], Exception] = TaskStreamClientDecodeError
        self._role = TaskStreamCodecRole.CLIENT
        self._init_metas: Dict[int, Dict[int, StreamInitMeta]] = defaultdict(dict)

    @abstractmethod
    def _encode_init_frame(self, message: TrpcMessage, body: bytes) -> bytes:
        """Pack an INIT frame from ``message.stream_frame``.

        ``body`` is unused; the payload is serialized
        ``trpc_pb2.TrpcStreamInitMeta``. The signature stays aligned with
        ``_encode_data_frame`` so :meth:`encode` can dispatch all four
        states the same way.

        Args:
            message: Outbound stream state; client fills request_meta,
                server fills response_meta.
            body: Unused; kept for a shared encode signature.

        Returns:
            Complete stream frame with the 16-byte head.
        """

    def _encode_data_frame(self, message: TrpcMessage, body: bytes) -> bytes:
        """Pack a DATA frame: frame head plus the business body.

        Unlike INIT / CLOSE / FEEDBACK, DATA has no stream-meta proto;
        ``body`` is the already-serialized (and maybe compressed) RPC
        payload.

        Args:
            message: Supplies ``frame_head`` (type / state / stream id).
            body: Business body written after the 16-byte head.

        Returns:
            Complete DATA frame bytes.
        """
        return _common_pack(message.frame_head, body)

    def _encode_close_frame(self, message: TrpcMessage, _: bytes) -> bytes:
        """Pack a CLOSE frame from ``message.stream_frame``.

        ``body`` is unused; the payload is serialized
        ``trpc_pb2.TrpcStreamCloseMeta``.

        Args:
            message: Must already hold a ``trpc_pb2.TrpcStreamCloseMeta``.
            _: Unused; kept so the four encode methods share a signature.

        Returns:
            Complete CLOSE frame bytes.

        Raises:
            Exception: ``stream_frame`` is not a ``trpc_pb2.TrpcStreamCloseMeta``.
                The concrete class is ``_encode_err_cls``.
        """
        close_frame: PbMessage = message.stream_frame
        if not isinstance(close_frame, trpc_pb2.TrpcStreamCloseMeta):
            raise self._encode_err_cls(
                f"{self._role.name} encode close stream frame type: '{type(close_frame)}' invalid")
        return _common_pack(message.frame_head, close_frame.SerializeToString())

    def _encode_feedback_frame(self, message: TrpcMessage, _: bytes) -> bytes:
        """Pack a FEEDBACK frame from ``message.stream_frame``.

        ``body`` is unused; the payload is serialized
        ``trpc_pb2.TrpcStreamFeedBackMeta`` (window / credit updates).

        Args:
            message: Must already hold a ``trpc_pb2.TrpcStreamFeedBackMeta``.
            _: Unused; kept so the four encode methods share a signature.

        Returns:
            Complete FEEDBACK frame bytes.

        Raises:
            Exception: ``stream_frame`` is not a ``trpc_pb2.TrpcStreamFeedBackMeta``.
                The concrete class is ``_encode_err_cls``.
        """
        feedback_frame: PbMessage = message.stream_frame
        if not isinstance(feedback_frame, trpc_pb2.TrpcStreamFeedBackMeta):
            raise self._encode_err_cls(
                f"{self._role.name} encode feedback stream frame type: {type(feedback_frame)} invalid")
        return _common_pack(message.frame_head, feedback_frame.SerializeToString())

    @abstractmethod
    def _decode_init_frame(self, message: TrpcMessage, body: bytes) -> bytes:
        """Parse an INIT frame into ``message``.

        ``body`` is the **complete frame**, including the 16-byte head;
        implementations slice ``body[FRAME_HEAD_LEN:]``. Client applies
        compress / serialization from the meta and checks
        ``response_meta.ret``. Server stores the meta in
        ``_init_metas`` and sets ``server_rpc_name``.

        Args:
            message: Destination; frame_head and stream_id are already set.
            body: Complete INIT frame, not the payload alone.

        Returns:
            Empty / unused; INIT has no business body.
        """

    def _decode_data_frame(self, message: TrpcMessage, body: bytes) -> bytes:
        """Slice the DATA payload after the 16-byte head.

        ``body`` is the complete frame. ``head_len`` is ignored; the
        business body always starts at ``FRAME_HEAD_LEN``.

        Args:
            message: ``stream_id`` is copied from ``frame_head``.
            body: Complete DATA frame.

        Returns:
            Business body bytes after the frame head.
        """
        message.set_stream_id(message.frame_head.stream_id)
        return body[FRAME_HEAD_LEN:]

    def _decode_close_frame(self, message: TrpcMessage, body: bytes) -> bytes:
        """Parse CLOSE meta; RESET or a non-zero ``ret`` is an error.

        ``body`` is the complete frame. A reset or failed close raises
        ``_decode_err_cls`` and does **not** call ``set_stream_frame``.
        There is no ``return``; :meth:`decode` turns the implicit
        ``None`` into ``b''``.

        Args:
            message: Destination for a successful ``trpc_pb2.TrpcStreamCloseMeta``.
            body: Complete CLOSE frame.

        Returns:
            Implicit ``None``; no business body on CLOSE.

        Raises:
            Exception: ``close_type == trpc_pb2.TRPC_STREAM_RESET`` or ``ret != 0``.
                The concrete class is ``_decode_err_cls``.
        """
        close: PbMessage = trpc_pb2.TrpcStreamCloseMeta()
        close.ParseFromString(body[FRAME_HEAD_LEN:])

        if close.close_type == trpc_pb2.TRPC_STREAM_RESET or close.ret != 0:  # pylint: disable=no-member
            # pylint: disable=no-member
            raise self._decode_err_cls(
                f"{self._role.name} decode close stream error, close_type: {close.close_type}, ret: {close.ret}")
        message.set_stream_frame(close)

    def _decode_feedback_frame(self, message: TrpcMessage, body: bytes) -> bytes:
        """Parse FEEDBACK meta onto ``message.stream_frame``.

        ``body`` is the complete frame. No return value; :meth:`decode`
        maps ``None`` to ``b''``.

        Args:
            message: Destination for the ``trpc_pb2.TrpcStreamFeedBackMeta``.
            body: Complete FEEDBACK frame.

        Returns:
            Implicit ``None``; no business body on FEEDBACK.
        """
        feedback: PbMessage = trpc_pb2.TrpcStreamFeedBackMeta()
        feedback.ParseFromString(body[FRAME_HEAD_LEN:])
        message.set_stream_frame(feedback)

    def _check_init_metas(self, message: TrpcMessage):
        """Restore ``server_rpc_name`` from the INIT cache.

        Called only for non-INIT frames on the server. A missing
        ``session_id`` means the stream is already closed (CLOSE encode
        dropped the session). A missing ``stream_id`` means this frame
        was never inited.

        Args:
            message: Must already have ``session_id`` and ``stream_id``.

        Raises:
            TaskStreamServerDecodeError: Session gone ("task stream
                closed") or stream id unknown ("invalid task").
        """
        session_id, stream_id = message.session_id, message.stream_id
        if session_id not in self._init_metas:
            raise TaskStreamServerDecodeError(f"task stream closed, session_id: {session_id} stream_id: {stream_id}")
        if stream_id not in self._init_metas[session_id]:
            raise TaskStreamServerDecodeError(f"invalid task, session_id: {session_id} stream_id: {stream_id}")
        init_meta: StreamInitMeta = self._init_metas[session_id][stream_id]
        message.server_rpc_name = init_meta.request_meta.func.decode('utf8')

    def _delete_init_metas(self, message: TrpcMessage):
        """Drop the INIT cache entry for this session/stream.

        No-op if the pair is already gone (duplicate CLOSE, or a CLOSE
        that never saw INIT). An empty session dict is removed so later
        DATA on that session reports "task stream closed".

        Args:
            message: Identifies the cache key via ``session_id`` /
                ``stream_id``.
        """
        session_id, stream_id = message.session_id, message.stream_id
        if (session_id not in self._init_metas) or (stream_id not in self._init_metas[session_id]):
            return
        self._init_metas[session_id].pop(stream_id)
        if len(self._init_metas[session_id]) == 0:
            self._init_metas.pop(session_id)

    def encode(self, message: TrpcMessage, body: bytes) -> bytes:
        """Dispatch on ``frame_state`` and pack one stream frame.

        After a successful server CLOSE pack, the INIT cache entry is
        deleted so later frames on that stream fail
        :meth:`_check_init_metas`.

        Args:
            message: ``frame_head.frame_state`` selects INIT / DATA /
                CLOSE / FEEDBACK.
            body: Used only for DATA; INIT / CLOSE / FEEDBACK read
                ``message.stream_frame`` instead and may pass ``None``.

        Returns:
            Complete stream frame bytes.

        Raises:
            Exception: Unknown ``frame_state``. The concrete class is
                ``_encode_err_cls``.
        """
        frame_state = message.frame_head.frame_state
        func = self._encode_map.get(frame_state, None)
        if func is None:
            raise self._encode_err_cls(f"{self._role.name} encode stream frame state: '{frame_state}' invalid")
        data = func(message, body)
        if self._role == TaskStreamCodecRole.SERVER and frame_state == trpc_pb2.TRPC_STREAM_FRAME_CLOSE:
            self._delete_init_metas(message)
        return data

    def decode(self, message: TrpcMessage, buffer: bytes) -> bytes:
        """Parse the 16-byte head, then dispatch on ``frame_state``.

        ``buffer`` is the **complete frame**, not a body slice. On the
        server, every state except INIT runs :meth:`_check_init_metas`
        so DATA / CLOSE / FEEDBACK recover ``server_rpc_name``. CLOSE
        and FEEDBACK decoders return ``None``; that is turned into
        ``b''`` here.

        Args:
            message: Filled with ``frame_head`` and ``stream_id`` first.
            buffer: Complete stream frame from the parser.

        Returns:
            DATA business body, or ``b''`` for INIT / CLOSE / FEEDBACK.

        Raises:
            Exception: Unknown ``frame_state``. The concrete class is
                ``_decode_err_cls``.
        """
        frame_head: TrpcFrameHead = parse_frame_head(buffer[:FRAME_HEAD_LEN])
        message.set_frame_head(frame_head)
        message.set_stream_id(frame_head.stream_id)
        frame_state = frame_head.frame_state
        func = self._decode_map.get(frame_state, None)
        if func is None:
            raise self._decode_err_cls(f"{self._role.name} decode stream frame state: '{frame_state}' invalid")
        rsp = func(message, buffer)
        if self._role == TaskStreamCodecRole.SERVER:
            if frame_state != trpc_pb2.TRPC_STREAM_FRAME_INIT:
                self._check_init_metas(message)
        return rsp or b''


@register_plugin(PluginType.CLIENT_CODEC, INNER_TASK_STREAM, init_pos=PluginInitPos.WORKER)
class TaskStreamClientCodec(_TaskStreamCodec):
    """Worker-side (client) codec for the inner task stream.

    Registered as ``CLIENT_CODEC`` / ``inner_task_stream``. Keeps the
    base class's CLIENT role and client encode/decode error
    classes. INIT encode fills ``request_meta.func`` from
    ``client_rpc_name``; INIT decode applies the peer's compress /
    serialization and fails on a non-zero ``response_meta.ret``.
    """

    def __init__(self):
        super().__init__()
        self._encode_err_cls: Callable[[str], Exception] = TaskStreamClientEncodeError
        self._decode_err_cls: Callable[[str], Exception] = TaskStreamClientDecodeError

    def _encode_init_frame(self, message: TrpcMessage, body: bytes) -> bytes:
        """Pack a client INIT: func, content_type, content_encoding.

        Reuses ``message.stream_frame`` when it is already a
        ``trpc_pb2.TrpcStreamInitMeta``; otherwise a fresh meta is created.
        ``body is None`` is normalized to ``b''`` for historical
        callers; the packed payload is still the serialized meta, not
        ``body``.

        Args:
            message: ``client_rpc_name``, ``serialization_type``, and
                ``compress_type`` are copied onto the INIT meta.
            body: Unused after the ``None`` guard; kept for the shared
                encode signature.

        Returns:
            Complete INIT frame bytes.

        Raises:
            TaskStreamClientEncodeError: ``stream_frame`` is set but is
                not a ``trpc_pb2.TrpcStreamInitMeta``.
        """
        if body is None:
            body = b''
        if message.stream_frame:
            if not isinstance(message.stream_frame, trpc_pb2.TrpcStreamInitMeta):
                raise TaskStreamClientEncodeError(
                    f"client encode init stream frame type: '{type(message.stream_frame)}' invalid")
            init_meta = message.stream_frame
        else:
            init_meta = trpc_pb2.TrpcStreamInitMeta()

        req = init_meta.request_meta
        req.func = message.client_rpc_name.encode('utf8')
        # Downstream ContentType: which serializer the task should use.
        init_meta.content_type = message.serialization_type
        # Downstream ContentEncoding: which compressor the task should use.
        init_meta.content_encoding = message.compress_type
        stream_buf = init_meta.SerializeToString()
        return _common_pack(message.frame_head, stream_buf)

    def _decode_init_frame(self, message: TrpcMessage, body: bytes) -> bytes:
        """Apply the server INIT meta and fail on a non-zero ret.

        ``body`` is the complete frame. Compress / serialization on
        ``message`` are overwritten from ``content_encoding`` /
        ``content_type`` so later DATA frames match the task.

        Args:
            message: Destination for compress type, serialization type,
                and ``stream_frame``.
            body: Complete INIT frame.

        Returns:
            Implicit ``None``; no business body on INIT.

        Raises:
            TaskStreamClientDecodeError: ``response_meta.ret != 0``.
        """
        init_meta = trpc_pb2.TrpcStreamInitMeta()
        init_meta.ParseFromString(body[FRAME_HEAD_LEN:])
        message.set_compress_type(init_meta.content_encoding)  # pylint: disable=no-member
        message.set_serialization_type(init_meta.content_type)  # pylint: disable=no-member
        response_meta = init_meta.response_meta  # pylint: disable=no-member
        if response_meta and response_meta.ret != 0:
            raise TaskStreamClientDecodeError(f"client decode init stream frame, error {response_meta.ret}")
        message.set_stream_frame(init_meta)


@register_plugin(PluginType.SERVER_CODEC, INNER_TASK_STREAM, init_pos=PluginInitPos.WORKER)
class TaskStreamServerCodec(_TaskStreamCodec):
    """Task-side (server) codec for the inner task stream.

    Registered as ``SERVER_CODEC`` / ``inner_task_stream``. Flips
    ``_role`` to SERVER and uses server encode/decode
    errors. INIT decode stores meta under ``session_id + stream_id``;
    INIT encode copies ``server_rsp_err`` onto ``response_meta``.
    """

    def __init__(self):
        super().__init__()
        self._encode_err_cls: Callable[[str], Exception] = TaskStreamServerEncodeError
        self._decode_err_cls: Callable[[str], Exception] = TaskStreamServerDecodeError
        self._role = TaskStreamCodecRole.SERVER

    def _encode_init_frame(self, message: TrpcMessage, _: bytes) -> bytes:
        """Pack a server INIT, attaching ``server_rsp_err`` as response_meta.

        Reuses ``message.stream_frame`` when it is already a
        ``trpc_pb2.TrpcStreamInitMeta``. ``content_type`` / ``content_encoding``
        are the types this side will use on later DATA frames.
        ``body`` is unused.

        Args:
            message: ``serialization_type``, ``compress_type``, and
                ``server_rsp_err`` feed the INIT meta.
            _: Unused; kept for the shared encode signature.

        Returns:
            Complete INIT frame bytes.

        Raises:
            TaskStreamServerEncodeError: ``stream_frame`` is set but is
                not a ``trpc_pb2.TrpcStreamInitMeta``.
        """
        if message.stream_frame:
            if not isinstance(message.stream_frame, trpc_pb2.TrpcStreamInitMeta):
                raise TaskStreamServerEncodeError(
                    f"server encode init stream frame type: '{type(message.stream_frame)}' invalid")
            init_meta = message.stream_frame
        else:
            init_meta = trpc_pb2.TrpcStreamInitMeta()
        init_meta.content_type = message.serialization_type
        init_meta.content_encoding = message.compress_type
        rsp_meta = trpc_pb2.TrpcStreamInitResponseMeta()
        err = message.server_rsp_err
        rsp_meta.error_msg = err.msg.encode('utf8')
        rsp_meta.ret = err.code
        init_meta.response_meta.CopyFrom(rsp_meta)
        stream_buf = init_meta.SerializeToString()
        return _common_pack(message.frame_head, stream_buf)

    def _decode_init_frame(self, message: TrpcMessage, body: bytes) -> bytes:
        """Parse client INIT, set rpc name, and cache the meta.

        ``body`` is the complete frame. The meta is stored at
        ``_init_metas[session_id][stream_id]`` so later non-INIT frames
        can recover ``server_rpc_name`` via :meth:`_check_init_metas`.

        Args:
            message: Destination; ``session_id`` must already be set by
                the transport so the cache key is stable.
            body: Complete INIT frame.

        Returns:
            Implicit ``None``; no business body on INIT.
        """
        init_meta: PbMessage = trpc_pb2.TrpcStreamInitMeta()
        init_meta.ParseFromString(body[FRAME_HEAD_LEN:])
        message.set_stream_id(message.frame_head.stream_id)
        message.set_compress_type(init_meta.content_encoding)  # pylint: disable=no-member
        message.set_serialization_type(init_meta.content_type)  # pylint: disable=no-member
        message.set_stream_frame(init_meta)
        req = init_meta.request_meta
        message.set_server_rpc_name(req.func.decode('utf8'))
        self._init_metas[message.session_id][message.stream_id] = init_meta


@register_plugin(PluginType.CLIENT_CHECKER, INNER_TASK_STREAM, init_pos=PluginInitPos.WORKER)
class TaskStreamClientParser(BaseClientProtocolChecker):
    """Async client framer: read 16 bytes, then the rest of the package.

    Registered as ``CLIENT_CHECKER`` / ``inner_task_stream``. Does not
    decode stream meta; it only returns a complete frame for
    :class:`TaskStreamClientCodec`. Magic is checked against
    ``trpc_pb2.TRPC_MAGIC_VALUE`` (same integer as ``TASK_STREAM_MAGIC_VALUE``).
    """

    async def read_frame(self, conn: PooledConnection) -> bytes:
        """Read one complete frame from a pooled connection.

        First ``FRAME_HEAD_LEN`` bytes, then
        ``package_len - FRAME_HEAD_LEN``. A bad magic closes the
        connection with ``force=True`` before raising, so a corrupt
        stream is not reused from the pool.

        Args:
            conn: Connected ``PooledConnection``; ``read`` waits until enough
                bytes are available.

        Returns:
            Concatenated head + remainder, a complete stream frame.

        Raises:
            TaskStreamClientReadFrameError: ``stx`` is not the tRPC magic.
        """
        frame_head_buff = await conn.read(FRAME_HEAD_LEN)

        frame_head: TrpcFrameHead = parse_frame_head(frame_head_buff)
        if frame_head.stx != trpc_pb2.TRPC_MAGIC_VALUE:
            conn.close(True)
            raise TaskStreamClientReadFrameError(f"invalid magic number {frame_head.stx}, expect: {trpc_pb2.TRPC_MAGIC_VALUE}")

        left_pkg = await conn.read(frame_head.package_len - FRAME_HEAD_LEN)

        return frame_head_buff + left_pkg


class TaskStreamServerParser:
    """Buffered server framer: scan magic + ``package_len`` out of ``buf``.

    Not plugin-registered; the task-stream server constructs it
    directly. ``feeds_data`` appends socket chunks; :meth:`read` yields
    every complete frame and leaves a partial one in ``buf``.

    This is not :class:`~trpc_sdk.abc.BaseServerProtocolChecker`: that
    ABC yields one ``bytes`` frame from ``_buffer``, while this parser
    yields a list and keeps a ``bytearray`` named ``buf``.
    """

    def __init__(self):
        self.buf = bytearray()
        self.size = 1024
        self.length = 0

    def feeds_data(self, data: bytes):
        """Append a socket chunk and bump the buffered-byte counter.

        ``self.buf`` is the local sticky-packet buffer.
        ``self.length`` tracks ``len(buf)`` so :meth:`read` can scan
        without calling ``len`` on every iteration.

        Args:
            data: Next bytes from the Unix-domain socket.
        """
        self.length = self.length + len(data)
        self.buf.extend(data)

    def read(self) -> List[bytes]:
        """Cut every complete frame currently in the buffer.

        Magic is ``TASK_STREAM_MAGIC_VALUE`` (same as
        ``trpc_pb2.TRPC_MAGIC_VALUE``). ``package_len`` is read from offset 4 of
        each candidate. A frame that has not fully arrived ``break``s
        the loop and stays in ``buf``. Consumed bytes are sliced off;
        a partial tail is kept.

        Returns:
            Zero or more complete frame ``bytes`` objects, in arrival
            order. Empty if only a partial frame is buffered.

        Raises:
            TaskStreamServerDecodeError: Magic mismatch, or
                ``package_len`` smaller than ``FRAME_HEAD_LEN``.
        """
        results = []
        scan_end = 0
        while self.length >= scan_end + FRAME_HEAD_LEN:
            stx = socket.ntohs(struct.unpack('=H', self.buf[scan_end:scan_end + 2])[0])
            if stx != TASK_STREAM_MAGIC_VALUE:
                raise TaskStreamServerDecodeError(f"magic is {stx} != {TASK_STREAM_MAGIC_VALUE}")
            package_len = socket.ntohl(struct.unpack('=I', self.buf[scan_end + 4:scan_end + 8])[0])
            if package_len < FRAME_HEAD_LEN:
                raise TaskStreamServerDecodeError(f"package_len is {package_len} < {FRAME_HEAD_LEN}")
            if self.length < scan_end + package_len:
                break
            results.append(bytes(self.buf[scan_end:scan_end + package_len]))
            scan_end = scan_end + package_len
        if scan_end > 0:
            self.buf = self.buf[scan_end:self.length]
            self.length = self.length - scan_end
        return results

    def reset(self):
        """Clear ``buf``.

        ``self.length`` is not reset here (historical). Callers that
        reuse the parser after an error should treat ``length`` as
        stale unless they also zero it.
        """
        self.buf.clear()
