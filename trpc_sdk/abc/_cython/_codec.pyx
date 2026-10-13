# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

# cython: language_level=3, boundscheck=False, wraparound=False

from abc import abstractmethod
from typing import Any

# Imported from the submodules rather than the ``trpc_sdk.abc`` package so
# this module can be pulled in while that package's ``__init__`` is still
# executing. ``_message_builder`` does not import ``_codec`` back.
from trpc_sdk.abc import BasePlugin

from ._context cimport TrpcContext


cdef class _Codec:
    """Private C-level root shared by every tRPC codec.

    Framework Cython codecs (the unary and stream codecs under
    ``trpc_sdk.codec._cython``) inherit this type directly, because a
    ``cdef class`` cannot inherit the Python-level :class:`BaseCodec`.
    Each implementation overrides only the hooks it serves: unary codecs
    implement ``encode`` / ``decode``, stream codecs implement
    ``encode_stream`` / ``decode_stream``. The rest keep the default
    ``NotImplementedError``.

    Not exported from ``trpc_sdk.abc``; user codecs subclass
    :class:`BaseCodec` instead.
    """

    cpdef object encode_data(self, TrpcContext context, object body):
        """Encode a body and a message into a transport frame.

        Args:
            message: :class:`TrpcMessage` to encode.
            body: Serialized request or response body; ``None`` for
                stream INIT / CLOSE / FEEDBACK frames.

        Returns:
            Complete transport frame bytes.

        Raises:
            NotImplementedError: The subclass does not encode.
        """
        raise NotImplementedError("BaseCodec.encode is not implemented")

    cpdef object decode_data(self, TrpcContext context, object buffer):
        """Decode a complete transport frame into its body.

        Python subclasses may override this with a plain function or a
        coroutine function; the caller decides whether to ``await`` the
        result. The tRPC and HTTP server codecs are ``async``, the client
        codecs are not.

        Args:
            message: :class:`TrpcMessage` that receives decoded metadata.
            buffer: Complete transport frame bytes.

        Returns:
            Decoded request or response body.

        Raises:
            NotImplementedError: The subclass does not decode.
        """
        raise NotImplementedError("BaseCodec.decode is not implemented")

    cpdef object encode_stream(self, TrpcContext context, object body):
        """Encode one stream frame selected by ``frame_head.frame_state``.

        Args:
            message: :class:`TrpcMessage` whose ``frame_head`` carries the
                stream sub-type.
            body: Business body for DATA; ``None`` for INIT / CLOSE /
                FEEDBACK, whose payload is ``message.stream_frame``.

        Returns:
            Complete stream frame bytes.

        Raises:
            NotImplementedError: The subclass does not encode streams.
        """
        raise NotImplementedError("BaseCodec.encode_stream is not implemented")

    cpdef object decode_stream(self, TrpcContext context, object buffer):
        """Decode one stream frame selected by ``frame_head.frame_state``.

        Args:
            message: :class:`TrpcMessage` whose ``frame_head`` was already
                parsed from ``buffer``.
            buffer: Complete stream frame bytes, head included.

        Returns:
            Stream payload for DATA, empty bytes for the meta frames.

        Raises:
            NotImplementedError: The subclass does not decode streams.
        """
        raise NotImplementedError("BaseCodec.decode_stream is not implemented")


class BaseCodec(_Codec, BasePlugin):
    """Public codec plugin base, message- and context-first.

    The message-first hooks ``encode`` / ``decode`` (and the stream pair
    ``encode_stream`` / ``decode_stream``) come from the private cdef
    :class:`_Codec`, so a Python subclass overrides them with ordinary
    ``def`` / ``async def`` methods. ``encode_data`` / ``decode_data`` are
    the context-first entry points callers use; their default
    implementation pulls the message out of the context and forwards.
    Only a codec that needs request-scoped metadata beyond the message --
    HTTP, which reads the aiohttp request / response and the accepted
    status codes from context meta -- overrides the ``*_data`` pair.

    No method is abstract. ``encode_data`` / ``decode_data`` carry a
    working default, and making the message-first pair abstract would
    stop a context-first-only codec such as
    :class:`~trpc_sdk.http.ServerHttpCodec` from being constructed at
    plugin setup. A genuinely missing implementation fails on first call
    instead.
    """

    def __init__(self):
        # _Codec is first in the MRO and its __init__ does not cooperate
        # with super(); BasePlugin must run so ``_name`` exists before the
        # plugin registry reads ``.name``.
        BasePlugin.__init__(self)

cdef class BaseServerProtocolParser:
    """Maintain a reusable byte buffer for incremental frame parsing.

    Subclasses implement ``read`` and may access ``_buffer`` directly from
    Cython. Python subclasses can override the cpdef methods when necessary.
    """

    def __cinit__(self):
        self._buffer = bytearray()

    cpdef void feed_data(self, bytes data) except *:
        """Feed data to the parser."""
        self._buffer.extend(data)

    cpdef list read(self):
        """Return every complete frame currently buffered."""
        raise NotImplementedError("BaseServerProtocolParser.read is not implemented")

    cpdef void reset(self) except *:
        """Reset the parser."""
        self._buffer.clear()


class BaseClientProtocolParser(BasePlugin):
    """Define asynchronous client-side frame reading."""

    @abstractmethod
    async def read_frame(self, connection: Any) -> bytes:
        """Read a frame from the connection."""
        ...
