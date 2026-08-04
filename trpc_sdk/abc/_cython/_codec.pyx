# Tencent is pleased to support the open source community by making tRPC-Agent-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Agent-Python is licensed under Apache-2.0.

# cython: language_level=3, boundscheck=False, wraparound=False

from abc import ABC
from abc import abstractmethod
from typing import Any

from ._context cimport TrpcContext


class BaseCodec(ABC):
    """Define the context-first protocol codec interface.

    The context owns the message and provides request-scoped metadata. Internal
    codecs therefore receive one object instead of relying on a reverse
    Message-to-Context reference.
    """

    @abstractmethod
    def encode_data(self, TrpcContext context, bytes body):
        """Encode a body and the context message into a transport frame.

        Args:
            context: RPC context containing the message to encode.
            body: Serialized request or response body.

        Returns:
            Complete transport frame bytes.
        """
        ...

    @abstractmethod
    def decode_data(self, TrpcContext context, bytes buffer):
        """Decode a complete transport frame into its body.

        Args:
            context: RPC context whose message receives decoded metadata.
            buffer: Complete transport frame bytes.

        Returns:
            Decoded request or response body.
        """
        ...

cdef class BaseServerProtocolChecker:
    """Maintain a reusable byte buffer for incremental frame parsing.

    Subclasses implement ``read`` and may access ``_buffer`` directly from
    Cython. Python subclasses can override the cpdef methods when necessary.
    """

    def __cinit__(self):
        self._buffer = bytearray()

    cpdef void feed_data(self, bytes data) except *:
        """Feed data to the parser."""
        self._buffer.extend(data)

    cpdef bytes read(self):
        """Read data from the parser."""
        raise NotImplementedError("BaseServerProtocolChecker.read is not implemented")

    cpdef void reset(self) except *:
        """Reset the parser."""
        self._buffer.clear()


class BaseClientProtocolChecker(ABC):
    """Define asynchronous client-side frame reading."""

    @abstractmethod
    async def read_frame(self, connection: Any) -> bytes:
        """Read a frame from the connection."""
        ...
