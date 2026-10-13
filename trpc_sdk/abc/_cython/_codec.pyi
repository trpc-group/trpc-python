# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

from abc import abstractmethod
from typing import Any
from typing import List
from typing import Optional

from trpc_sdk.abc import BasePlugin
from ._context import TrpcContext
from ._message import TrpcMessage


class _Codec:
    """Private C-level codec root; framework Cython codecs inherit it.

    Every hook raises ``NotImplementedError`` until a subclass overrides
    it. Unary codecs implement ``encode`` / ``decode``; stream codecs
    implement ``encode_stream`` / ``decode_stream``. Not exported from
    ``trpc_sdk.abc``.
    """

    def encode(self, message: TrpcMessage, body: Optional[bytes]) -> Any: ...

    def decode(self, message: TrpcMessage, buffer: Optional[bytes]) -> Any: ...

    def encode_stream(self, message: TrpcMessage, body: Optional[bytes]) -> Any: ...

    def decode_stream(self, message: TrpcMessage, buffer: Optional[bytes]) -> Any: ...


class BaseCodec(_Codec, BasePlugin):
    """Public codec plugin base, message-first hooks plus context-first entries.

    The message-first hooks, including the stream pair, come from
    :class:`_Codec`. No method is abstract: ``encode_data`` /
    ``decode_data`` have working defaults that forward to the
    message-first pair, and a codec that implements only one half of
    either pair stays constructible with the missing half raising
    ``NotImplementedError`` when first called. ``decode`` /
    ``decode_data`` may also be coroutine functions; the caller decides
    whether to ``await`` them.
    """

    def __init__(self) -> None: ...

    def encode(self, message: TrpcMessage, body: Optional[bytes]) -> bytes: ...

    def decode(self, message: TrpcMessage, buffer: Optional[bytes]) -> Any: ...

    def encode_stream(self, message: TrpcMessage, body: Optional[bytes]) -> bytes: ...

    def decode_stream(self, message: TrpcMessage, buffer: Optional[bytes]) -> Any: ...

    def encode_data(self, context: TrpcContext, body: Optional[bytes]) -> bytes: ...

    def decode_data(self, context: TrpcContext, buffer: Optional[bytes]) -> Any: ...


class BaseServerProtocolParser:
    """Reusable incremental transport frame parser."""

    @abstractmethod
    def feed_data(self, data: bytes) -> None:
        """Feed data to the parser."""
        ...

    @abstractmethod
    def read(self) -> List[bytes]:
        """Return every complete frame currently buffered."""
        ...

    @abstractmethod
    def reset(self) -> None:
        """Reset the parser."""
        ...


class BaseClientProtocolParser(BasePlugin):
    """Asynchronous client-side frame reader."""

    @abstractmethod
    async def read_frame(self, connection: Any) -> bytes:
        """Read a frame from the connection."""
        ...
