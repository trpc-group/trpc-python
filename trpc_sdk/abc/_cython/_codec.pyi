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


class _Codec:
    """Private C-level codec root; framework Cython codecs inherit it.

    Every hook takes the :class:`TrpcContext` and raises
    ``NotImplementedError`` until a subclass overrides it. Unary codecs
    implement ``encode_data`` / ``decode_data``; stream codecs implement
    ``encode_stream`` / ``decode_stream``. Not exported from
    ``trpc_sdk.abc``.
    """

    def encode_data(self, context: TrpcContext, body: Optional[bytes]) -> Any: ...

    def decode_data(self, context: TrpcContext, buffer: Optional[bytes]) -> Any: ...

    def encode_stream(self, context: TrpcContext, body: Optional[bytes]) -> Any: ...

    def decode_stream(self, context: TrpcContext, buffer: Optional[bytes]) -> Any: ...


class BaseCodec(_Codec, BasePlugin):
    """Public codec plugin base with four context-first hooks.

    No method is abstract: a codec that implements only the unary pair
    or only the stream pair stays constructible, with the missing half
    raising ``NotImplementedError`` when first called. ``decode_data``
    may be a coroutine function; the caller decides whether to ``await``
    it.
    """

    def __init__(self) -> None: ...

    def encode_data(self, context: TrpcContext, body: Optional[bytes]) -> bytes: ...

    def decode_data(self, context: TrpcContext, buffer: Optional[bytes]) -> Any: ...

    def encode_stream(self, context: TrpcContext, body: Optional[bytes]) -> bytes: ...

    def decode_stream(self, context: TrpcContext, buffer: Optional[bytes]) -> Any: ...


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
