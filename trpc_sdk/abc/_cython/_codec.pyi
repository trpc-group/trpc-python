# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

from abc import ABC
from abc import abstractmethod
from typing import Any

from ._context import TrpcContext


class BaseCodec(ABC):
    """Context-first protocol codec interface."""

    @abstractmethod
    def encode_data(self, context: TrpcContext, body: bytes) -> bytes: ...

    @abstractmethod
    def decode_data(self, context: TrpcContext, buffer: bytes) -> bytes: ...


class BaseServerProtocolChecker:
    """Reusable incremental transport frame parser."""

    @abstractmethod
    def feed_data(self, data: bytes) -> None:
        """Feed data to the parser."""
        ...

    @abstractmethod
    def read(self) -> bytes:
        """Read data from the parser."""
        ...

    @abstractmethod
    def reset(self) -> None:
        """Reset the parser."""
        ...


class BaseClientProtocolChecker(ABC):
    """Asynchronous client-side frame reader."""

    @abstractmethod
    async def read_frame(self, connection: Any) -> bytes:
        """Read a frame from the connection."""
        ...
