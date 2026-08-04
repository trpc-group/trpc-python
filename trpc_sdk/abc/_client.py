# Tencent is pleased to support the open source community by making tRPC-Agent-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Agent-Python is licensed under Apache-2.0.

"""Client-side extension interfaces.

This module defines the abstract contracts for unary RPC clients, application
streams, and stream transports used by pluggable client implementations.
"""
from __future__ import annotations

from abc import ABC
from abc import abstractmethod
from typing import Any
from typing import List

from ._cython import TrpcContext  # pyright: ignore[reportMissingModuleSource]


class BaseClient(ABC):
    """Define unary RPC invocation."""

    @abstractmethod
    async def invoke(
        self,
        ctx: TrpcContext,
        req: Any,
        rsp_cls: Any,
        options: List,
    ) -> Any:
        """Invoke one unary RPC and return the decoded response.

        Args:
            ctx: Request-scoped RPC context.
            req: Application request object to encode and send.
            rsp_cls: Response class used to decode the reply.
            options: Client option callables applied to this invocation.

        Returns:
            Decoded application response object.
        """
        ...


class BaseClientStream(ABC):
    """Define application-level client stream operations."""

    @abstractmethod
    async def recv_msg(self, msg: Any):
        """Receive and decode one message from the peer stream.

        Args:
            msg: Destination object or type used to decode the payload.

        Returns:
            Decoded application message.
        """
        ...

    @abstractmethod
    async def send_msg(self, msg: Any):
        """Encode and send one message to the peer stream.

        Args:
            msg: Application message to send.
        """
        ...

    @abstractmethod
    async def close_send(self):
        """Close the local sending half of the stream."""
        ...

    @abstractmethod
    def context(self) -> TrpcContext:
        """Return the context associated with this stream.

        Returns:
            Stream-scoped RPC context.
        """
        ...


class BaseClientStreamConnection(ABC):
    """Define byte-oriented client stream connection operations."""

    @abstractmethod
    async def recv(self, ctx: TrpcContext) -> bytes:
        """Receive one transport frame from the peer.

        Args:
            ctx: Stream-scoped RPC context.

        Returns:
            Raw frame bytes.
        """
        ...

    @abstractmethod
    async def send(self, ctx: TrpcContext, req: Any):
        """Send one request payload over the stream transport.

        Args:
            ctx: Stream-scoped RPC context.
            req: Encoded request payload to send.
        """
        ...

    @abstractmethod
    def close(self, ctx: TrpcContext):
        """Close the stream transport.

        Args:
            ctx: Stream-scoped RPC context.
        """
        ...

    @abstractmethod
    async def start(self, ctx: TrpcContext, options: List) -> Any:
        """Establish the stream transport session.

        Args:
            ctx: Stream-scoped RPC context.
            options: Client option callables applied when starting the stream.

        Returns:
            Transport-specific session handle or connection object.
        """
        ...
