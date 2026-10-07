# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Filter extension interfaces.

This module defines the abstract contracts and shared result types used by
pluggable filters that intercept unary and streaming RPC call chains.
"""
from __future__ import annotations

from abc import abstractmethod
from dataclasses import dataclass
from typing import Any
from typing import AsyncGenerator
from typing import Awaitable
from typing import Callable
from typing import Generic
from typing import Optional
from typing import Tuple
from typing import TypeVar

from ._cython import TrpcContext  # pyright: ignore[reportMissingModuleSource]
from ._plugin import BasePlugin

FILTER_RSP = TypeVar("FILTER_RSP")


@dataclass
class FilterStreamResultType(Generic[FILTER_RSP]):
    """Carry one streaming filter result.

    Attributes:
        rsp: Response payload produced by the filter, if any.
        error: Exception raised during filter execution, if any.
        is_continue: Whether the filter chain should continue processing.
    """

    rsp: Optional[FILTER_RSP] = None
    error: Optional[Exception] = None
    is_continue: bool = True


FilterReturnType = Tuple[Any, Optional[Exception]]
"""Unary filter result as ``(response, error)``."""

FilterHandleType = Callable[[], Awaitable[FilterReturnType]]
"""Next unary handler or filter in the chain."""

FilterAsyncGenReturnType = AsyncGenerator[FilterStreamResultType, None]
"""Streaming filter result as an asynchronous generator."""

FilterAsyncGenHandleType = Callable[[], AsyncGenerator[FilterStreamResultType, None]]
"""Next streaming handler or filter in the chain."""


class BaseFilter(BasePlugin):
    """Define unary and streaming filter interception points.

    Filters wrap the next handler in the RPC call chain so that cross-cutting
    concerns can observe or modify requests and responses.
    """

    @abstractmethod
    async def run(
        self,
        ctx: TrpcContext,
        req: Any,
        next_handle: FilterHandleType,
    ) -> FilterReturnType:
        """Intercept one unary RPC through the filter chain.

        Args:
            ctx: Request-scoped RPC context.
            req: Application request object being processed.
            next_handle: Next unary handler or filter in the chain.

        Returns:
            Tuple of ``(response, error)`` produced by this filter stage.
        """
        ...

    @abstractmethod
    async def run_stream(
        self,
        ctx: TrpcContext,
        req: Any,
        next_handle: FilterAsyncGenHandleType,
    ) -> FilterAsyncGenReturnType:
        """Intercept one streaming RPC through the filter chain.

        This method is used when the next handler is an asynchronous generator.
        Implementations that do not support streaming should raise
        ``NotImplementedError``.

        Args:
            ctx: Request-scoped RPC context.
            req: Application request object being processed.
            next_handle: Next streaming handler or filter in the chain.

        Returns:
            Asynchronous generator that yields streaming filter results.

        Raises:
            NotImplementedError: If the filter does not support streaming.
        """
        ...
