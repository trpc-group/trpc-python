# -*- coding: utf-8 -*-
#
# Copyright @ 2025 Tencent.com
"""Base filter module for TRPC framework."""

# System imports
from abc import ABC, abstractmethod
from typing import Any, AsyncGenerator, Awaitable, Callable, Optional, Tuple, Generic, TypeVar
from dataclasses import dataclass

# if python version is 3.6, install dataclasses

# Local imports
from .context import Context

FILTER_RSP = TypeVar('FILTER_RSP')


@dataclass
class FilterStreamResultType(Generic[FILTER_RSP]):
    """Standardized result container for filter operations.
    
    Attributes:
        rsp: Response data produced by the filter (optional)
        error: Exception encountered during execution (optional)
    """
    rsp: Optional[FILTER_RSP] = None
    """The response data produced by filter execution."""

    error: Optional[Exception] = None
    """The exception encountered during filter execution."""

    is_continue: bool = True
    """Whether to continue processing."""


# Type aliases
FilterReturnType = Tuple[Any, Optional[Exception]]
"""Type alias for filter return value (result, error) tuple."""

FilterHandleType = Callable[[], Awaitable[FilterReturnType]]
"""Type alias for async filter handler function."""

FilterAsyncGenReturnType = AsyncGenerator[FilterStreamResultType, None]
"""Type alias for async generator filter return type."""

FilterAsyncGenHandleType = Callable[[], AsyncGenerator[FilterStreamResultType, None]]
"""Type alias for async generator filter handler function."""


class Filter(ABC):
    """Abstract base class for TRPC filters.
    
    Filters are used to intercept and process requests/responses in the TRPC call chain.
    """

    def __init__(self) -> None:
        """Initialize the filter with its class name."""
        super().__init__()
        self._name = self.__class__.__name__

    @abstractmethod
    async def run(self, ctx: Context, req: Any, handle: FilterHandleType) -> FilterReturnType:
        """Process the request synchronously.
        
        Args:
            ctx: TRPC context object containing request metadata
            req: The request object to be processed
            handle: Next filter/handler in the chain
            
        Returns:
            Tuple containing processed result and optional error
        """

    async def run_stream(self, ctx: Context, req: Any, handle: FilterAsyncGenHandleType) -> FilterAsyncGenReturnType:
        """Process the request in streaming mode.
        
        Note:
            1. This interface is only used when the handler is an async generator
            2. Uses async generator for streaming processing
            
        Args:
            ctx: TRPC context object containing request metadata
            req: The request object to be processed
            handle: Next filter/handler in the chain, must be an async generator
            
        Raises:
            NotImplementedError: If streaming is not supported by the filter
            
        Returns:
            Async generator yielding processed results
        """
        raise NotImplementedError(f'run_stream for {self._name}, type: {type(self)} is not implemented.')
