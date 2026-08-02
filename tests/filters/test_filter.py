# -*- coding: utf-8 -*-
#
# Copyright @ 2022 Tencent.com
"""Test cases for TRPC filter functionality.

This module contains unit tests for verifying the behavior of TRPC filters,
including synchronous and asynchronous filter chains, error handling,
and timeout scenarios.
"""

import asyncio

from typing import Any

import pytest

from trpc_sdk import context
from trpc_sdk import exceptions
from trpc_sdk import filters as filter
from trpc_sdk.abc.context import Context
from trpc_sdk.abc.filter import Filter
from trpc_sdk.abc.filter import FilterAsyncGenHandleType
from trpc_sdk.abc.filter import FilterAsyncGenReturnType
from trpc_sdk.abc.filter import FilterHandleType
from trpc_sdk.abc.filter import FilterReturnType
from trpc_sdk.abc.filter import FilterStreamResultType
from trpc_sdk.utils import get_or_create_event_loop


class FakeResponse:
    """Mock response object for testing filter functionality."""

    def __init__(self, msg, count):
        """Initialize fake response with message and counter.
        
        Args:
            msg: Response message string
            count: Counter value for tracking filter operations
        """
        self.msg = msg
        self.count = count


class FakeRequest:
    """Mock request object for testing filter functionality."""

    def __init__(self, msg, count):
        """Initialize fake request with message and counter.
        
        Args:
            msg: Request message string
            count: Counter value for tracking filter operations
        """
        self.msg = msg
        self.count = count


class DefaultFilter(Filter):
    """Default test filter implementation for verifying filter chain behavior."""

    # @override # only python3.8 or higher from typing_extensions import override
    async def run(self, ctx: Context, req: Any, handle: FilterHandleType) -> FilterReturnType:
        """Process synchronous request through filter chain.
        
        Args:
            ctx: TRPC context object
            req: Request object
            handle: Next handler in the filter chain
            
        Returns:
            Tuple of (response, error) after processing
        """
        req.count += 1
        rsp, error = await handle()
        if error is None:
            rsp.count += 1
        return rsp, error

    # @override # only python3.8 or higher
    async def run_stream(self, ctx: Context, req: Any, handle: FilterAsyncGenHandleType) -> FilterAsyncGenReturnType:
        """Process asynchronous stream request through filter chain.
        
        Args:
            ctx: TRPC context object
            req: Request object
            handle: Next handler in the filter chain
            
        Returns:
            Async generator yielding (response, error) tuples
        """
        req.count += 1
        async for event in handle():
            rsp: FilterStreamResultType = event
            rsp.rsp.count += 1
            yield event
        return


async def fake_handle(ctx: Context, req: FakeRequest):
    """Mock handler that returns successful response.
    
    Args:
        ctx: TRPC context object
        req: Request object
        
    Returns:
        FakeResponse with processed message and count
    """
    return FakeResponse("rsp+" + req.msg, req.count)


async def fake_handle_with_exception(ctx: Context, req: FakeRequest):
    """Mock handler that raises generic exception.
    
    Args:
        ctx: TRPC context object
        req: Request object
        
    Raises:
        Exception: Always raises for testing error handling
    """
    raise Exception("exception in handle")


async def fake_handle_with_trpc_exception(ctx: Context, req: FakeRequest):
    """Mock handler that raises TRPC-specific exception.
    
    Args:
        ctx: TRPC context object
        req: Request object
        
    Raises:
        ClientConnectionError: TRPC-specific error for testing
    """
    raise exceptions.ClientConnectionError("exception in handle")


async def fake_handle_with_timeout(ctx: Context, req: FakeRequest):
    """Mock handler that simulates timeout by sleeping.
    
    Args:
        ctx: TRPC context object
        req: Request object
    """
    await asyncio.sleep(2)


def test_filter():
    """Test basic filter chain functionality with multiple filters."""
    chain = filter.FilterChain()
    chain.add_filter(DefaultFilter())
    chain.add_filter(DefaultFilter())
    h = chain.handle(context.Context(), FakeRequest("req", 0), fake_handle)
    result, error = get_or_create_event_loop().run_until_complete(h)
    assert result.msg == "rsp+req" and result.count == 4


def test_filter_with_timeout():
    """Test filter chain behavior with timeout handler."""
    chain = filter.FilterChain()
    chain.add_filter(DefaultFilter())
    h = chain.handle(context.Context(), FakeRequest("req", 0), fake_handle_with_timeout, wait_for_time=1)
    result, error = get_or_create_event_loop().run_until_complete(h)
    assert error is not None


def test_filter_with_timeout_and_no_filter():
    """Test timeout behavior without any filters in chain."""
    chain = filter.FilterChain()
    h = chain.handle(context.Context(), FakeRequest("req", 0), fake_handle_with_timeout, wait_for_time=1)
    result, error = get_or_create_event_loop().run_until_complete(h)
    assert error is not None


def test_filter_with_exception():
    """Test filter chain behavior with exception-throwing handler."""
    chain = filter.FilterChain()
    chain.add_filter(DefaultFilter())
    chain.add_filter(DefaultFilter())
    h = chain.handle(context.Context(), FakeRequest("req", 0), fake_handle_with_exception)
    result, error = get_or_create_event_loop().run_until_complete(h)
    assert error is not None


def test_one_filter():
    """Test filter chain with single filter."""
    chain = filter.FilterChain()
    chain.add_filter(DefaultFilter())
    h = chain.handle(context.Context(), FakeRequest("req", 0), fake_handle)
    result, error = get_or_create_event_loop().run_until_complete(h)
    assert result.msg == "rsp+req" and result.count == 2


def test_one_filter_with_exception():
    """Test single filter chain with exception-throwing handler."""
    chain = filter.FilterChain()
    chain.add_filter(DefaultFilter())
    h = chain.handle(context.Context(), FakeRequest("req", 0), fake_handle_with_exception)
    result, error = get_or_create_event_loop().run_until_complete(h)
    assert error is not None


def test_no_filter():
    """Test filter chain behavior without any filters."""
    chain = filter.FilterChain()
    h = chain.handle(context.Context(), FakeRequest("req", 0), fake_handle)
    result, error = get_or_create_event_loop().run_until_complete(h)
    assert result.msg == "rsp+req" and result.count == 0


@pytest.mark.asyncio
async def test_stream():
    """Test asynchronous stream processing through filter."""
    filter = DefaultFilter()

    async def handler():
        yield FilterStreamResultType(rsp=FakeResponse("rsp", 0))

    ctx = context.Context()
    async for event in filter.run_stream(ctx, FakeRequest("req", 0), handler):
        result: FilterStreamResultType = event
        print(result)
        assert result.rsp.msg == "rsp"
        assert result.rsp.count == 1
