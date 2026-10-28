# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""
SSE (Server-Sent Events) Client Example for tRPC Python

This module demonstrates how to use the tRPC SSE client to:
1. Connect to an SSE server endpoint
2. Receive and process real-time event streams
3. Handle both direct event streaming and iterable event sources
"""

import argparse
import asyncio

from trpc_sdk.abc import TrpcContext as Context
from trpc_sdk.client import Options
from trpc_sdk.http import SSEClient


def sse_url(port: int = 8080) -> str:
    """Return the SSE endpoint on localhost.

    Args:
        port: TCP port of the hellosse server.

    Returns:
        The ``/sse`` URL.
    """
    return f"http://127.0.0.1:{port}/sse"


async def test_http_client_sse(port: int = 8080):
    """Test basic SSE client functionality with direct event streaming.

    Demonstrates:
    - Initializing SSE client
    - Making GET request to SSE endpoint
    - Processing incoming events in real-time

    Args:
        port: TCP port of the SSE server. Defaults to 8080.
    """
    # Initialize SSE client instance
    http_sse_cli = SSEClient()
    # Create request context
    ctx = Context()
    # Configure request options (10s timeout)
    options = [Options().set_timeout(10000)]
    # Stream events directly from the server
    async for event in http_sse_cli.request_events(ctx, "GET", sse_url(port), options=options):
        # Print each received event's payload
        print(f"test_http_client_sse got body: {event.data} \n")


async def test_http_client_with_sse_iter(port: int = 8080):
    """Test SSE client with iterable event sources.

    Demonstrates:
    - Getting multiple event sources from server
    - Iterating through each source's events
    - Handling nested event streams

    Args:
        port: TCP port of the SSE server. Defaults to 8080.
    """
    # Initialize SSE client instance
    http_sse_cli = SSEClient()
    # Create request context
    ctx = Context()
    # Configure request options (10s timeout)
    options = [Options().set_timeout(10000)]
    # Get iterable event sources from server
    async for event_source in http_sse_cli.sse_iter_events(ctx, "GET", sse_url(port), options=options):
        # Process events from each individual source
        async for sse in event_source.aiter_sse():
            # Print each received event's payload
            print(f"test_http_client_with_sse_iter got body: {sse.data} \n")


def main() -> None:
    """Read both event shapes from ``--port`` (default 8080)."""
    parser = argparse.ArgumentParser(description="SSE client for the hellosse example")
    parser.add_argument("--port", type=int, default=8080, help="server port (default: 8080)")
    args = parser.parse_args()
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    futures = asyncio.gather(test_http_client_sse(args.port), test_http_client_with_sse_iter(args.port))
    loop.run_until_complete(futures)


if __name__ == "__main__":
    main()
