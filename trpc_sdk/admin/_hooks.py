# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""User callbacks run when the admin endpoints start.

The lists are consumed by :meth:`~trpc_sdk.admin.AdminPlugin.on_master_start`
and :meth:`~trpc_sdk.admin.AdminPlugin.on_worker_start`, and only when
``server.admin`` is configured. Master callbacks run once in the root process
after every child is forked; worker callbacks run in every worker process,
never in task processes. Register callbacks before ``Server.serve`` forks.
"""

from __future__ import annotations

from typing import Callable
from typing import List

on_master_starts: List[Callable] = []
"""Callbacks invoked as ``func(routes, configer)`` before the admin HTTP server starts."""

on_worker_starts: List[Callable] = []
"""Callbacks invoked as ``func(configer)`` before a worker's admin WS client starts."""


def on_master_start(func: Callable) -> Callable:
    """Register a callback for the admin HTTP server side.

    Args:
        func: Called as ``func(routes, configer)`` with the admin server's
            aiohttp route table and command registry, so it can add routes
            or commands before the server runs.

    Returns:
        ``func`` unchanged, so this can be used as a decorator.
    """
    on_master_starts.append(func)
    return func


def on_worker_start(func: Callable) -> Callable:
    """Register a callback for the worker-side admin WS client.

    Args:
        func: Called as ``func(configer)`` with the client's command
            registry, so it can add commands before the client connects.

    Returns:
        ``func`` unchanged, so this can be used as a decorator.
    """
    on_worker_starts.append(func)
    return func
