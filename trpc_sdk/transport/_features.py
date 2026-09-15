# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Runtime accessors for transport feature switches.

Call these at the point of use. Do not cache the return value as a module-level
constant: the uvloop flag is set while ``trpc_sdk.common`` installs the loop
policy, so a snapshot taken during package load can predate it.

This module used to own the ``CDispatcher`` singleton as well. That went away
with the native dispatcher itself: no process accepts connections in C any more,
so nothing needs to ask a native connection table for the peer address of an fd
Python never accepted. The archived implementation is under
``trpc_sdk/old_server``.
"""

from __future__ import annotations


def is_uvloop_enabled() -> bool:
    """Return whether the process event-loop policy is uvloop.

    Asks :func:`trpc_sdk.common.use_uvloop` each call. That flag is True only
    when ``TRPC_UVLOOP=1`` and the ``uvloop`` package imported successfully.
    The import stays inside the function so the switch is never frozen at
    package-load time.

    Returns:
        True if uvloop is the current event-loop policy.
    """
    from trpc_sdk.common import use_uvloop

    return bool(use_uvloop())
