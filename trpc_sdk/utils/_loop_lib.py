# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Event-loop policy helpers for the tRPC process.

Whether uvloop is used comes from the ``global.enable_uvloop`` key of the
framework YAML. Callers pass that flag to :func:`get_async_lib` at the point of
use; nothing is decided at import time, because the YAML is loaded long after
this module.
"""

from __future__ import annotations

import asyncio

from trpc_sdk.config import TrpcConfig

# Last result of get_async_lib(); read it at call time, not via a from-import.
asynclib = asyncio


def get_async_lib(is_uv_loop: bool = False):
    """Return the module whose ``new_event_loop`` creates this process's loops.

    When ``is_uv_loop`` is True and uvloop is importable, the uvloop event-loop
    policy is installed on first use. Later calls keep the installed policy,
    since replacing it drops every thread's ``set_event_loop`` binding.

    Args:
        is_uv_loop: True to use uvloop, typically
            ``get_global_config().enable_uvloop``.

    Returns:
        ``uvloop`` if requested and installed, otherwise ``asyncio``.
    """
    global asynclib  # pylint: disable=global-statement
    asynclib = asyncio
    if is_uv_loop:
        try:
            import uvloop
            if not isinstance(asyncio.get_event_loop_policy(), uvloop.EventLoopPolicy):
                asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
            asynclib = uvloop
        except ImportError:
            asynclib = asyncio
    return asynclib


def is_uvloop_enabled() -> bool:
    """Return whether ``global.enable_uvloop`` is on and uvloop is installed.

    Reads the config on every call, so call it at the point of use rather
    than caching the result while the YAML may not be loaded yet.

    Returns:
        True if the config asks for uvloop and the ``uvloop`` package imports.
    """
    if not TrpcConfig.global_config().enable_uvloop:
        return False
    try:
        import uvloop  # noqa: F401  # pylint: disable=import-outside-toplevel,unused-import
    except ImportError:
        return False
    return True


def get_or_create_event_loop():
    """Return the event loop attached to the current thread, creating one if needed.

    Behavior across Python versions:

      * Python 3.7 ~ 3.13:
          ``asyncio.get_event_loop()`` works as before. In 3.10+ it emits a
          DeprecationWarning when no running loop exists, but still returns
          a (newly created) loop.
      * Python 3.14+:
          ``asyncio.get_event_loop()`` raises ``RuntimeError`` when there is
          no current event loop in the calling thread. Fall back to
          ``new_event_loop()`` + ``set_event_loop()`` to mimic the legacy
          auto-create behavior, keeping every call site backwards compatible.
          The new loop comes from :data:`asynclib`, so it is a uvloop loop
          once :func:`get_async_lib` has selected uvloop.

    Returns:
        The thread's current event loop.
    """
    try:
        return asyncio.get_event_loop()
    except RuntimeError:
        loop = asynclib.new_event_loop()
        asyncio.set_event_loop(loop)
        return loop
