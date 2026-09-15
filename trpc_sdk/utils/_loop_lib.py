# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Event-loop policy helpers for the tRPC process.

:data:`USE_UVLOOP` is set while this module is imported, based on
``TRPC_UVLOOP=1`` and a successful ``uvloop`` import. Callers must read
the flag at the point of use (see :func:`trpc_sdk.transport.is_uvloop_enabled`);
caching it as another module-level constant can snapshot the value
before this module has run.
"""

from __future__ import annotations

import asyncio
import os

asynclib = asyncio

# True only when TRPC_UVLOOP=1 and uvloop imported successfully.
USE_UVLOOP = False

if os.environ.get('TRPC_UVLOOP', '0') == '1':
    try:
        import uvloop
        asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
        asynclib = uvloop
        USE_UVLOOP = True
    except ImportError:
        asynclib = asyncio


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

    Returns:
        The thread's current event loop.
    """
    try:
        return asyncio.get_event_loop()
    except RuntimeError:
        loop = asynclib.new_event_loop()
        asyncio.set_event_loop(loop)
        return loop
