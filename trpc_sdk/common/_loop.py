# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.
"""Event loop module.

This module provides a function to get or create an event loop.
"""

import asyncio


def get_or_create_event_loop():
    """Return the event loop attached to the current thread, creating one if needed.

    Behavior across Python versions:
      * Python 3.7 ~ 3.13:
          ``asyncio.get_event_loop()`` works as before. In 3.10+ it emits a
          DeprecationWarning when no running loop exists, but still returns
          a (newly created) loop
      * Python 3.14+:
          ``asyncio.get_event_loop()`` raises ``RuntimeError`` when there is
          no current event loop in the calling thread. Fall back to
          ``new_event_loop()`` + ``set_event_loop()`` to mimic the legacy
          auto-create behavior, keeping every call site backwards compatible.
          The new loop follows the installed policy, so it is a uvloop loop
          once :func:`trpc_sdk.utils.get_async_lib` has installed uvloop.
    """
    try:
        return asyncio.get_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        return loop
