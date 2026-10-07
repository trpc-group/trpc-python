# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Unix timestamps used by RPCZ, the client, and transport timeouts.

:func:`now_us` and :func:`now_ms` are bound at import time. When
``time.time_ns`` exists (Python 3.7+), both divide integer nanoseconds
so the result has no ``time.time()`` float rounding. The ``else``
branch is kept for interpreters without ``time_ns``.
"""

from __future__ import annotations

import time

# Resolved once at import so each call does not repeat ``getattr``.
_time_ns = getattr(time, "time_ns", None)

if _time_ns is not None:

    def now_us() -> int:
        """Return the current Unix timestamp in microseconds.

        Returns:
            ``time.time_ns() // 1000``.
        """
        return int(_time_ns() // 1000)

    def now_ms() -> int:
        """Return the current Unix timestamp in milliseconds.

        Returns:
            ``time.time_ns() // 1_000_000``.
        """
        return int(_time_ns() // 1_000_000)

else:

    def now_us() -> int:
        """Return the current Unix timestamp in microseconds.

        Returns:
            ``int(time.time() * 1_000_000)`` when ``time_ns`` is missing.
        """
        return int(time.time() * 1_000_000)

    def now_ms() -> int:
        """Return the current Unix timestamp in milliseconds.

        Returns:
            ``int(time.time() * 1000)`` when ``time_ns`` is missing.
        """
        return int(time.time() * 1000)
