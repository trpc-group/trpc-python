# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.
"""Test time_utils fallback when time.time_ns is unavailable."""

import importlib
import sys
import time


def test_time_utils_fallback_without_time_ns(monkeypatch):
    """Cover the else branch that uses time.time()."""
    monkeypatch.delattr(time, "time_ns", raising=False)
    # Drop cached module so reload re-evaluates getattr(time, "time_ns", None).
    sys.modules.pop("trpc_sdk.utils._time_utils", None)
    time_utils = importlib.import_module("trpc_sdk.utils._time_utils")

    assert time_utils._time_ns is None
    us = time_utils.now_us()
    ms = time_utils.now_ms()
    assert isinstance(us, int) and us > 0
    assert isinstance(ms, int) and ms > 0
    assert abs(us // 1000 - ms) < 50

    # Restore the normal (time_ns) implementation for other tests.
    monkeypatch.undo()
    sys.modules.pop("trpc_sdk.utils._time_utils", None)
    restored = importlib.import_module("trpc_sdk.utils._time_utils")
    assert restored._time_ns is not None
    assert restored.now_us() > 0
    assert restored.now_ms() > 0
