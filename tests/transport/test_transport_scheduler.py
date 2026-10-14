# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.
"""Read-only query of the unary scheduler hooks."""

from __future__ import annotations

import pytest

from trpc_sdk.transport import _scheduler
from trpc_sdk.transport import has_custom_scheduler
from trpc_sdk.transport import register_dispatcher_worker_func


@pytest.fixture(autouse=True)
def _restore_hooks(monkeypatch):
    monkeypatch.setattr(_scheduler, "_dispatcher_worker_func", None)
    monkeypatch.setattr(_scheduler, "_worker_scheduler_func", None)


def test_no_hook_registered():
    assert has_custom_scheduler() is False


@pytest.mark.parametrize("is_scheduler", [False, True])
def test_either_hook_counts(is_scheduler):
    register_dispatcher_worker_func(lambda _: 0, is_scheduler=is_scheduler)
    assert has_custom_scheduler() is True
    register_dispatcher_worker_func(None, is_scheduler=is_scheduler)
    assert has_custom_scheduler() is False
