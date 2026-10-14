# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.
"""Tests for :func:`trpc_sdk.utils.is_uvloop_enabled`."""

from __future__ import annotations

import sys
import types

import pytest

from trpc_sdk.config import TrpcConfig
from trpc_sdk.utils import is_uvloop_enabled


@pytest.fixture
def enable_uvloop(monkeypatch):
    def _set(enabled: bool) -> None:
        monkeypatch.setattr(TrpcConfig.global_config(), "enable_uvloop", enabled)

    return _set


def test_disabled_in_config(enable_uvloop) -> None:
    enable_uvloop(False)
    assert is_uvloop_enabled() is False


def test_enabled_but_uvloop_missing(enable_uvloop, monkeypatch) -> None:
    enable_uvloop(True)
    monkeypatch.setitem(sys.modules, "uvloop", None)
    assert is_uvloop_enabled() is False


def test_enabled_and_uvloop_installed(enable_uvloop, monkeypatch) -> None:
    enable_uvloop(True)
    monkeypatch.setitem(sys.modules, "uvloop", types.ModuleType("uvloop"))
    assert is_uvloop_enabled() is True
