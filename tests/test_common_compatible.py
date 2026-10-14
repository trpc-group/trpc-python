# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.
"""Test asyncio compatibility helpers."""

import sys

import pytest

from trpc_sdk.common import compatible


@pytest.mark.asyncio
async def test_wait_for_by_loop_pops_loop_on_py310(monkeypatch):
    async def _ready():
        return 42

    monkeypatch.setattr(compatible, "PY_310", True)
    result = await compatible.wait_for_by_loop(_ready(), timeout=1, loop=object())
    assert result == 42


@pytest.mark.asyncio
async def test_sleep_by_loop_legacy_branch(monkeypatch):
    """Force the pre-3.10 path that still passes loop= to asyncio.sleep."""
    calls = {}

    async def fake_sleep(second, loop=None):
        calls["second"] = second
        calls["loop"] = loop

    monkeypatch.setattr(compatible, "PY_310", False)
    monkeypatch.setattr(compatible.asyncio, "sleep", fake_sleep)
    marker = object()
    await compatible.sleep_by_loop(0.5, loop=marker)
    assert calls == {"second": 0.5, "loop": marker}


@pytest.mark.asyncio
async def test_sleep_by_loop_py310_branch(monkeypatch):
    monkeypatch.setattr(compatible, "PY_310", True)
    await compatible.sleep_by_loop(0, loop=object())


def test_create_object_with_loop_pops_loop(monkeypatch):
    class Dummy:
        def __init__(self, value, loop=None):
            self.value = value
            self.loop = loop

    monkeypatch.setattr(compatible, "PY_310", True)
    obj = compatible.create_object_with_loop(Dummy, 1, loop=object())
    assert obj.value == 1
    assert obj.loop is None

    monkeypatch.setattr(compatible, "PY_310", False)
    marker = object()
    obj2 = compatible.create_object_with_loop(Dummy, 2, loop=marker)
    assert obj2.loop is marker


@pytest.mark.skipif(sys.version_info >= (3, 8), reason="TransportSocket fallback only on <3.8")
def test_transport_socket_fallback_pre_38():
    assert compatible.TransportSocket is not None
