# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.
"""ConnWriter: write on the owning loop's transport from any thread."""

from __future__ import annotations

import asyncio
import threading

from tests.protocol._fakes import FakeTransport
from trpc_sdk.transport import ConnWriter
from trpc_sdk.transport._server import _conn_writer


def test_still_importable_from_runtime_package():
    from trpc_sdk.runtime import ConnWriter as RuntimeConnWriter  # pylint: disable=import-outside-toplevel
    assert RuntimeConnWriter is ConnWriter


async def test_owner_thread_writes_directly():
    transport = FakeTransport()
    writer = ConnWriter(asyncio.get_running_loop(), transport)
    writer.write(b"a")
    assert transport.written == [b"a"]


async def test_other_thread_posts_to_the_owning_loop():
    transport = FakeTransport()
    writer = ConnWriter(asyncio.get_running_loop(), transport)
    writer_thread = threading.Thread(target=writer.write, args=(b"b",))
    writer_thread.start()
    writer_thread.join()
    # Not written from the other thread; only once this loop turns.
    assert transport.written == []
    await asyncio.sleep(0)
    assert transport.written == [b"b"]


async def test_writes_after_close_are_dropped_and_counted(monkeypatch):
    drops = []
    monkeypatch.setattr(_conn_writer, "report_attr", lambda name, value: drops.append(name))
    transport = FakeTransport()
    writer = ConnWriter(asyncio.get_running_loop(), transport)
    writer.close()
    assert writer.closed
    writer.write(b"c")
    assert transport.written == []
    assert drops == ["trpc_conn_writer_drop"]
