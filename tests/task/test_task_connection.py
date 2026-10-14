# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.
"""Tests for the task-side end of the worker <-> task pipe."""

from __future__ import annotations

import asyncio
import socket
import time

import pytest

from trpc_sdk.task import TASK_CONN
from trpc_sdk.task import TaskWorkerConnection
from trpc_sdk.task import inner_encode_task_req
from trpc_sdk.task import _connection as task_connection


@pytest.mark.asyncio
async def test_task_worker_connection_dispatches_frames(monkeypatch):
    """A complete frame reaches the task runtime; garbage does not break the pipe."""
    received = []
    monkeypatch.setattr(task_connection, "receive_pkg_from_worker",
                        lambda index, package: received.append((index, package)))

    loop = asyncio.get_running_loop()
    rsock, wsock = socket.socketpair()
    transport, protocol = await loop.create_connection(lambda: TaskWorkerConnection(app=None, index=1), sock=rsock)
    try:
        assert TASK_CONN[1] is transport

        frame = inner_encode_task_req(1, int(time.time() * 1000), 1000, "abc")
        wsock.send(frame)
        await asyncio.sleep(0.1)
        assert received == [(1, frame)]

        wsock.send(b'123xxxxxxxxxxxxxxxxxx')
        await asyncio.sleep(0.1)
        assert len(received) == 1
    finally:
        wsock.close()
        await asyncio.sleep(0.1)
        assert protocol.aio_connection.is_closing()
        TASK_CONN.pop(1, None)
