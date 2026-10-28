# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.
"""Task IPC that replaced the removed task-stream service.

A task child no longer speaks a private stream protocol. The worker encodes a
body with the task codec, and a stream the handler owns is a normal
``ServerStream``.
"""

import pytest

from trpc_sdk.abc import TrpcContext
from trpc_sdk.abc import get_message
from trpc_sdk.exceptions import ServerDecodeError
from trpc_sdk.serialization import Body
from trpc_sdk.serialization import SerializationType
from trpc_sdk.server import ServiceOption
from trpc_sdk.stream import ServerStream
from trpc_sdk.task import TASK_CONN
from trpc_sdk.task import TaskWorkerConnection
from trpc_sdk.task import inner_decode_task_req
from trpc_sdk.task import inner_encode_task_req
from trpc_sdk.utils import now_ms


@pytest.mark.asyncio
async def test_task_frame_round_trip_and_server_stream_recv():
    payload = {"kind": "bidi", "data": b"task data"}
    frame = inner_encode_task_req(7, int(now_ms()), 1000, payload)
    pkg_id, _recv, timeout, obj = inner_decode_task_req(frame)
    assert (pkg_id, timeout, obj) == (7, 1000, payload)

    received = []

    def _capture(index, package):
        received.append((index, package))

    import trpc_sdk.task._connection as task_connection
    original = task_connection.receive_pkg_from_worker
    task_connection.receive_pkg_from_worker = _capture
    protocol = TaskWorkerConnection(app=None, index=2)
    try:
        protocol.connection_made(type("Pipe", (), {"is_closing": lambda self: False, "close": lambda self: None})())
        protocol.data_received(frame)
        assert received == [(2, frame)]
        protocol.data_received(b"not-a-frame")
        assert received == [(2, frame)]
    finally:
        task_connection.receive_pkg_from_worker = original
        TASK_CONN.pop(2, None)

    opts = ServiceOption()
    opts.set_protocol("trpc")
    opts.current_serialization_type = SerializationType.Noop
    ctx = TrpcContext()
    get_message(ctx).stream_id = 1
    server_stream = ServerStream(ctx, 1, opts)
    body = Body()
    server_stream.push_to_recv_queue((payload["data"], None))
    await server_stream.recv_msg(body)
    assert body.data == b"task data"


def test_short_frame_is_not_a_task_request():
    with pytest.raises(ServerDecodeError):
        inner_decode_task_req(b"\x00" * 8)
