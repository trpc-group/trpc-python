# -*- coding: utf-8 -*-
"""Test server task"""

import asyncio
import socket
import time
import traceback

from functools import partial

import pytest

from tests.common_util import create_test_connection
from trpc_sdk.codec.message import get_message
from trpc_sdk.codec.serialization import SerializationType
from trpc_sdk.codec.task_stream_codec import TaskStreamClientCodec
from trpc_sdk.context import Context
from trpc_sdk.server.option import ServiceOption
from trpc_sdk.stream.task_service import BaseTaskStreamServiceHandler
from trpc_sdk.stream.task_service import TaskStreamServiceAdapter
from trpc_sdk.task.task_util_c import inner_encode_task_req
from trpc_sdk.transport.defines import TASK_CONN
from trpc_sdk.transport.server_task import TaskConnection
from trpc_sdk.transport.server_task import TaskServerStreamConnectionHandler


class MockTaskConnection(TaskConnection):
    """Mock task connection"""

    def __init__(self, app, index, on_con_lost=None):
        super().__init__(app, index)
        self.on_con_lost: asyncio.Future = on_con_lost

    def connection_lost(self, exc):
        super().connection_lost(exc)
        self.on_con_lost.set_result(True)


@pytest.mark.asyncio
async def test_trpc_server_stream_transport():
    """Test trpc server stream"""

    async def test_cb(loop: asyncio.BaseEventLoop, wsock: socket.socket):
        data = inner_encode_task_req(1, time.time() * 1000, 1000, "abc")
        loop.call_soon(wsock.send, data)
        await asyncio.sleep(1)
        loop.call_soon(wsock.send, b'123xxxxxxxxxxxxxxxxxx')
        await asyncio.sleep(1)
        TASK_CONN[1].close()

    handler = partial(MockTaskConnection, app=None, index=1)
    await create_test_connection(handler, test_cb)


class MockTaskStreamServiceHandler(BaseTaskStreamServiceHandler):
    """Task stream service"""

    def __init__(self) -> None:
        options = ServiceOption()
        options.current_serialization_type = SerializationType.Noop
        super().__init__(options=options)

    def create_req_obj(self) -> object:
        return str()

    def create_rsp_obj(self) -> object:
        return str()

    async def bidi_method_handle(self, stream):
        stream_method = self._create_bidi_stream_method(stream)
        while True:
            try:
                req = await stream_method.recv()
                print(f"task recv request is : {req}")
            except EOFError as err:
                print(f"recv done error {err}")
                return
            except Exception as err:  # pylint: disable=broad-except
                print(f"recv err: {err}, {traceback.format_stack()}")
            # business
            rsp = f"{req} rsp is task data"
            await stream_method.send(rsp)


_TASK_STREAM = None


class MockTaskStreamConnection(TaskServerStreamConnectionHandler):
    """Mock task connection"""

    def __init__(self, service_adapter, loop, on_con_lost):
        super().__init__(service_adapter, loop)
        self.on_con_lost: asyncio.Future = on_con_lost
        global _TASK_STREAM
        _TASK_STREAM = self

    def connection_lost(self, exc):
        super().connection_lost(exc)
        self.on_con_lost.set_result(True)


@pytest.mark.asyncio
async def test_task_server_stream_transport():
    """Test trpc server stream"""
    loop = asyncio.get_running_loop()
    adapter = TaskStreamServiceAdapter(task_service=MockTaskStreamServiceHandler())
    handler = partial(MockTaskStreamConnection, service_adapter=adapter, loop=loop)

    async def test_cb(loop: asyncio.BaseEventLoop, wsock: socket.socket):
        codec = TaskStreamClientCodec()
        context = Context()
        msg = get_message(context)
        msg.frame_head.frame_state = 1
        loop.call_soon(wsock.send, codec.encode(msg, b"123"))
        await asyncio.sleep(1)
        loop.call_soon(wsock.send, b'123xxxxxxxxxxxxxxxxxx')
        await asyncio.sleep(1)
        _TASK_STREAM._transport.close()

    await create_test_connection(handler, test_cb)
