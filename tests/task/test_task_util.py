# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.
"""Test task util"""
import time
import traceback

from trpc_sdk.codec.serialization import SerializationType
from trpc_sdk.server.option import ServiceOption
from trpc_sdk.stream.server import ServerStream
from trpc_sdk.stream.task_client import BaseTaskStreamClientHandler
from trpc_sdk.stream.task_service import BaseTaskStreamServiceHandler
from trpc_sdk.task.task import Task
from trpc_sdk.task.task import TaskStreamHandler
from trpc_sdk.task.task_util import TaskRequest
from trpc_sdk.task.task_util import TaskResponse
from trpc_sdk.task.task_util import TaskStatusCode
from trpc_sdk.task.task_util import TaskStreamClientMap
from trpc_sdk.task.task_util import register_task_num


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

    async def bidi_method_handle(self, stream: ServerStream):
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


class MockTaskStreamClientHandler(BaseTaskStreamClientHandler):
    """Task stream service"""

    def create_req_obj(self) -> object:
        return str()

    def create_rsp_obj(self) -> object:
        return str()


def test_task_stream():
    """Test task stream"""
    task = Task()
    assert task.task_stream_handler is None
    handler = TaskStreamHandler(client=MockTaskStreamClientHandler(), service=MockTaskStreamServiceHandler())
    task.task_stream_handler = handler
    assert task.task_stream_handler is handler
    TaskStreamClientMap.register_task_stream_client("test", handler.client)
    stream_client = TaskStreamClientMap.get_task_stream_client("test")
    assert stream_client is not None
    register_task_num("aa", 1)
    TaskStreamClientMap.register_task_stream_client("aa:0", handler.client)
    stream_client = TaskStreamClientMap.get_task_stream_client("aa")
    assert stream_client is not None
    stream_client = TaskStreamClientMap.get_task_stream_client("aa")
    assert stream_client is not None


def test_task_req():
    """Test task req"""
    recv_time = time.time() * 1000
    task_req = TaskRequest(1, 1, recv_time, 1, "req")
    assert task_req.is_timeout() is False
    time.sleep(1.5)
    assert task_req.is_timeout() is True


def test_task_req_and_rsp():
    """Test TaskRequest and TaskResponse"""
    req = TaskRequest(0, 0, 0, 0, "127.0.0.1")
    assert req.is_timeout() is False
    rsp = TaskResponse(req, '123')
    rsp.set_status_code(TaskStatusCode.TIMEOUT)
    assert rsp.ret_code == TaskStatusCode.TIMEOUT
