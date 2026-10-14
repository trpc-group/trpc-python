# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.
"""Test transport module"""
import asyncio
import os
import pickle
import signal

import pytest

from tests.transport_util import TestClientConn
from trpc_sdk.server import server_engine
from trpc_sdk.server.option import ServerOption
from trpc_sdk.server.task import CTaskProcess
from trpc_sdk.server.task import Task
from trpc_sdk.server.task import TaskProcess
from trpc_sdk.stream.task_service import BaseTaskStreamServiceHandler
from trpc_sdk.task.task import TaskStreamHandler
from trpc_sdk.transport import server_task
from trpc_sdk.transport.server_conn_info import inner_encode

# server_task
global_task_info = b''


def _handle_task(task):
    global global_task_info
    global_task_info = task


def _init_cb():
    return


class _MockTaskProcess(TaskProcess):
    """Mock task process"""

    def run(self):
        pass


def test_server_task():
    """Test server task"""
    task_info = Task()
    task_info.task_handle = [None, _handle_task]
    task_info.handle = TaskProcess
    engine = server_engine.TrpcServerEngine(ServerOption())
    engine.opts = ServerOption()
    engine.opts.task_batch_size = 1
    engine.opts.task_queue_size = 10000
    engine.opts.task_wait_interval = 100
    app = _MockTaskProcess(task_info, engine)

    task_conn = server_task.TaskConnection(app, _handle_task)
    task_conn.connection_made(TestClientConn())
    task_conn.data_received(inner_encode(pickle.dumps(b'123')))

    assert global_task_info == b''

    app.block = False
    try:
        app.run()
    except Exception:  # pylint: disable=broad-except
        assert False


@pytest.mark.asyncio
async def test_server_task_with_pipe():
    """Test server task"""
    task_info = Task()
    task_info.task_handle = [None, _handle_task]
    task_info.handle = TaskProcess
    engine = server_engine.TrpcServerEngine(ServerOption())
    engine.opts = ServerOption()
    engine.opts.task_batch_size = 1
    engine.opts.task_queue_size = 10000
    engine.opts.task_wait_interval = 100
    app = TaskProcess(task_info, engine)
    app.start()
    await asyncio.sleep(2)
    os.kill(app.pid, signal.SIGTERM)
    print("test_server_task_with_pipe TaskProcess kill")


class MockStreamService(BaseTaskStreamServiceHandler):

    def create_req_obj(self) -> object:
        """Create req object"""
        return str

    def create_rsp_obj(self) -> object:
        """Create rsp object"""
        return str


@pytest.mark.asyncio
async def test_server_task_with_unix():
    """Test server task"""
    task_info = Task()
    task_info.name = "test"
    task_info.task_handle = [None, _handle_task]
    handler = TaskStreamHandler(client=None, service=MockStreamService())
    task_info.task_stream_handler = handler
    task_info.handle = TaskProcess
    engine = server_engine.TrpcServerEngine(ServerOption())
    engine.opts = ServerOption()
    engine.opts.task_batch_size = 1
    engine.opts.task_queue_size = 10000
    engine.opts.task_wait_interval = 100
    app = TaskProcess(task_info, engine)
    app1 = TaskProcess(task_info, engine)
    app.start()
    app1.start()
    await asyncio.sleep(2)
    os.kill(app.pid, signal.SIGTERM)
    os.kill(app1.pid, signal.SIGTERM)
    print("test_server_task_with_unix TaskProcess kill")


class _MockCTaskProcess(CTaskProcess):
    """Mock task process"""

    def run(self):
        pass


def test_server_ctask():
    """Test server task"""
    task_info = Task()
    task_info.task_handle = [None, _handle_task]
    task_info.handle = CTaskProcess
    engine = server_engine.TrpcServerEngine(ServerOption())
    engine.opts = ServerOption()
    engine.opts.task_batch_size = 1
    engine.opts.task_queue_size = 10000
    engine.opts.task_wait_interval = 100
    app = _MockCTaskProcess(task_info, engine)

    task_conn = server_task.TaskConnection(app, _handle_task)
    task_conn.connection_made(TestClientConn())
    task_conn.data_received(inner_encode(pickle.dumps(b'123')))

    assert global_task_info == b''

    app.block = False
    try:
        app.run()
    except Exception:  # pylint: disable=broad-except
        assert False


class _MockModuleCtask:

    class CTask:
        """CTask"""

        def set_task_info(self, task_info):
            """test interface"""
            print(task_info)

        def start(self):
            """test interface"""
            pass


class _MockRegisterTask:

    class RegisterTask:
        """RegisterTask"""

        def __init__(self, task_initializer, _, __):
            """test interface"""
            print(task_initializer)


class _MockDriver:

    def run_c_task_process(self, task_info, codec, task_initializer, task_handle, **kwargs):
        """test interface"""
        pass


def _mock_import_module(name, package=None):
    if name == "trpc_sdk.task.ctask":
        return _MockModuleCtask()
    if name == "trpc_driver":
        return _MockDriver()

    if name == "trpc_sdk.task.python_handler":
        return _MockRegisterTask()


def test_server_c_driver_task(mocker):
    """Test server task"""

    class _MockEngin:

        def __init__(self):
            self.opts = ServerOption()

    mocker.patch('importlib.import_module', _mock_import_module)
    task_info = Task()
    task_info.task_handle = [_init_cb, _handle_task]
    task_info.c_task_driver_params = {"log_level": 4, "log_file": "./test.log"}
    ctask = CTaskProcess(task_info=task_info, engine=_MockEngin())
    ctask.run()
    task_info.c_task_driver_params = {}
    ctask = CTaskProcess(task_info=task_info, engine=_MockEngin())
    ctask.run()
