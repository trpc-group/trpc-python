# -*- coding: utf-8 -*-
"""Test transport module"""
import asyncio
import os
import signal
import time

from unittest import mock

import pytest

from tests.transport_util import TestClientConn
from tests.transport_util import TestPipeSocket
from trpc_sdk.abc.transport import BaseServerTransportOption
from trpc_sdk.server import server_engine
from trpc_sdk.server import server_pipe_connection
from trpc_sdk.server.option import ServerOption
from trpc_sdk.server.option import ServiceOption
from trpc_sdk.server.service import Service
from trpc_sdk.server.worker import CWorkerProcess
from trpc_sdk.server.worker import Worker
from trpc_sdk.server.worker import WorkerProcess
from trpc_sdk.transport import server_worker
from trpc_sdk.utils import get_or_create_event_loop
from trpc_sdk.utils.loop_lib import asynclib
from trpc_sdk.utils.multi_process import get_current_process_var
from trpc_sdk.utils.multi_process import get_process_name
from trpc_sdk.utils.multi_process import get_process_type
from trpc_sdk.utils.multi_process import is_dispatcher_process
from trpc_sdk.utils.multi_process import is_task_process
from trpc_sdk.utils.multi_process import is_timer_process
from trpc_sdk.utils.multi_process import is_worker_process
from trpc_sdk.utils.multi_process import set_current_process_var


class _TestCWorkerProcess(CWorkerProcess):

    def start_workers(self):
        pass


class _TestWorkerProcess(WorkerProcess):

    def start_workers(self):
        pass


class _FakeInnerParser:

    def feeds_data(self, _):
        """Feed data"""
        pass

    def read(self):
        """Read data"""
        pass


def test_server_worker():
    """Test server worker"""
    engine = server_engine.TrpcServerEngine(ServerOption())
    service = Service(service_option=ServiceOption())
    engine.process_info.add_handler(service.handle)
    engine.process_info.add_timeout(1000)

    worker = Worker()
    worker.pipe_conn = server_pipe_connection.PipeConnection((TestPipeSocket("left"), TestPipeSocket("right")),
                                                             server_pipe_connection.LEFT_PORT)
    app = _TestWorkerProcess(worker, engine)

    future = get_or_create_event_loop().create_future()
    future.set_result(b"test")
    worker_conn = server_worker.WorkerConnection(app, {11174: 0, 8000: 0}, [mock.Mock(return_value=future)], {0: 1000})
    worker_conn.connection_made(TestClientConn())
    worker_conn.loop = get_or_create_event_loop()

    try:
        worker_conn.data_received(
            b'"\x05\xa8\x00\x00\x00!\x00\x00\x00\xa6+\x00\x00\x01\x00\x00\x00+\x00\xbf\xd4x\x01\x00'
            b'\x00\x04\x00\t3\x15\xc3\t0\x00\x00\x00\x00\x00\x88\x00q\x00\x00\x00\x00\x00\x00\x18\x0e'
            b' \xb8\x17*\x08test.eab2*trpc.trpc_python_group.echo_server.Greeter:4/'
            b'trpc.trpc_python_group.echo_server.Greeter/SayHello\n\x05hello')
    except:  # pylint: disable=bare-except
        assert False
    try:
        worker_conn.data_received(
            b'"\x05\x86\x00\x00\x00\x0e\x00\x00\x00@\x1f\x00\x00\x01\x00\x00\x005\xa7+\xdd~\x01\x00\x00\x04\x00\x7f\x00'
            b'\x00\x01\t0\x01\x01\x00\x00\x00f\x00\x00\x00\x00\x00d\x00'
            b'\x00\nP\n\x17trpc_sdk.app.server.service\x12\x13trpc_sdk.stream.Greeter\x1a /trpc_sdk.stream.Greeter/SayHelloReq\x18'
            b'\xff\xff\x03')
    except:  # pylint: disable=bare-except
        assert False

    try:
        worker_conn.data_received(
            bytes.fromhex("2205860000000e000000401f000003000000907637dd7e01000004007f00000109300201000000660000000000"
                          "6600000a500a17747270632e6170702e7365727665722e736572766963651213747270632e73747265616d2e477"
                          "265657465721a202f747270632e73747265616d2e477265657465722f53617948656c6c6f416c6c18ffff03"))
    except:  # pylint: disable=bare-except
        assert False

    worker_conn.data_received(b'aaaaaaaaaaaa')

    worker_conn.connection_lost(None)

    app.block = False
    try:
        app.run()
    except Exception:  # pylint: disable=broad-except
        assert False


def test_start_cworkers():
    """Test start cworker"""
    engine = server_engine.TrpcServerEngine(ServerOption())
    worker = Worker()
    worker.pipe_conn = server_pipe_connection.PipeConnection((TestPipeSocket("left"), TestPipeSocket("right")),
                                                             server_pipe_connection.LEFT_PORT)
    engine.worker_process_list = [worker] * 8
    app = CWorkerProcess(worker, engine)
    app.loop = asynclib.new_event_loop()

    async def cor():
        pass

    app.loop.create_datagram_endpoint = mock.Mock(return_value=cor())
    app.start_workers()


def test_cserver_worker():
    """Test sever cworker"""
    engine = server_engine.TrpcServerEngine(ServerOption())
    service = Service(service_option=ServiceOption())
    engine.process_info.add_handler(service.handle)
    engine.process_info.add_timeout(1000)

    future = get_or_create_event_loop().create_future()
    future.set_result(b"test")
    service_handle = mock.Mock(return_value=future)
    worker = Worker()
    worker.pipe_conn = server_pipe_connection.PipeConnection((TestPipeSocket("left"), TestPipeSocket("right")),
                                                             server_pipe_connection.LEFT_PORT)
    engine.worker_process_list = [worker] * 8
    app = _TestCWorkerProcess(worker, engine)
    set_current_process_var("test", "test")
    get_current_process_var("test")
    get_process_type()
    get_process_name()
    is_worker_process()
    is_dispatcher_process()
    is_task_process()
    is_timer_process()

    worker_conn = server_worker.CWorkerConnection(app, {11174: 0}, [service_handle], {0: 1000})
    worker_conn.loop = get_or_create_event_loop()
    worker_conn.connection_made(TestClientConn())

    worker_conn.checker = _FakeInnerParser()
    worker_conn.checker.feeds_data = mock.Mock(return_value=0)
    worker_conn.checker.read = mock.Mock(return_value="")

    try:
        worker_conn.datagram_received(
            b'"\x05\x00\x00\x89\x00\x00\x00K\x00\x00\x00I\x00\x00\x00\x02\x00\x00\x00\x00\x00\x00\x00'
            b'\x00\x00\x00\x00\x00\x00\x00\x000\x11\xc5\xb5\xf5&\xd8A\x00\x00\x01\x00\x00\x00\x00\x00'
            b'\t0\x00\x00\x00\x00\x00\x89\x00r\x00\x00\x00\x00\x00\x00\x18\x9f\x01 \xb8\x17*\x08'
            b'test.eab2*trpc.trpc_python_group.echo_server.Greeter:4/'
            b'trpc.trpc_python_group.echo_server.Greeter/SayHello\n\x05hello', "")
    except:  # pylint: disable=bare-except
        assert False

    worker_conn.connection_lost(None)

    app.block = False
    try:
        app.run()
    except Exception:  # pylint: disable=broad-except
        assert False


def test_worker_recv_task():
    """Test connection between worker and task"""
    task_conn = server_worker.TaskConnection()
    task_conn.data_received(b'123')
    task_conn.connection_made(None)
    assert task_conn.aio_connection is None
    # test error data
    task_conn.data_received(b'aaaaaaaaaaaaa')
    task_conn.connection_lost(None)


@pytest.mark.asyncio
async def test_server_worker_handle_request():
    """Test server worker"""
    engine = server_engine.TrpcServerEngine(ServerOption())
    service = Service(service_option=ServiceOption())
    engine.process_info.add_handler(service.handle)
    engine.process_info.add_timeout(1000)

    worker = Worker()
    worker.pipe_conn = server_pipe_connection.PipeConnection((TestPipeSocket("left"), TestPipeSocket("right")),
                                                             server_pipe_connection.LEFT_PORT)
    app = _TestWorkerProcess(worker, engine)

    future = get_or_create_event_loop().create_future()
    future.set_result(b"test")
    worker_conn = server_worker.WorkerConnection(app, {11174: 0, 8000: 0}, [mock.Mock(return_value=future)], {0: 1000})
    worker_conn.connection_made(TestClientConn())
    worker_conn.loop = get_or_create_event_loop()

    try:
        worker_conn.data_received(
            b'"\x05\xa8\x00\x00\x00!\x00\x00\x00\xa6+\x00\x00\x01\x00\x00\x00+\x00\xbf\xd4x\x01\x00'
            b'\x00\x04\x00\t3\x15\xc3\t0\x00\x00\x00\x00\x00\x88\x00q\x00\x00\x00\x00\x00\x00\x18\x0e'
            b' \xb8\x17*\x08test.eab2*trpc.trpc_python_group.echo_server.Greeter:4/'
            b'trpc.trpc_python_group.echo_server.Greeter/SayHello\n\x05hello')
    except:  # pylint: disable=bare-except
        assert False

    worker_conn.connection_lost("error")
    app.start()
    await asyncio.sleep(2)
    os.kill(app.pid, signal.SIGTERM)


@pytest.mark.asyncio
async def test_worker_create_service_coro_with_unix_socket():
    """Test _create_service_coro with unix socket type"""
    engine = server_engine.TrpcServerEngine(ServerOption())
    service = Service(service_option=ServiceOption())
    engine.process_info.add_handler(service.handle)
    engine.process_info.add_timeout(1000)

    worker = Worker()
    worker.pipe_conn = server_pipe_connection.PipeConnection((TestPipeSocket("left"), TestPipeSocket("right")),
                                                             server_pipe_connection.LEFT_PORT)
    app = _TestWorkerProcess(worker, engine)

    future = get_or_create_event_loop().create_future()
    future.set_result(b"test")

    # Create transport options with unix socket type
    tran_opt = BaseServerTransportOption()
    tran_opt.socket_type = "unix"
    tran_opt.address = "/tmp/test.sock"

    worker_conn = server_worker.WorkerConnection(app, {8000: 0}, [mock.Mock(return_value=future)], {0: 1000},
                                                 {0: tran_opt})
    worker_conn.connection_made(TestClientConn())
    worker_conn.loop = get_or_create_event_loop()

    # Test with unix socket - this will trigger the unix socket branch in _create_service_coro
    try:
        worker_conn.data_received(
            b'"\x05\xa8\x00\x00\x00!\x00\x00\x00\xa6+\x00\x00\x01\x00\x00\x00+\x00\xbf\xd4x\x01\x00'
            b'\x00\x04\x00\t3\x15\xc3\t0\x00\x00\x00\x00\x00\x88\x00q\x00\x00\x00\x00\x00\x00\x18\x0e'
            b' \xb8\x17*\x08test.eab2*trpc.trpc_python_group.echo_server.Greeter:4/'
            b'trpc.trpc_python_group.echo_server.Greeter/SayHello\n\x05hello')
    except:  # pylint: disable=bare-except
        assert False

    await asyncio.sleep(0.1)
    worker_conn.connection_lost(None)


def test_worker_data_received_process_exception():
    """Test data_received with exception in _create_service_coro to cover line 117-118"""
    engine = server_engine.TrpcServerEngine(ServerOption())
    service = Service(service_option=ServiceOption())
    engine.process_info.add_handler(service.handle)
    engine.process_info.add_timeout(1000)

    worker = Worker()
    worker.pipe_conn = server_pipe_connection.PipeConnection((TestPipeSocket("left"), TestPipeSocket("right")),
                                                             server_pipe_connection.LEFT_PORT)
    app = _TestWorkerProcess(worker, engine)

    future = get_or_create_event_loop().create_future()
    future.set_result(b"test")
    worker_conn = server_worker.WorkerConnection(app, {11174: 0, 8000: 0}, [mock.Mock(return_value=future)], {0: 1000})
    worker_conn.connection_made(TestClientConn())
    worker_conn.loop = get_or_create_event_loop()

    # Mock _create_service_coro to raise an exception
    original_create_service_coro = worker_conn._create_service_coro

    def mock_create_service_coro(packages):
        raise RuntimeError("Mock exception in _create_service_coro")

    worker_conn._create_service_coro = mock_create_service_coro

    # This should trigger the exception handling at line 117-118
    try:
        worker_conn.data_received(
            b'"\x05\xa8\x00\x00\x00!\x00\x00\x00\xa6+\x00\x00\x01\x00\x00\x00+\x00\xbf\xd4x\x01\x00'
            b'\x00\x04\x00\t3\x15\xc3\t0\x00\x00\x00\x00\x00\x88\x00q\x00\x00\x00\x00\x00\x00\x18\x0e'
            b' \xb8\x17*\x08test.eab2*trpc.trpc_python_group.echo_server.Greeter:4/'
            b'trpc.trpc_python_group.echo_server.Greeter/SayHello\n\x05hello')
    except:  # pylint: disable=bare-except
        # Should not raise exception, it should be caught and logged
        assert False

    worker_conn.connection_lost(None)


@pytest.mark.asyncio
async def test_worker_create_service_coro_full_coverage():
    """Test _create_service_coro to ensure full coverage of the method"""
    engine = server_engine.TrpcServerEngine(ServerOption())
    service = Service(service_option=ServiceOption())
    engine.process_info.add_handler(service.handle)
    engine.process_info.add_timeout(2000)

    worker = Worker()
    worker.pipe_conn = server_pipe_connection.PipeConnection((TestPipeSocket("left"), TestPipeSocket("right")),
                                                             server_pipe_connection.LEFT_PORT)
    app = _TestWorkerProcess(worker, engine)

    future = get_or_create_event_loop().create_future()
    future.set_result(b"response_data")

    # Create transport options with tcp socket type
    tran_opt = BaseServerTransportOption()
    tran_opt.socket_type = "tcp"
    tran_opt.address = "127.0.0.1:8000"

    worker_conn = server_worker.WorkerConnection(app, {8000: 0}, [mock.Mock(return_value=future)], {0: 2000},
                                                 {0: tran_opt})
    worker_conn.connection_made(TestClientConn())
    worker_conn.loop = get_or_create_event_loop()

    # Build inner-encoded request with port=8000 matching the addresses dict
    from trpc_sdk.transport.c_server_conn_info import inner_encode_req
    addr_info = b"127.0.0.1:8000|10.0.0.1:9090"
    inner_data = inner_encode_req(1, 8000, 1, int(time.time() * 1000), addr_info, b"hello")
    try:
        worker_conn.data_received(inner_data)
    except:  # pylint: disable=bare-except
        assert False

    # Wait for async task to complete
    await asyncio.sleep(0.2)
    worker_conn.connection_lost(None)
