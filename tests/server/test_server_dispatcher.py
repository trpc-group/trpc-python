# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.
"""Test transport module"""
import asyncio
import time

import pytest

from tests.transport_util import TestParser
from tests.transport_util import TestClientConn
from trpc_sdk.server import dispatcher
from trpc_sdk.server import server_engine
from trpc_sdk.server.option import ServerOption
from trpc_sdk.transport import server_dispatcher
from trpc_sdk.transport.settings import SchedulerParams
from trpc_sdk.transport.settings import register_dispatcher_worker_func
from trpc_sdk.utils import get_or_create_event_loop


class _TestDispatcherProcess(dispatcher.DispatcherProcess):
    """Test dispatcher process"""

    def start_dispatchers(self):
        pass


@pytest.mark.asyncio
async def test_conn_idle_check():
    """Test conn idle check"""
    engine = server_engine.TrpcServerEngine(ServerOption())
    engine.process_info.add_address(("127.0.0.1", 8000))
    engine.process_info.add_socket_type("net")
    engine.process_info.add_parser(TestParser())
    dispatcher_obj = dispatcher.Dispatcher()
    app = _TestDispatcherProcess(dispatcher_obj, engine)
    app.loop = get_or_create_event_loop()

    dispatcher_conn = server_dispatcher.DispatcherConnection(app, ('127.0.0.1', 8000), TestParser())
    dispatcher_conn.check_idle_timeout = 50
    dispatcher_conn.last_recv_time = time.time() - 0.06
    dispatcher_conn.connection_made(TestClientConn())
    try:
        await dispatcher_conn.conn_idle_check()
        assert not dispatcher_conn.app.client_connections.get(dispatcher_conn.fd, None)
    except Exception:  # pylint: disable=broad-except
        assert False
    assert True


@pytest.mark.asyncio
async def test_conn_idle_check_lost():
    """Test conn idle check lost"""
    engine = server_engine.TrpcServerEngine(ServerOption())
    engine.process_info.add_address(("127.0.0.1", 8000))
    engine.process_info.add_socket_type("net")
    engine.process_info.add_parser(TestParser())
    dispatcher_obj = dispatcher.Dispatcher()
    app = _TestDispatcherProcess(dispatcher_obj, engine)
    app.loop = get_or_create_event_loop()

    dispatcher_conn = server_dispatcher.DispatcherConnection(app, ('127.0.0.1', 8000), TestParser())
    dispatcher_conn.check_idle_timeout = 50
    dispatcher_conn.last_recv_time = time.time() - 0.06
    dispatcher_conn.connection_made(TestClientConn())
    dispatcher_conn.connection_lost(None)
    try:
        await dispatcher_conn.conn_idle_check()
        assert not dispatcher_conn.app.client_connections.get(dispatcher_conn.fd, None)
    except Exception:  # pylint: disable=broad-except
        assert False
    assert True


def test_server_dispatcher_tcp():
    """Test dispatcher tcp"""
    engine = server_engine.TrpcServerEngine(ServerOption())
    engine.process_info.add_address(("127.0.0.1", 8000))
    engine.process_info.add_socket_type("net")
    engine.process_info.add_parser(TestParser())
    dispatcher_obj = dispatcher.Dispatcher()
    app = _TestDispatcherProcess(dispatcher_obj, engine)

    dispatcher_conn = server_dispatcher.DispatcherConnection(app, ('127.0.0.1', 8000), TestParser())
    worker_conn = server_dispatcher.WorkerConnection(app)

    dispatcher_conn.connection_made(TestClientConn())
    worker_conn.connection_made(TestClientConn())

    assert app.client_connections[1].conn.buf == b''

    app.block = False

    try:
        app.run()
    except Exception:  # pylint: disable=broad-except
        assert False

    try:
        dispatcher_conn.data_received(
            b'\t0\x00\x00\x00\x00\x00\x1f\x00\x02\x00\x00\x00\x00\x00\x00\x18\x01\n\x0bhi: hello: ')
        worker_conn.data_received(app.worker_connections[0].buf)

        dispatcher_conn.connection_lost(None)
        worker_conn.connection_lost(None)
    except Exception:  # pylint: disable=broad-except
        assert False

    # Cover _handle_package write-back path (lines 195-198)
    from trpc_sdk.transport.c_server_conn_info import inner_encode_rsp
    from trpc_sdk.transport.server_dispatcher import ClientConnectionInfo
    client_transport = TestClientConn()
    app.client_connections[1] = ClientConnectionInfo(session_id=1, conn=client_transport)
    worker_conn2 = server_dispatcher.WorkerConnection(app)
    worker_conn2.connection_made(TestClientConn())
    worker_conn2.data_received(inner_encode_rsp(1, 1, 0, b"rsp"))
    assert client_transport.buf == b"rsp"


def test_get_target_worker():
    """Test get_target_worker"""
    data = SchedulerParams(worker_conn_num=1, remote_ip='', session_id=1, stream_id=1)
    assert 0 == server_dispatcher.get_next_worker(data)

    def _dispatcher(index: int):
        print(f"dispatcher index {index}")
        return index

    register_dispatcher_worker_func(_dispatcher)
    data = SchedulerParams(worker_conn_num=1, remote_ip='', session_id=0, stream_id=0)
    assert 1 == server_dispatcher.get_next_worker(data)

    def _dispatcher1(data: SchedulerParams):
        return data.worker_conn_num

    register_dispatcher_worker_func(_dispatcher1, True)
    data = SchedulerParams(worker_conn_num=1, remote_ip='', session_id=0, stream_id=0)
    assert 1 == server_dispatcher.get_next_worker(data)
    # 还原
    register_dispatcher_worker_func(None)
    register_dispatcher_worker_func(None, True)


@pytest.mark.asyncio
async def test_dispatcher_connection_made_exception():
    """Test dispatcher connection_made with exception to cover line 78"""
    engine = server_engine.TrpcServerEngine(ServerOption())
    engine.process_info.add_address(("127.0.0.1", 8000))
    engine.process_info.add_socket_type("net")
    engine.process_info.add_parser(TestParser())
    dispatcher_obj = dispatcher.Dispatcher()
    app = _TestDispatcherProcess(dispatcher_obj, engine)
    app.loop = get_or_create_event_loop()

    dispatcher_conn = server_dispatcher.DispatcherConnection(app, ('127.0.0.1', 8000), TestParser())

    # Create a mock transport that raises exception when getting extra info
    mock_transport = TestClientConn()
    original_get_extra_info = mock_transport.get_extra_info

    def mock_get_extra_info(key):
        if key == "socket":
            raise RuntimeError("Mock socket error")
        return original_get_extra_info(key)

    mock_transport.get_extra_info = mock_get_extra_info

    # This should trigger the exception handling at line 78
    dispatcher_conn.connection_made(mock_transport)

    # Verify the connection was closed due to exception
    assert dispatcher_conn.fd not in app.client_connections


def test_dispatcher_data_received_parser_exception():
    """Test dispatcher data_received with parser exception to cover line 125"""
    engine = server_engine.TrpcServerEngine(ServerOption())
    engine.process_info.add_address(("127.0.0.1", 8000))
    engine.process_info.add_socket_type("net")
    engine.process_info.add_parser(TestParser())
    dispatcher_obj = dispatcher.Dispatcher()
    app = _TestDispatcherProcess(dispatcher_obj, engine)
    app.loop = get_or_create_event_loop()

    class MockParse():

        def read(self):
            raise RuntimeError("Mock parser fatal error")

        def feeds_data(self, data):
            pass

        def reset(self):
            pass

    dispatcher_conn = server_dispatcher.DispatcherConnection(app, ('127.0.0.1', 8000), TestParser(MockParse))
    dispatcher_conn.connection_made(TestClientConn())

    # Feed some data to trigger the exception at line 125
    dispatcher_conn.data_received(b'some_data')

    # Verify the connection was closed due to exception
    assert dispatcher_conn.fd not in app.client_connections


def test_dispatcher_data_received_worker_write_exception():
    """Test dispatcher data_received with worker write exception to cover line 131"""
    engine = server_engine.TrpcServerEngine(ServerOption())
    engine.process_info.add_address(("127.0.0.1", 8000))
    engine.process_info.add_socket_type("net")
    engine.process_info.add_parser(TestParser())
    dispatcher_obj = dispatcher.Dispatcher()
    app = _TestDispatcherProcess(dispatcher_obj, engine)
    app.loop = get_or_create_event_loop()

    test_data = [
        b'\\t0\\x00\\x00\\x00\\x00\\x00\\x1f\\x00\\x02\\x00\\x00\\x00\\x00\\x00\\x00\\x18\\x01\\n\\x0bhi: hello: '
    ]

    class MockParse():

        def read(self):
            return test_data

        def feeds_data(self, data):
            pass

        def reset(self):
            pass

    dispatcher_conn = server_dispatcher.DispatcherConnection(app, ('127.0.0.1', 8000), TestParser(MockParse))
    worker_conn = server_dispatcher.WorkerConnection(app)

    dispatcher_conn.connection_made(TestClientConn())
    worker_conn.connection_made(TestClientConn())

    # Mock the _dispatch_to_worker_process to raise exception
    def mock_dispatch(packages):
        test_data.clear()
        raise RuntimeError("Mock worker write error")

    dispatcher_conn._dispatch_to_worker_process = mock_dispatch

    # This should trigger the exception handling at line 131
    dispatcher_conn.data_received(b'some_data')

    # Verify the connection was closed due to exception
    assert dispatcher_conn.fd not in app.client_connections


def test_dispatcher_connection_lost_with_exception():
    """Test dispatcher connection_lost with exception to cover line 143"""
    engine = server_engine.TrpcServerEngine(ServerOption())
    engine.process_info.add_address(("127.0.0.1", 8000))
    engine.process_info.add_socket_type("net")
    engine.process_info.add_parser(TestParser())
    dispatcher_obj = dispatcher.Dispatcher()
    app = _TestDispatcherProcess(dispatcher_obj, engine)
    app.loop = get_or_create_event_loop()

    dispatcher_conn = server_dispatcher.DispatcherConnection(app, ('127.0.0.1', 8000), TestParser())
    dispatcher_conn.connection_made(TestClientConn())

    # Call connection_lost with an exception to trigger line 143
    test_exception = RuntimeError("Test connection lost exception")
    dispatcher_conn.connection_lost(test_exception)

    # Verify the connection was properly cleaned up
    assert dispatcher_conn.fd not in app.client_connections
    register_dispatcher_worker_func(None, True)
