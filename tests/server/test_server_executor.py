# -*- coding: utf-8 -*-
"""Test server executor module"""
import asyncio
from unittest.mock import MagicMock

import pytest

from trpc_sdk.server import server_engine
from trpc_sdk.server.service import Service
from trpc_sdk.server.option import ServiceOption, ServerOption
from trpc_sdk.transport import server_executor, BaseServerTransportOption
from trpc_sdk.server.worker import Worker, ExecutorProcess
from trpc_sdk.http import routes as http_routes
from trpc_sdk.exceptions import ServerDecodeError
from trpc_sdk.transport.server_executor import ExecutorUDPConnection, TaskConnection
from trpc_sdk.utils.loop_lib import get_or_create_event_loop

from ..common_util import unused_port
from ..transport_util import TestChecker, TestClientConn


def create_service(engine: server_engine.TrpcServerEngine):
    """Create mock service"""
    engine.process_info.add_socket_type("net")
    service_option = ServiceOption()
    engine.process_info.add_checker(TestChecker())
    service = Service(service_option=service_option)
    engine.process_info.add_handler(service.handle)
    engine.process_info.add_timeout(1000)
    engine.process_info.add_service(service)
    return service


def test_server_executor_tcp():
    """Test server_executor_tcp"""
    engine = server_engine.TrpcServerEngine(ServerOption())
    engine.process_info.add_address(("127.0.0.1", unused_port()))
    create_service(engine)
    engine.process_info.add_network("tcp")
    engine.process_info.add_checker(TestChecker())
    worker = Worker()
    app = ExecutorProcess(worker, engine)
    app.block = False
    try:
        app.run()
    except Exception:  # pylint: disable=broad-except
        assert False
    assert True


def test_server_executor_http():
    """Test server_executor_http"""
    engine = server_engine.TrpcServerEngine(ServerOption())
    service = create_service(engine)
    engine.process_info.add_service(service)
    engine.process_info.add_address(("127.0.0.1", unused_port()))
    engine.process_info.add_network("http")
    http_routes.StaticFileRouters = {}

    opt = BaseServerTransportOption()
    opt.tls_key = "./examples/helloworld/cert/domain_srv.key"
    opt.tls_cert = "./examples/helloworld/cert/domain_srv.crt"
    worker = Worker()
    app = ExecutorProcess(worker, engine)
    app.tran_opts = [opt]
    app.block = False
    try:
        app.run()
    except Exception:  # pylint: disable=broad-except
        assert False
    assert True


def test_server_executor_user_protocol():
    """Test test_server_executor_user_protocol"""
    engine = server_engine.TrpcServerEngine(ServerOption())
    service = create_service(engine)
    service.opt.protocol = "http_test"
    engine.process_info.add_service(service)
    engine.process_info.add_address(("127.0.0.1", unused_port()))
    engine.process_info.add_network("http")
    http_routes.StaticFileRouters = {}

    opt = BaseServerTransportOption()
    opt.tls_key = "./examples/helloworld/cert/domain_srv.key"
    opt.tls_cert = "./examples/helloworld/cert/domain_srv.crt"
    worker = Worker()
    app = ExecutorProcess(worker, engine)
    app.user_protocol_handles["http_test"] = lambda index: print(f"index : {index}, http_test")
    app.tran_opts = [opt]
    app.block = False
    try:
        app.run()
    except Exception:  # pylint: disable=broad-except
        assert False
    assert True


# server_executor_udp
def test_server_executor_udp():
    """Test server_executor_udp"""
    engine = server_engine.TrpcServerEngine(ServerOption())
    service = create_service(engine)
    engine.process_info.add_address(("127.0.0.1", unused_port()))
    engine.process_info.add_network("udp")
    engine.process_info.add_checker(TestChecker())
    engine.process_info.add_service(service)
    worker = Worker()
    app = ExecutorProcess(worker, engine)
    app.block = False
    try:
        app.run()
    except Exception:  # pylint: disable=broad-except
        assert False

    task_conn = server_executor.TaskConnection()
    task_conn.data_received(b'123')
    task_conn.connection_made(None)
    task_conn.connection_lost(None)

    try:
        dispatcher_conn = server_executor.ExecutorUDPConnection(app, TestChecker(), service.handle, 1000)
        dispatcher_conn.connection_made(TestClientConn())
        dispatcher_conn.datagram_received(
            b'\t0\x00\x00\x00\x00\x00\x1f\x00\x02\x00\x00\x00\x00\x00\x00\x18\x01\n\x0bhi: hello: ', 'test_addr')
    except Exception:  # pylint: disable=broad-except
        assert False

    assert True


@pytest.mark.asyncio
async def test_executor_udp_send_failure_and_decode_error(monkeypatch):
    checker = MagicMock()
    checker.create.return_value = checker
    checker.read.side_effect = ServerDecodeError("bad")

    async def handle(ctx, req):
        return b"rsp"

    conn = ExecutorUDPConnection(app=None, checker=checker, handle=handle, timeout=1000)
    conn.connection_made(MagicMock())
    conn.datagram_received(b"x", ("127.0.0.1", 1))

    # process exception is caught around _handle_packages
    checker2 = MagicMock()
    checker2.create.return_value = checker2
    checker2.read.side_effect = [[b"pkg"], []]
    conn2 = ExecutorUDPConnection(app=None, checker=checker2, handle=handle, timeout=1)
    transport2 = MagicMock()
    transport2.get_extra_info.return_value = ("127.0.0.1", 0)
    conn2.connection_made(transport2)

    def boom(*_a, **_k):
        raise RuntimeError("proc")

    monkeypatch.setattr(conn2, "_handle_packages", boom)
    conn2.datagram_received(b"y", ("127.0.0.1", 2))

    transport3 = MagicMock()
    transport3.sendto.side_effect = OSError("send fail")
    conn3 = ExecutorUDPConnection(
        app=None, checker=MagicMock(create=lambda: MagicMock()), handle=handle, timeout=1
    )
    conn3.connection_made(transport3)
    await conn3._handle_request(b"req", transport3, ("127.0.0.1", 3), MagicMock())


def test_task_connection_decode_and_process_errors(monkeypatch):
    tc = TaskConnection()
    transport = MagicMock()
    transport.is_closing.return_value = False
    tc.connection_made(transport)

    checker = MagicMock()
    checker.read.side_effect = ServerDecodeError("d")
    tc.checker = checker
    tc.data_received(b"x")

    checker2 = MagicMock()
    checker2.read.side_effect = [[b"pkg"], []]
    tc.checker = checker2
    monkeypatch.setattr(
        "trpc_sdk.transport.server_executor.inner_decode_task_rsp",
        lambda pkg: (_ for _ in ()).throw(RuntimeError("decode")),
    )
    tc.data_received(b"y")

    tc.connection_lost(None)
    transport.close.assert_called()
