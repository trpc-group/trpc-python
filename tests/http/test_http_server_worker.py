# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.
"""Test http module"""
import threading

import pytest

from aiohttp import web

from trpc_sdk.abc.codec import Parser  # pylint: disable=no-name-in-module
from trpc_sdk.codec import TrpcServerParser
from trpc_sdk.http import StaticFileRouters
from trpc_sdk.http import register_static
from trpc_sdk.http import routes
from trpc_sdk.http.http_transport import HttpMessageHandle
from trpc_sdk.server import ServerOption
from trpc_sdk.server import ServiceAdapter
from trpc_sdk.server import ServiceOption  # pylint: disable=no-name-in-module
from trpc_sdk.server.server_engine import TrpcServerEngine
from trpc_sdk.server.server_process_info import BaseServerTransportOption
from trpc_sdk.server.worker import ExecutorProcess
from trpc_sdk.server.worker import Worker
from trpc_sdk.server.worker import get_https_certs
from trpc_sdk.transport import server_executor


class _TestParser(Parser):
    """Test parser"""

    def create(self):
        """Create server parser"""
        return TrpcServerParser()


def _create_executor_process() -> ExecutorProcess:
    service_option = ServiceOption()
    service_obj = ServiceAdapter(service_option)
    engine = TrpcServerEngine(ServerOption())
    engine.process_info.add_address(("127.0.0.1", 8000))
    opts = BaseServerTransportOption()
    opts.set_address(("127.0.0.1", 8000))
    engine.process_info.add_transport_opt(opts)
    engine.process_info.add_network("http")
    engine.process_info.add_handler(service_obj.handle)
    engine.process_info.add_parser(_TestParser())
    engine.process_info.add_timeout(1000)
    engine.process_info.add_service(service_obj)
    app = ExecutorProcess(Worker(), engine)
    return app


@pytest.mark.asyncio
async def test_server_executor_udp():
    """Test server udp"""
    app = _create_executor_process()
    task_conn = server_executor.TaskConnection()
    task_conn.data_received(b'123')
    task_conn.connection_made(None)
    task_conn.connection_lost(None)

    threading.Thread(target=app.run, daemon=True).start()


def test_https_cert():
    """Test https cert"""
    opt = BaseServerTransportOption()
    opt.tls_key = "./examples/helloworld/cert/domain_srv.key"
    opt.tls_cert = "./examples/helloworld/cert/domain_srv.crt"
    try:
        get_https_certs(opt)
    except Exception:  # pylint: disable=broad-except
        assert False
    else:
        assert True


def test_https_cert_empty():
    """Test https empty cert"""
    opt = BaseServerTransportOption()
    opt.tls_key = ""
    opt.tls_cert = ""
    try:
        ret = get_https_certs(opt)
    except Exception:  # pylint: disable=broad-except
        assert False
    else:
        assert ret is None


def test_https_without_cert_key():
    """Test https cert key"""
    opt = BaseServerTransportOption()
    opt.tls_key = "fake"
    opt.tls_cert = "fake"
    try:
        get_https_certs(opt)
    except KeyError:  # pylint: disable=broad-except
        assert True
    else:
        assert False


def test_https_without_cert():
    """Test https without cert"""
    opt = BaseServerTransportOption()
    opt.tls_key = "./examples/helloworld/cert/domain_srv.key"
    opt.tls_cert = "fake"
    try:
        get_https_certs(opt)
    except ValueError:  # pylint: disable=broad-except
        assert True
    else:
        assert False


def test_register_static():
    """Test register static"""
    register_static("/static", "static")
    assert StaticFileRouters["/static"] == "static"


async def _service_1(_, __) -> bytes:  # return rsp
    return b"_service_1"


async def _service_2(_, __) -> bytes:  # return rsp
    return b"_service_2"


def test_many_service():
    """Test many service"""
    web_app = web.Application(client_max_size=100)
    app = _create_executor_process()
    msg_handle = HttpMessageHandle(0, app)
    routes.route("/test", service_name="trpc_sdk.test.many.service")(_service_1)
    routes.route("/test1")(_service_2)
    app._add_user_route(web_app, "trpc_sdk.test.many.service", msg_handle)
