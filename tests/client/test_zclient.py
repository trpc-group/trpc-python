# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.
"""
test client
"""
from socketserver import BaseRequestHandler
from socketserver import TCPServer
from threading import Thread

import pytest

from examples.helloworld.stub.trpc_trpc_python_group_helloworld import helloworld_pb2
from tests.common_util import unused_port
from trpc_sdk.abc import TrpcContext
from trpc_sdk.client import Client
from trpc_sdk.client import Options
from trpc_sdk.codec import create_message
from trpc_sdk.config import ClientServiceConfig
from trpc_sdk.config import TrpcConfig
from trpc_sdk.exceptions import ClientEncodeError
from trpc_sdk.exceptions import ClientRouteError
from trpc_sdk.exceptions import TrpcErrorCode


class MockTCPServer(object):

    def __init__(self, port, handler):
        TCPServer.allow_reuse_address = True
        self.serv = TCPServer(('127.0.0.1', port), handler)
        self.thread = Thread(target=self.serv.serve_forever, daemon=True)
        self.thread.start()

    def stop(self):
        self.serv.shutdown()
        self.serv.server_close()


class TestClient:

    def setup_class(self):

        class HandlerRead(BaseRequestHandler):

            def handle(self):
                self.data = self.request.recv(1024)
                self.request.sendall(
                    b'\t0\x00\x00\x00\x00\x00$\x00\x02\x00\x00\x00\x00\x00\x00\x18\x01\n\x10hi: hello: hello')
                self.request.close()

        self.port = unused_port()
        self.srv = MockTCPServer(self.port, HandlerRead)

    def teardown_class(self):
        self.srv.stop()

    def test_client(self):
        ctx, msg = create_message(TrpcContext())
        msg.set_namespace('Development')
        req = helloworld_pb2.HelloRequest()
        req.msg = 'hello'
        rsp_cls = helloworld_pb2.HelloReply
        cli = Client()

        with pytest.raises(Exception, match="empty service_name"):
            cli.invoke_sync(ctx, req, rsp_cls, Options())

        saved = dict(TrpcConfig._client_service_configs)
        try:
            TrpcConfig.set_client_service_config(
                "codec.empty.service",
                ClientServiceConfig(callee="codec.empty.service", protocol=""),
            )
            empty_ctx, empty_msg = create_message(TrpcContext())
            empty_msg.set_callee_service_name("codec.empty.service")
            options = (Options()
                       .set_target('ip://127.0.0.1:%s' % self.port)
                       .set_service_name('codec.empty.service'))
            with pytest.raises(ClientEncodeError) as encode_error:
                cli.invoke_sync(empty_ctx, req, rsp_cls, options)
            assert encode_error.value.code == TrpcErrorCode.TRPC_CLIENT_ENCODE_ERR
        finally:
            TrpcConfig.set_client_service_configs(saved)

        options = (Options()
                   .set_target('ip:/127.0.0.1:%s' % self.port)
                   .set_service_name('trpc_sdk.test.helloworld.Greeter'))
        with pytest.raises(ClientRouteError) as route_error:
            cli.invoke_sync(ctx, req, rsp_cls, options)
        assert route_error.value.code == TrpcErrorCode.TRPC_CLIENT_ROUTER_ERR

        options = (Options()
                   .set_target('ip://127.0.0.1:%s' % self.port)
                   .set_service_name('trpc_sdk.test.helloworld.Greeter')
                   .set_protocol('trpc')
                   .set_caller_service_name('trpc_sdk.app.caller.service')
                   .set_timeout(10000)
                   .set_metadata('testKey', b'testValue'))
        rsp = cli.invoke_sync(ctx, req, rsp_cls, options)
        assert rsp.msg == 'hi: hello: hello'
