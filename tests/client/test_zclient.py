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

from examples.helloworld.stub.trpc_trpc_python_group_helloworld import helloworld_pb2
from tests.common_util import unused_port
from trpc_sdk.client import Client
from trpc_sdk.client.options import with_caller_service_name
from trpc_sdk.client.options import with_metadata
from trpc_sdk.client.options import with_protocol
from trpc_sdk.client.options import with_service_name
from trpc_sdk.client.options import with_target
from trpc_sdk.client.options import with_timeout
from trpc_sdk.codec.message import Message
from trpc_sdk.context import Context
from trpc_sdk.exceptions import ClientEncodeError
from trpc_sdk.exceptions import ClientRouteError
from trpc_sdk.exceptions import ErrorCode


class MockTCPServer(object):

    def __init__(self, port, handler):
        TCPServer.allow_reuse_address = True
        self.serv = TCPServer(('127.0.0.1', port), handler)
        self.thread = Thread(target=self.serv.serve_forever, daemon=True)
        self.thread.start()

    def stop(self):
        self.serv.shutdown()
        print("after self.serv.shutdown")

        self.serv.server_close()
        print("after self.serv.server_close")


class TestClient:

    def setup_class(self):

        class HandlerRead(BaseRequestHandler):

            def handle(self):
                self.data = self.request.recv(1024)
                self.request.sendall(
                    b'\t0\x00\x00\x00\x00\x00$\x00\x02\x00\x00\x00\x00\x00\x00\x18\x01\n\x10hi: hello: hello')
                self.request.close()
                pass

        self.port = unused_port()
        self.srv = MockTCPServer(self.port, HandlerRead)

    def teardown_class(self):
        self.srv.stop()

    def test_client(self):
        ctx = Context()
        msg = Message()
        msg.set_namespace('Development')
        ctx.msg = msg
        req = helloworld_pb2.HelloRequest()
        req.msg = 'hello'
        rsp_cls = helloworld_pb2.HelloReply

        cli = Client()
        # 测试service_name为空
        options = []
        try:
            cli.invoke_sync(ctx, req, rsp_cls, options)
        except Exception as e:  # pylint: disable=broad-except
            assert str(e) == 'service_name empty'

        # 测试不指定protocol，codec为空
        options = [
            with_target('ip://127.0.0.1:%s' % self.port),
            with_service_name('trpc_sdk.test.helloworld.Greeter'),
        ]
        try:
            cli.invoke_sync(ctx, req, rsp_cls, options)
        except ClientEncodeError as e:
            assert e.code == ErrorCode.TRPC_CLIENT_ENCODE_ERR
        except Exception:  # pylint: disable=broad-except
            pass

        # 测试非法target
        options = [
            with_target('ip:/127.0.0.1:%s' % self.port),
            with_service_name('trpc_sdk.test.helloworld.Greeter'),
        ]
        try:
            cli.invoke_sync(ctx, req, rsp_cls, options)
        except ClientRouteError as e:
            assert e.code == ErrorCode.TRPC_CLIENT_ROUTER_ERR
        except Exception:  # pylint: disable=broad-except
            pass

        # 正常请求
        try:
            options = [
                with_target('ip://127.0.0.1:%s' % self.port),
                with_service_name('trpc_sdk.test.helloworld.Greeter'),
                with_protocol('trpc'),
                with_caller_service_name('trpc_sdk.app.caller.service'),
                with_timeout(10000),
                with_metadata('testKey', b'testValue')
            ]
            rsp = cli.invoke_sync(ctx, req, rsp_cls, options)
            assert rsp.msg == 'hi: hello: hello'
        except Exception:  # pylint: disable=broad-except
            pass
