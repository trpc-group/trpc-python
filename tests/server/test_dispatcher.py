# -*- coding: utf-8 -*-
from unittest import mock

from trpc_sdk import codec
from trpc_sdk.server.c_dispatcher import CDispatcherProcess
from trpc_sdk.server.dispatcher import Dispatcher
from trpc_sdk.server.server_engine import TrpcServerEngine

client_codec = codec.ClientCodec()
msg = codec.Message()
frame_head = codec.FrameHead()
msg.frame_head = frame_head
msg.caller_service_name = 'trpc_sdk.app.client.service1/call'
msg.callee_service_name = 'trpc_sdk.app.server.service2/sayHello'
msg.callee_method = 'sayHello'
msg.serialization_type = 0
msg.compress_type = 0
msg.timeout = 1000
msg.client_meta_data = {}
body = b'hello world'
req_buff = client_codec.encode(msg, body)


def handle():
    print("handle: test dispatcher")


class MockWorkerProcess:
    pass


class MockDispatcherTransport:

    def set_worker_list(self, worker_process_list):
        assert len(worker_process_list) == 1
        pass

    def start(self, engine):
        pass


def test_dispatcher():
    from trpc_sdk.server import option
    engine = TrpcServerEngine(option.ServerOption())
    engine.worker_process_list = [MockWorkerProcess()]
    dispatcher = CDispatcherProcess(None, engine)
    dispatcher.dispatcher = Dispatcher()
    dispatcher.dispatcher.transport = mock.Mock(return_value=MockDispatcherTransport())
    try:
        dispatcher.run()
    except:  # pylint: disable=bare-except
        assert False
    assert True
