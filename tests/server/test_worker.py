# -*- coding: utf-8 -*-
import sys
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

import trpc_sdk.server.worker as worker_mod
from trpc_sdk import codec
from trpc_sdk.http.routes import HttpHandle


client_codec = codec.ClientCodec()
msg = codec.Message()
frame_head = codec.FrameHead()
msg.frame_head = frame_head
msg.caller_service_name = 'trpc.app.client.service1/call'
msg.callee_service_name = 'trpc.app.server.service2/sayHello'
msg.callee_method = 'sayHello'
msg.serialization_type = 0
msg.compress_type = 0
msg.timeout = 1000
msg.client_meta_data = {}
body = b'hello world'
req_buff = client_codec.encode(msg, body)


class TmpTransport:

    def write(self, data):
        print("write")


def handle(data):
    print("handle: test worker; data:", data)


def test_worker():
    # worker = server_worker.RequestWorker("worker-1", handle)

    # recvInfo = RecvInfo(0, 0, req_buff, time.time())

    # conn = server_worker.WorkerConnection(worker, handle)
    # conn.aio_connection = TmpTransport()
    # conn.data_received(pickle.dumps(recvInfo))
    # conn.connection_lost(None)
    pass


class _Loop:
    def __init__(self):
        self.connection = object()
        self.calls = []

    def create_connection(self, factory, sock=None):
        self.calls.append(("stream", factory, sock))
        return self.connection

    def create_datagram_endpoint(self, factory, sock=None):
        self.calls.append(("datagram", factory, sock))
        return self.connection

    def run_until_complete(self, coro):
        if coro is self.connection:
            return ("transport", "protocol")
        return None


def test_create_pipe_conn_registers_transport(monkeypatch):
    instance = worker_mod.BaseWorker.__new__(worker_mod.BaseWorker)
    instance.loop = _Loop()
    pipe = MagicMock()
    pipe.get_socket.return_value = "socket"
    task = SimpleNamespace(use_c_task=False, name="task")
    register = MagicMock()
    monkeypatch.setattr(worker_mod, "register_task_conn", register)

    instance._BaseWorker__create_pipe_conn(pipe, task)

    pipe.close_extra_conn.assert_called_once()
    register.assert_called_once_with("task", "transport")


def test_create_unix_conn_registers_stream_client(monkeypatch):
    instance = worker_mod.BaseWorker.__new__(worker_mod.BaseWorker)
    client = MagicMock()
    task = SimpleNamespace(name="task-a", task_stream_handler=SimpleNamespace(client=client))
    register = MagicMock()
    monkeypatch.setattr(worker_mod.TaskStreamClientMap, "register_task_stream_client", register)

    instance._BaseWorker__create_unix_conn(task)

    client.set_unix_path.assert_called_once_with(".__task-a")
    register.assert_called_once_with("task-a", client)


def test_start_message_listener_sets_transport_and_protocol():
    instance = worker_mod.BaseWorker.__new__(worker_mod.BaseWorker)
    instance.loop = _Loop()
    instance.worker = SimpleNamespace(read_message_pipe=MagicMock())
    instance.worker.read_message_pipe.get_socket.return_value = "message-socket"

    instance.start_message_listener("engine")

    assert instance.message_pipe_transport == "transport"
    assert instance.message_pipe_protocol == "protocol"


def test_worker_process_start_workers_closes_pipes_and_connects():
    instance = worker_mod.WorkerProcess.__new__(worker_mod.WorkerProcess)
    instance.loop = _Loop()
    instance.worker_pipe_conn = MagicMock()
    instance.worker_pipe_conn.get_socket.return_value = "worker-socket"
    instance.rpcz_pipe_conn = MagicMock()
    instance.addresses_map = {}
    instance.handlers = []
    instance.timeouts = []
    instance.tran_opts = []

    instance.start_workers()

    instance.worker_pipe_conn.close_extra_conn.assert_called_once()
    instance.rpcz_pipe_conn.close_extra_conn.assert_called_once()
    assert instance.loop.calls[0][0] == "stream"


@pytest.mark.parametrize("use_trpcio, expected", [(False, "datagram"), (True, "stream")])
def test_cworker_process_transport_switch(monkeypatch, use_trpcio, expected):
    instance = worker_mod.CWorkerProcess.__new__(worker_mod.CWorkerProcess)
    instance.loop = _Loop()
    instance.worker_pipe_list = []
    instance.worker_pipe_conn = MagicMock()
    instance.worker_pipe_conn.get_socket.return_value = "worker-socket"
    instance.addresses_map = {}
    instance.handlers = []
    instance.timeouts = []
    other = SimpleNamespace(pipe_conn=SimpleNamespace(socket_pair=("a", "b")))
    instance.server_engine = SimpleNamespace(worker_process_list=[other])
    monkeypatch.setattr(worker_mod, "PipeConnection", MagicMock())
    monkeypatch.setattr(worker_mod, "USE_TRPCIO", use_trpcio)
    if use_trpcio:
        fake_module = SimpleNamespace(CWorkerConnectionTrpcio=object)
        monkeypatch.setitem(sys.modules, "trpc_sdk.transport.server_worker_trpcio", fake_module)

    instance.start_workers()

    assert instance.loop.calls[0][0] == expected


def test_get_https_certs_rejects_missing_files(monkeypatch):
    cfg = SimpleNamespace(tls_key="/missing/key", tls_cert="/missing/cert")
    monkeypatch.setattr(worker_mod.os.path, "exists", lambda _: False)
    with pytest.raises(KeyError, match="key file"):
        worker_mod.get_https_certs(cfg)

    monkeypatch.setattr(worker_mod.os.path, "exists", lambda path: path.endswith("key"))
    with pytest.raises(ValueError, match="cert key file"):
        worker_mod.get_https_certs(cfg)


def test_add_user_route_skips_service_name_mismatch(monkeypatch):
    matching = HttpHandle(lambda *_: None, "GET", "/yes", "svc")
    other = HttpHandle(lambda *_: None, "GET", "/no", "other")
    monkeypatch.setattr(worker_mod.routes, "RegisteredRoutes", {"yes": matching, "no": other})
    router = MagicMock()
    app = SimpleNamespace(router=router)
    instance = worker_mod.ExecutorProcess.__new__(worker_mod.ExecutorProcess)

    instance._add_user_route(app, "svc", MagicMock())

    router.add_resource.assert_called_once_with("/yes")
