# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.
"""Server service table, request handling, signals, and shutdown wiring.

``TrpcServerEngine`` and ``trpc.new`` are gone. ``create_server`` builds the
``Server``, and the worker exit template owns what used to be
``handle_master_exit``.
"""

import textwrap
from unittest.mock import MagicMock

import pytest

from trpc_sdk.abc import BaseServerTransportOption
from trpc_sdk.abc import Method
from trpc_sdk.abc import ServiceDesc
from trpc_sdk.abc import TrpcContext
from trpc_sdk.abc import get_message
from trpc_sdk.codec import TrpcClientCodec
from trpc_sdk.config import ServerConfig
from trpc_sdk.config import TrpcConfig
from trpc_sdk.runtime import MergeWorker
from trpc_sdk.runtime import WorkerInfo
from trpc_sdk.runtime import set_signal_handle
from trpc_sdk.runtime._common._signals import run_server_signal_handle
from trpc_sdk.runtime._common._signals import run_worker_signal_handle
from trpc_sdk.serialization import Body
from trpc_sdk.serialization import SerializationType
from trpc_sdk.server import Server
from trpc_sdk.server import ServerOption
from trpc_sdk.server import ServiceAdapter
from trpc_sdk.server import ServiceOption
from trpc_sdk.server import TrpcServerAdapter
from trpc_sdk.server import create_server
from trpc_sdk.server import _server as server_mod
from trpc_sdk.transport import TrpcServerTransport
from trpc_sdk.transport._cython import decode_worker_msg_req
from trpc_sdk.utils import now_ms


def _request() -> bytes:
    ctx = TrpcContext()
    msg = get_message(ctx)
    msg.set_client_rpc_name("/trpc.test.Greeter/SayHello")
    msg.set_callee_method("SayHello")
    msg.set_callee_service_name("trpc.test.Greeter")
    msg.set_caller_service_name("trpc.test.Caller")
    msg.set_timeout(1000)
    msg.set_serialization_type(SerializationType.Noop)
    msg.set_compress_type(0)
    return TrpcClientCodec().encode_data(ctx, b"hello")


def _service() -> ServiceAdapter:
    opt = ServiceOption()
    opt.set_protocol("trpc")
    opt.set_service_name("trpc.test.Greeter")
    opt.set_timeout(5000)
    opt.set_address(("127.0.0.1", 8000))
    opt.set_network("tcp")
    return ServiceAdapter(opt)


def _ctx(recv_time=None) -> TrpcContext:
    ctx = TrpcContext()
    ctx.timeout = 5000
    ctx.recv_time = now_ms() if recv_time is None else recv_time
    return ctx


@pytest.mark.asyncio
async def test_handle():
    """Unknown methods, HTTP routes, and a live handler all stay on ServiceAdapter."""
    service = _service()
    data = _request()
    missing = await service.handle(_ctx(), data)
    assert b"func not exist" in missing

    async def say_hello(_ctx, req):
        rsp = Body()
        rsp.data = b"ok-" + (req.data or b"")
        return rsp

    service.register(ServiceDesc("trpc.test.Greeter", [Method("/trpc.test.Greeter/SayHello", Body, Body, say_hello)]),
                     False)
    found = await service.handle(_ctx(), data)
    assert b"ok-hello" in found

    async def http_cb(_ctx, _request):
        return "http-ok"

    assert await service.handle_http(_ctx(), http_cb) == "http-ok"

    short = _ctx()
    short.timeout = 1
    assert isinstance(await service.handle(short, data), bytes)
    negative = _ctx()
    negative.timeout = -1
    assert b"ok-hello" in await service.handle(negative, data)


@pytest.mark.asyncio
async def test_avalanche():
    """Queue-wait timeout is encoded when the server checks timeouts."""
    previous = TrpcConfig.server_config().check_timeout
    TrpcConfig.server_config().check_timeout = True
    try:
        service = _service()
        data = _request()
        late = _ctx(now_ms() - 10000)
        ret = await service.handle(late, data)
        assert b"package timeout" in ret
    finally:
        TrpcConfig.server_config().check_timeout = previous


def test_get_service_and_add_service_adapter():
    server = Server(ServerOption())
    assert server.get_service("missing") is None

    service = _service()
    server.add_service_adapter("test", service)
    assert server.get_service("test") is service

    async def say_hello(_ctx, req):
        return req

    server.register(ServiceDesc("test", [Method("/trpc.test.Greeter/SayHello", Body, Body, say_hello)]))
    assert "/trpc.test.Greeter/SayHello" in service.unary_handlers


def test_create_server_loads_one_service_per_yaml_entry(monkeypatch, tmp_path):
    """``create_server`` is the replacement for ``trpc.new``."""
    monkeypatch.setattr("trpc_sdk.config._defaults.get_ip_with_nic", lambda _nic: "127.0.0.1")
    conf = tmp_path / "trpc_python.yaml"
    conf.write_text(
        textwrap.dedent("""\
            global:
              namespace: Development
              env_name: test
            server:
              app: test
              server: demo
              close_wait_time: 1500
              service:
                - name: trpc.test.Greeter
                  ip: 127.0.0.1
                  port: 18080
                  network: tcp
                  protocol: trpc
                  timeout: 1000
            """))
    server = create_server(str(conf))
    assert isinstance(server, Server)
    assert server.opt.close_wait_time == 1500
    assert server.opt.mode == "merge"
    service = server.get_service("trpc.test.Greeter")
    assert service is not None
    assert service.opt.address == ("127.0.0.1", 18080)
    assert service.opt.protocol == "trpc"
    assert server.get_service("missing") is None
    assert service.close() is None


def test_serve_hands_shutdown_callbacks_to_the_worker(monkeypatch):
    server = Server(ServerOption())
    server.opt.close_wait_time = 1000

    class _Service:
        def init_transport(self):
            return None

    server.service_adapters["test"] = _Service()
    worker = MagicMock()
    worker.ready_callbacks = []
    worker.exit_callbacks = []
    worker.post_drain_callbacks = []
    worker.master_exit_callbacks = []
    monkeypatch.setattr(server_mod, "create_worker", lambda mode, adapter: worker)

    server.serve()

    assert worker.drain_seconds == 1.0
    assert server._shutdown_services in worker.exit_callbacks
    assert run_server_signal_handle in worker.master_exit_callbacks
    worker._spawn_children.assert_called_once()
    worker.run.assert_called_once()


def test_shutdown_services_marks_the_server_closed():
    server = Server(ServerOption())
    service = _service()
    server.add_service_adapter("test", service)
    server._shutdown_services()
    assert server.close is True


def test_set_signal_handle():
    ran = []
    set_signal_handle(master_handle=lambda: ran.append("master"), worker_handle=lambda: ran.append("worker"))
    run_server_signal_handle()
    run_worker_signal_handle()
    assert "master" in ran and "worker" in ran

    with pytest.raises(Exception, match="register signal master handle failed"):
        set_signal_handle("1", None)
    with pytest.raises(Exception, match="register signal worker handle failed"):
        set_signal_handle(None, "1")


def test_close_wait_time_config():
    assert ServerConfig(close_wait_time=5000).close_wait_time == 5000
    assert ServerConfig().close_wait_time == 0


def test_close_wait_time_order_on_the_worker():
    """Drain callbacks run service exit, then post-drain, then the master handle."""
    worker = MergeWorker(TrpcServerAdapter(ServerOption()))
    worker.loop = None
    worker.drain_seconds = 1.0
    order = []
    worker.exit_callbacks.append(lambda: order.append("service_close"))
    worker.post_drain_callbacks.append(lambda: order.append("post_drain"))
    worker.master_exit_callbacks.append(lambda: order.append("master"))
    worker._on_drained = lambda: order.append("worker")

    with pytest.raises(SystemExit):
        worker._handle_sigterm()

    # The loop is not running, so the drain sleep is skipped; the order is unchanged.
    assert order == ["service_close", "post_drain", "master", "worker"]


def test_unknown_mode_is_rejected():
    with pytest.raises(ValueError, match="unknown server.mode"):
        server_mod.create_worker("not-a-mode", TrpcServerAdapter(ServerOption()))


def test_address_validation_rejects_bad_endpoints(tmp_path):
    adapter = TrpcServerAdapter(ServerOption())
    bad = MagicMock()
    bad.opts = BaseServerTransportOption()
    bad.opts.set_network("tcp")
    bad.opts.set_address(("127.0.0.1", -1))
    bad.check_endpoint.return_value = False
    with pytest.raises(SystemExit):
        adapter.add_listen_endpoint(bad)

    unix = TrpcServerTransport()
    opt = BaseServerTransportOption()
    opt.set_network("tcp")
    opt.set_socket_type("unix")
    opt.set_address((str(tmp_path), 0))
    unix.bind(opt)
    assert unix.check_endpoint() is False


def test_send_msg_to_worker_bounds_and_does_not_broadcast():
    adapter = TrpcServerAdapter(ServerOption())
    assert adapter.send_msg_to_worker(0, 0, "x") is False

    workers = []
    for index in range(3):
        pipe = MagicMock()
        info = WorkerInfo(id=index, write_message_pipe=pipe)
        workers.append(info)
    adapter.worker_process_list = workers

    assert adapter.send_msg_to_worker(0, 3, "x") is False
    assert adapter.send_msg_to_worker(0, -1, "all") is False
    for info in workers:
        info.write_message_pipe.send.assert_not_called()

    assert adapter.send_msg_to_worker(0, 2, "one") is None
    sent = workers[2].write_message_pipe.send.call_args.args[0]
    from_worker, payload = decode_worker_msg_req(sent)
    assert from_worker == 0
    assert payload == b"one"
    workers[0].write_message_pipe.send.assert_not_called()
    workers[1].write_message_pipe.send.assert_not_called()


def test_adapter_close_waits_once_per_service():
    adapter = TrpcServerAdapter(ServerOption(service_num=0))
    adapter.close()

    waiting = TrpcServerAdapter(ServerOption(service_num=1))
    waiting.registry_semaphore.release()
    waiting.close()
