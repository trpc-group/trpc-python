# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.
"""How ``Server.serve`` wires the worker, plus the parent-death helpers."""

from __future__ import annotations

import asyncio
import multiprocessing
import os
import signal
import threading
from functools import partial
from types import SimpleNamespace

import pytest

from trpc_sdk.abc import BaseServerTransportOption
from trpc_sdk.abc import TrpcContext
from trpc_sdk.common import _process_helper
from trpc_sdk.http import HttpServerTransport
from trpc_sdk.common import bind_lifetime_to_parent
from trpc_sdk.server import _server
from trpc_sdk.server import _engine
from trpc_sdk.server._registry import RegistryThread
from trpc_sdk.server._registry import start_service_registry
from trpc_sdk.runtime._common import _healthy_checker
from trpc_sdk.runtime._common._signals import run_server_signal_handle
from trpc_sdk.transport import TrpcServerTransport
from trpc_sdk.transport import UserProtocolTransport
from trpc_sdk.transport import _separate as transport_separate
from trpc_sdk.transport._server import _datagram_connection as server_datagram
from trpc_sdk.transport._server import _trpc_transport as server_transport


class _FakeWorker:
    def __init__(self) -> None:
        self.ready_callbacks = []
        self.exit_callbacks = []
        self.post_drain_callbacks = []
        self.master_exit_callbacks = []
        self.drain_seconds = 0.0
        self.io_wait_count = 0
        self.threads_at_fork = None
        self.ran = False

    def _spawn_children(self) -> None:
        self.threads_at_fork = list(threading.enumerate())

    def run(self) -> None:
        self.ran = True


def _serve(monkeypatch, set_start_method):
    fake = _FakeWorker()
    monkeypatch.setattr(_server, "create_worker", lambda mode, engine: fake)
    monkeypatch.setattr(_server, "execute_before", lambda: None)
    monkeypatch.setattr(_server, "execute_after_in_parent", lambda: None)
    monkeypatch.setattr(_server._engine_module, "GLOBAL_SERVER_ENGINE", None)
    monkeypatch.setattr(multiprocessing, "set_start_method", set_start_method)
    server = _server.Server.__new__(_server.Server)
    server.services = {}
    server.opt = SimpleNamespace(mode="merge", close_wait_time=1500)
    server.server_engine = SimpleNamespace(close=lambda: None)
    server.worker = None
    server.close = False
    server.serve(io_wait_count=2)
    return server, fake


@pytest.fixture
def start_method_calls():
    return []


@pytest.fixture
def served(monkeypatch, start_method_calls):
    return _serve(monkeypatch, lambda *args, **kwargs: start_method_calls.append((args, kwargs)))


def test_serve_forces_fork_globally(served, start_method_calls):
    assert start_method_calls == [(("fork",), {"force": True})]


def test_serve_continues_when_start_method_cannot_be_set(monkeypatch):
    def refuse(*args, **kwargs):
        raise RuntimeError("context has already been set")

    _, worker = _serve(monkeypatch, refuse)

    assert worker.ran


def test_serve_registers_after_listen_and_drains_on_exit(served):
    server, worker = served

    assert len(worker.ready_callbacks) == 1
    registry = worker.ready_callbacks[0]
    assert isinstance(registry, partial)
    assert registry.func is start_service_registry and registry.args == (server,)
    assert worker.exit_callbacks == [server._shutdown_services]
    assert worker.post_drain_callbacks == [server.server_engine.close]
    assert worker.master_exit_callbacks == [run_server_signal_handle]
    assert worker.drain_seconds == pytest.approx(1.5)
    assert worker.io_wait_count == 2
    assert worker.ran


def test_no_registry_thread_exists_at_fork(served):
    _, worker = served

    assert not [t for t in worker.threads_at_fork if isinstance(t, RegistryThread)]


def _fake_parent(monkeypatch, pid):
    monkeypatch.setattr(_process_helper.multiprocessing, "parent_process", lambda: SimpleNamespace(pid=pid))


def test_bind_lifetime_sets_pdeathsig_on_linux(monkeypatch):
    calls = []
    _fake_parent(monkeypatch, os.getppid())
    monkeypatch.setattr(_process_helper.sys, "platform", "linux")
    monkeypatch.setattr(_process_helper.ctypes, "CDLL",
                        lambda name, use_errno: SimpleNamespace(prctl=lambda *args: calls.append(args)))

    bind_lifetime_to_parent()
    bind_lifetime_to_parent(signal.SIGUSR1)

    assert calls == [(_process_helper._PR_SET_PDEATHSIG, int(signal.SIGTERM), 0, 0, 0),
                     (_process_helper._PR_SET_PDEATHSIG, int(signal.SIGUSR1), 0, 0, 0)]


def test_bind_lifetime_exits_when_parent_already_gone(monkeypatch):
    _fake_parent(monkeypatch, os.getppid() + 100000)
    monkeypatch.setattr(_process_helper.sys, "platform", "darwin")

    with pytest.raises(SystemExit) as exc:
        bind_lifetime_to_parent()

    assert exc.value.code == 1


def test_bind_lifetime_is_a_no_op_outside_forked_children(monkeypatch):
    monkeypatch.setattr(_process_helper.multiprocessing, "parent_process", lambda: None)
    monkeypatch.setattr(_process_helper.ctypes, "CDLL", lambda *args, **kwargs: pytest.fail("no prctl"))

    bind_lifetime_to_parent()


@pytest.fixture
def heartbeat_tasks(monkeypatch):
    monkeypatch.setattr(_healthy_checker.TrpcConfig.server_config(), "heartbeat_interval", 1)
    monkeypatch.setattr(_healthy_checker, "_init_healthy_checker", None)
    _healthy_checker.init_healthy_checker([SimpleNamespace(name="infer:0"), SimpleNamespace(name="infer:1")])
    options = _healthy_checker._all_options()
    yield options
    for option in options:
        for sock in option.child_pipe.socket_pair:
            sock.close()


def test_task_keeps_only_its_own_heartbeat_pipe(heartbeat_tasks):
    kept = _healthy_checker.close_heartbeat_pipes(keep="infer:1")

    assert kept is heartbeat_tasks[1]
    assert all(sock.fileno() == -1 for sock in heartbeat_tasks[0].child_pipe.socket_pair)
    assert kept.child_pipe.get_socket().fileno() != -1


def test_non_task_child_closes_every_heartbeat_pipe(heartbeat_tasks):
    assert _healthy_checker.close_heartbeat_pipes() is None
    assert all(sock.fileno() == -1 for option in heartbeat_tasks for sock in option.child_pipe.socket_pair)


def test_worker_monitors_heartbeats_on_its_own_loop(heartbeat_tasks):
    loop = asyncio.new_event_loop()
    try:
        _healthy_checker.start_heartbeat_monitor(loop)
        assert all(option.child_pipe.get_socket().fileno() == -1 for option in heartbeat_tasks)
    finally:
        pending = asyncio.all_tasks(loop)
        for task in pending:
            task.cancel()
        loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
        loop.close()


def test_rpcz_side_pipe_carries_worker_id(monkeypatch):
    sent = []
    encoded = []
    app = SimpleNamespace(loop=None, worker=None, worker_id=3, rpcz_pipe_conn=SimpleNamespace(send=sent.append))
    monkeypatch.setattr(transport_separate, "prepare_server_span_for_dispatch",
                        lambda ctx: SimpleNamespace(to_dict=dict, span_id=1))
    monkeypatch.setattr(transport_separate, "encode_worker_msg_req",
                        lambda worker_id, payload: encoded.append(worker_id) or b"side")
    monkeypatch.setattr(transport_separate, "inner_encode_rsp", lambda fd, sid, ret, body: b"rsp")
    written = []

    async def handler(ctx, data):
        return b"ok"

    conn = transport_separate.RpcTaskConnection(app, [handler], [1000], [None])
    conn.aio_connection = SimpleNamespace(is_closing=lambda: False, write=written.append)
    ctx = TrpcContext()
    ctx.with_meta_data("fd", 9)
    ctx.with_meta_data("session_id", 1)

    asyncio.run(conn._handle_request(ctx, 0, b"req"))

    assert encoded == [3]
    assert sent == [b"side"]
    assert written == [b"rsp"]


def test_star_import_resolves_every_name_in_all():
    namespace = {}
    exec("from trpc_sdk.server import *", namespace)  # pylint: disable=exec-used

    assert "Network" in namespace


@pytest.mark.parametrize("network", ["tcp", "http", "udp"])
def test_unavailable_listen_address_exits_with_failure(monkeypatch, network):
    monkeypatch.setattr(server_transport, "is_tcp_port_available", lambda host, port: False)
    monkeypatch.setattr(server_transport, "is_udp_port_available", lambda host, port: False)
    engine = _engine.TrpcServerEngine(SimpleNamespace(service_num=1))
    opts = BaseServerTransportOption()
    opts.set_address(("127.0.0.1", 20000))
    opts.set_network(network)
    transport = HttpServerTransport() if network == "http" else TrpcServerTransport()
    transport.bind(opts)

    with pytest.raises(SystemExit) as exc_info:
        engine.add_listen_endpoint(transport)

    assert exc_info.value.code == 1


def test_user_protocol_transport_does_not_probe_the_address(monkeypatch):
    engine = _engine.TrpcServerEngine(SimpleNamespace(service_num=1))
    opts = BaseServerTransportOption()
    opts.set_address(("127.0.0.1", 20000))
    transport = UserProtocolTransport(lambda worker, index: None)
    transport.bind(opts)
    monkeypatch.setattr(server_transport, "is_tcp_port_available", lambda host, port: pytest.fail("probed"))
    monkeypatch.setattr(server_transport, "is_udp_port_available", lambda host, port: pytest.fail("probed"))

    engine.add_listen_endpoint(transport)

    assert engine.process_info.transports == [transport]


def test_udp_request_context_carries_receive_time():
    seen = []

    async def handler(ctx, data):
        seen.append(ctx)

    class _Parser:

        def create(self):
            return self

        def feed_data(self, data):
            self.frames = [data]

        def read(self):
            frames, self.frames = self.frames, []
            return frames

    transport = TrpcServerTransport()
    transport.bind(BaseServerTransportOption(parser=_Parser(), rpc_handler=handler, idle_timeout=1000))
    sock = SimpleNamespace(is_closing=lambda: False,
                           get_extra_info=lambda key, default=None: ("127.0.0.1", 8000) if key == "sockname" else None)

    async def serve():
        conn = transport.datagram_protocol_factory(SimpleNamespace(shared_pool=None))()
        assert isinstance(conn, server_datagram.DefaultDatagramConnection)
        conn.connection_made(sock)
        conn.datagram_received(b"req", ("127.0.0.1", 30000))
        await asyncio.sleep(0)

    asyncio.run(serve())

    assert seen[0].recv_time > 0
    assert seen[0].timeout == 1000
