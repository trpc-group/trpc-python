# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.
"""Listener selection and the separate-mode split between I/O process and rpc tasks."""

from __future__ import annotations

import asyncio

import pytest

from trpc_sdk.http import HttpServerTransport
from trpc_sdk.runtime._common import _shutdown
from trpc_sdk.runtime._constants import AddressKind
from trpc_sdk.runtime._listeners import classify_address
from trpc_sdk.runtime._listeners import pop_loop_resources
from trpc_sdk.runtime._process import _rpc_task
from trpc_sdk.runtime._process import _separate_worker
from trpc_sdk.runtime._process._rpc_task import RpcTaskProcess
from trpc_sdk.runtime._process._separate_worker import SeparateWorker
from trpc_sdk.transport import TrpcServerTransport
from trpc_sdk.transport import register_protocol_handle
from trpc_sdk.transport._server import _user_transport


@pytest.fixture
def custom_protocol(monkeypatch):
    calls = []
    monkeypatch.setattr(_user_transport, "_PROTOCOL_HANDLES", {})
    register_protocol_handle("custom", lambda worker, index: calls.append((worker, index)))
    return calls


def test_classify_address_user_protocol_then_network(engine_factory, custom_protocol):
    worker = SeparateWorker(engine_factory([
        ("tcp", "custom"),
        ("tcp,http", "trpc"),
        ("udp", "trpc"),
        ("http", "http"),
    ]))

    kinds = [classify_address(worker, i) for i in range(4)]

    assert kinds == [AddressKind.USER, AddressKind.TCP, AddressKind.UDP, AddressKind.HTTP]


def test_classify_address_flags_an_unservable_network(engine_factory):
    worker = SeparateWorker(engine_factory([("sctp", "trpc")]))

    assert classify_address(worker, 0) is AddressKind.UNKNOWN


def test_register_protocol_handle_is_exported():
    from trpc_sdk.runtime import register_protocol_handle as runtime_export
    from trpc_sdk.server import register_protocol_handle as server_export

    assert server_export is register_protocol_handle
    assert runtime_export is register_protocol_handle


def test_separate_listen_serves_only_tcp_with_its_own_factory(monkeypatch, engine_factory):
    worker = SeparateWorker(engine_factory([("tcp", "trpc"), ("udp", "trpc"), ("http", "http")]))
    listened = []
    monkeypatch.setattr(TrpcServerTransport, "listen",
                        lambda self, ctx: listened.append((self.opts.address, ctx)))
    monkeypatch.setattr(HttpServerTransport, "listen", lambda self, ctx: pytest.fail("http listened"))
    worker.loop = object()

    worker._listen()

    ((address, ctx),) = listened
    assert address == worker.addresses[0]
    assert ctx.loop is worker.loop and not ctx.serving_loops
    assert ctx.protocol_factory.keywords["index"] == 0
    assert worker._listening == [(worker.transports[0], worker.loop)]


def test_separate_stop_accepting_stops_the_transports(monkeypatch, engine_factory):
    worker = SeparateWorker(engine_factory([("tcp", "trpc")]))
    stopped = []
    monkeypatch.setattr(TrpcServerTransport, "listen", lambda self, ctx: None)
    monkeypatch.setattr(TrpcServerTransport, "stop", lambda self: stopped.append(self))
    monkeypatch.setattr(worker, "_signal", lambda records: None)
    worker.loop = object()
    worker._listen()

    worker._stop_accepting()
    worker._stop_accepting()

    assert stopped == [worker.transports[0]]


@pytest.mark.parametrize("network,protocol", [("sctp", "trpc"), ("tcp", "noparser")])
def test_separate_rejects_unservable_address_before_fork(monkeypatch, engine_factory, network, protocol):
    worker = SeparateWorker(engine_factory([(network, protocol)]))
    forked = []
    monkeypatch.setattr(_separate_worker.BaseWorker, "_spawn_children", lambda self: forked.append(True))

    with pytest.raises(ValueError):
        worker._spawn_children()
    assert not forked


def test_rpc_task_starts_its_own_listeners(monkeypatch, engine_factory, custom_protocol):
    engine = engine_factory([("tcp", "trpc"), ("udp", "trpc"), ("http", "http"), ("tcp", "custom")])
    worker = SeparateWorker(engine)
    worker._create_rpc_task_pipes()
    task = RpcTaskProcess(0, worker.rpc_task_pipes, worker._ready_flags, engine)
    task.loop = object()
    started = []

    def _udp_listen(self, ctx):
        started.append(("udp", ctx.index, ctx.reuse_port))
        self.keep_loop_resource(None, "endpoint")

    def _http_listen(self, ctx):
        started.append(("http", ctx.http_runner_factory.args[1:], ctx.reuse_port))
        self.keep_loop_resource(None, "runner")

    monkeypatch.setattr(TrpcServerTransport, "listen", _udp_listen)
    monkeypatch.setattr(HttpServerTransport, "listen", _http_listen)
    monkeypatch.setattr(_rpc_task, "create_http_msg_handle", lambda w, i: f"handle{i}")

    task._start_own_listeners()

    assert started == [("udp", 1, True), ("http", (2, "handle2"), True)]
    assert custom_protocol == [(task, 3)]
    assert task._user_transports == [task.transports[3]]
    assert task._resource_transports == [task.transports[1], task.transports[2]]
    assert pop_loop_resources(task._resource_transports, None) == ["endpoint", "runner"]
    for pipes in worker.rpc_task_pipes:
        pipes.close()


def test_rpc_task_exit_destroys_user_transports_after_on_stop(monkeypatch, engine_factory, custom_protocol):
    engine = engine_factory([("tcp", "custom")], worker_num=1)
    worker = SeparateWorker(engine)
    worker._create_rpc_task_pipes()
    task = RpcTaskProcess(0, worker.rpc_task_pipes, worker._ready_flags, engine)
    events = []
    transport = task.transports[0]

    async def _destroy():
        events.append("destroy")

    async def _stop_hooks(info):
        events.append("on_stop")

    monkeypatch.setattr(transport, "stop", lambda: events.append("stop"))
    monkeypatch.setattr(transport, "destroy", _destroy)
    monkeypatch.setattr(_rpc_task, "run_stop_hooks", _stop_hooks)
    loop = asyncio.new_event_loop()
    task.loop = loop
    task._thread_info = object()
    try:
        task._start_own_listeners()
        task._on_graceful_exit()
        task._on_graceful_exit()
    finally:
        loop.close()
        for pipes in worker.rpc_task_pipes:
            pipes.close()

    assert events == ["stop", "on_stop", "destroy"]


def test_separate_worker_runs_no_timers(engine_factory):
    worker = SeparateWorker(engine_factory([("tcp", "trpc")]))

    worker._start_timers()

    assert worker.timer is None


def test_rpc_task_runs_worker_exit_callbacks(monkeypatch, engine_factory):
    engine = engine_factory([("tcp", "trpc")], worker_num=1)
    worker = SeparateWorker(engine)
    worker._create_rpc_task_pipes()
    task = RpcTaskProcess(0, worker.rpc_task_pipes, worker._ready_flags, engine)
    calls = []
    monkeypatch.setattr(_shutdown, "run_worker_signal_handle", lambda: calls.append(True))
    loop = asyncio.new_event_loop()
    task.loop = loop
    try:
        loop.call_soon(task._handle_sigterm)
        with pytest.raises(SystemExit) as exc:
            loop.run_forever()
    finally:
        loop.close()
        for pipes in worker.rpc_task_pipes:
            pipes.close()

    assert calls == [True]
    assert exc.value.code == 0


def test_transport_stop_schedules_site_stop():
    scheduled = []

    class _Site:
        async def stop(self):
            return None

    class _Loop:
        def create_task(self, coro):
            scheduled.append(coro)
            coro.close()

    transport = HttpServerTransport()
    transport.keep_site(_Loop(), _Site())
    transport.keep_site(_Loop(), _Site())

    transport.stop()
    transport.stop()

    assert len(scheduled) == 2
