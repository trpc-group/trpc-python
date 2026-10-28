# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.
"""Listener selection and the separate-mode split between I/O process and rpc tasks."""

from __future__ import annotations

import asyncio

import pytest

from trpc_sdk.abc import BaseServerTransportOption
from trpc_sdk.http import HttpServerTransport
from trpc_sdk.runtime import ResourceKind
from trpc_sdk.runtime import current_thread_model
from trpc_sdk.runtime._common import _shutdown
from trpc_sdk.runtime._process import _rpc_task
from trpc_sdk.runtime._process import _separate_worker
from trpc_sdk.runtime._process._rpc_task import RpcTaskProcess
from trpc_sdk.runtime._process._separate_worker import SeparateForwardTransport
from trpc_sdk.runtime._process._separate_worker import SeparateWorker
from trpc_sdk.transport._server import StreamListener
from trpc_sdk.transport import TrpcServerTransport
from trpc_sdk.transport import UserProtocolTransport
from trpc_sdk.transport import register_protocol_handle
from trpc_sdk.transport._server import _custom_transport


@pytest.fixture
def custom_protocol(monkeypatch):
    calls = []
    monkeypatch.setattr(_custom_transport, "_PROTOCOL_HANDLES", {})
    register_protocol_handle("custom", lambda worker, index: calls.append((worker, index)))
    return calls


def test_only_trpc_over_tcp_is_ipc_forwardable(engine_factory, custom_protocol):
    worker = SeparateWorker(engine_factory([
        ("tcp", "custom"),
        ("tcp,http", "trpc"),
        ("udp", "trpc"),
        ("tcp", "http"),
    ]))

    transports = worker.transports

    assert isinstance(transports[0], UserProtocolTransport)
    assert [t.ipc_forwardable for t in transports] == [False, True, False, False]


def test_check_endpoint_rejects_an_unservable_network():
    opts = BaseServerTransportOption()
    opts.set_network("sctp")
    transport = TrpcServerTransport()
    transport.bind(opts)

    assert transport.check_endpoint() is False


def test_register_protocol_handle_is_exported():
    from trpc_sdk.runtime import register_protocol_handle as runtime_export
    from trpc_sdk.server import register_protocol_handle as server_export

    assert server_export is register_protocol_handle
    assert runtime_export is register_protocol_handle


def test_separate_listen_serves_only_tcp_with_its_own_factory(monkeypatch, engine_factory):
    worker = SeparateWorker(engine_factory([("tcp", "trpc"), ("udp", "trpc"), ("tcp", "http")]))
    listened = []
    bound_opts = worker.transports[0].opts
    monkeypatch.setattr(TrpcServerTransport, "listen", lambda self: listened.append(self))
    monkeypatch.setattr(HttpServerTransport, "listen", lambda self: pytest.fail("http listened"))
    worker.loop = object()

    worker._listen()

    (forward,) = listened
    assert isinstance(forward, SeparateForwardTransport)
    assert forward is worker.transports[0]
    assert forward.opts is bound_opts
    assert forward.protocol_factory().keywords["index"] == 0
    assert worker._listening == [(worker.transports[0], worker.loop)]


def test_separate_stop_accepting_stops_the_transports(monkeypatch, engine_factory):
    worker = SeparateWorker(engine_factory([("tcp", "trpc")]))
    stopped = []
    monkeypatch.setattr(TrpcServerTransport, "listen", lambda self: None)
    monkeypatch.setattr(TrpcServerTransport, "stop", lambda self: stopped.append(self))
    monkeypatch.setattr(worker, "_signal", lambda records: None)
    worker.loop = object()
    worker._listen()

    worker._stop_accepting()
    worker._stop_accepting()

    assert stopped == [worker.transports[0]]


def test_separate_rejects_unframeable_address_before_fork(monkeypatch, engine_factory):
    worker = SeparateWorker(engine_factory([("tcp", "noparser")]))
    forked = []
    monkeypatch.setattr(_separate_worker.BaseWorker, "_spawn_children", lambda self: forked.append(True))

    with pytest.raises(ValueError):
        worker._spawn_children()
    assert not forked


def test_rpc_task_starts_its_own_listeners(monkeypatch, engine_factory, custom_protocol):
    engine = engine_factory([("tcp", "trpc"), ("udp", "trpc"), ("tcp", "http"), ("tcp", "custom")])
    worker = SeparateWorker(engine)
    worker._create_rpc_task_pipes()
    task = RpcTaskProcess(0, worker.rpc_task_pipes, worker._ready_flags, engine)
    task.loop = asyncio.new_event_loop()
    asyncio.set_event_loop(task.loop)
    started = []

    def _udp_listen(self):
        model = current_thread_model()
        started.append(("udp", task.transports.index(self), model.reuse_port))
        model.keep_resource(model.acceptor, "endpoint", ResourceKind.ENDPOINT)

    def _http_listen(self):
        model = current_thread_model()
        started.append(("http", task.transports.index(self), model.reuse_port))
        model.keep_resource(model.acceptor, "runner", ResourceKind.RUNNER)

    monkeypatch.setattr(TrpcServerTransport, "listen", _udp_listen)
    monkeypatch.setattr(HttpServerTransport, "listen", _http_listen)

    task._install_thread_model()
    task._start_own_listeners()

    assert started == [("udp", 1, True), ("http", 2, True)]
    assert custom_protocol == [(task, 3)]
    assert task._user_transports == [task.transports[3]]
    assert task.thread_model.pop_resources(task.thread_model.acceptor) == ["endpoint", "runner"]
    asyncio.set_event_loop(None)
    task.loop.close()
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
    asyncio.set_event_loop(loop)
    task.loop = loop
    task._thread_info = object()
    try:
        task._install_thread_model()
        task._start_own_listeners()
        task._on_graceful_exit()
        task._on_graceful_exit()
    finally:
        asyncio.set_event_loop(None)
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


def test_transport_stop_closes_every_listener_once():
    closed = []

    class _Acceptor:
        def close(self):
            closed.append(self)

    transport = HttpServerTransport()
    transport._listeners = [StreamListener(acceptor=_Acceptor()), StreamListener(acceptor=_Acceptor())]

    transport.stop()
    transport.stop()

    assert len(closed) == 2
