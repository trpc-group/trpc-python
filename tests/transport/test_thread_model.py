# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.
"""The process thread model: install/lookup, serve_stream's two paths, per-reactor resources."""

from __future__ import annotations

import asyncio
import socket
import threading
from types import SimpleNamespace

import pytest

from trpc_sdk.abc import BaseServerTransportOption
from trpc_sdk.codec import get_server_parser
from trpc_sdk.runtime import InlineReactor
from trpc_sdk.runtime._threadmodel import MainLoopReactor
from trpc_sdk.runtime._worker import BaseWorker
from trpc_sdk.runtime import ReactorGroup
from trpc_sdk.runtime import ResourceKind
from trpc_sdk.runtime import ThreadModel
from trpc_sdk.runtime import current_thread_model
from trpc_sdk.runtime import install_thread_model
from trpc_sdk.runtime import use_thread_model
from trpc_sdk.runtime._threadmodel import _thread_model
from trpc_sdk.transport import TrpcServerTransport
from trpc_sdk.transport._server import serve_stream
from trpc_sdk.transport._server import _trpc_transport
from trpc_sdk.utils import create_new_socket


@pytest.fixture
def loop():
    new_loop = asyncio.new_event_loop()
    asyncio.set_event_loop(new_loop)
    yield new_loop
    asyncio.set_event_loop(None)
    new_loop.close()


@pytest.fixture(autouse=True)
def uninstalled():
    install_thread_model(None)
    yield
    install_thread_model(None)


class _FakeReactor:
    """Records the sockets handed to it instead of serving them."""

    def __init__(self, index: int, loop: asyncio.AbstractEventLoop) -> None:
        self.index = index
        self.loop = loop
        self.attached = []
        self.submitted = []

    def submit(self, func, timeout=None):
        self.submitted.append(timeout)
        return InlineReactor(self.loop).submit(func)

    def attach_socket(self, sock, protocol_factory, ssl_context=None):
        self.attached.append(protocol_factory)
        sock.close()


def _listen_socket() -> socket.socket:
    return create_new_socket(("127.0.0.1", 0), "", False, True, 16)


def test_default_model_is_rebuilt_on_the_calling_loop(loop):
    first = current_thread_model()
    second = current_thread_model()

    assert first is not second
    assert first.acceptor.loop is loop
    assert list(first.io.reactors) == [first.acceptor]


def test_use_thread_model_restores_the_previous_one(loop):
    outer = ThreadModel.single(loop)
    inner = ThreadModel.single(loop)

    with use_thread_model(outer):
        with use_thread_model(inner):
            assert current_thread_model() is inner
        assert current_thread_model() is outer
    assert current_thread_model() is not outer


def test_model_installed_in_another_process_is_rejected(monkeypatch, loop):
    install_thread_model(ThreadModel.single(loop))
    real_pid = _thread_model.os.getpid()
    monkeypatch.setattr(_thread_model.os, "getpid", lambda: real_pid + 1)

    with pytest.raises(RuntimeError, match="before fork"):
        current_thread_model()


def test_serve_stream_uses_create_server_for_the_acceptor_alone(loop):
    model = ThreadModel.single(loop)
    sock = _listen_socket()
    port = sock.getsockname()[1]

    listener = serve_stream(model, model.io, sock, lambda _: asyncio.Protocol, 16)

    assert listener.server is not None and listener.acceptor is None

    async def _connect():
        _, writer = await asyncio.open_connection("127.0.0.1", port)
        writer.close()

    loop.run_until_complete(_connect())
    loop.run_until_complete(listener.wait_closed())


def test_serve_stream_hands_connections_to_the_group_through_an_acceptor(loop, monkeypatch):
    model = ThreadModel.single(loop)
    reactors = [_FakeReactor(0, loop), _FakeReactor(1, loop)]
    targets = []
    monkeypatch.setattr("trpc_sdk.transport._server._acceptor.get_next_worker",
                        lambda params: targets.append(params.target) or 1)
    sock = _listen_socket()
    port = sock.getsockname()[1]
    factories = {id(r): (lambda r=r: r) for r in reactors}

    listener = serve_stream(model, ReactorGroup(reactors, "io-conn"), sock, lambda r: factories[id(r)], 16)

    assert listener.server is None and listener.acceptor is not None

    async def _connect():
        _, writer = await asyncio.open_connection("127.0.0.1", port)
        await asyncio.sleep(0.05)
        writer.close()

    loop.run_until_complete(_connect())
    listener.close()
    listener.close()

    assert targets == ["io-conn"]
    assert reactors[0].attached == []
    assert reactors[1].attached == [factories[id(reactors[1])]]


def test_resources_are_released_by_kind_and_in_order(loop):
    model = ThreadModel.single(loop)
    reactor = model.acceptor
    events = []

    async def _cleanup():
        events.append("runner")

    def _fail():
        raise OSError("boom")

    model.keep_resource(reactor, SimpleNamespace(close=lambda: events.append("udp1")), ResourceKind.ENDPOINT)
    model.keep_resource(reactor, SimpleNamespace(cleanup=_cleanup), ResourceKind.RUNNER)
    model.keep_resource(reactor, SimpleNamespace(close=_fail), ResourceKind.ENDPOINT)
    model.keep_resource(reactor, SimpleNamespace(close=lambda: events.append("udp2")), ResourceKind.ENDPOINT)

    loop.run_until_complete(model.release_resources(reactor, ResourceKind.ENDPOINT))
    assert events == ["udp1", "udp2"]
    loop.run_until_complete(model.release_resources(reactor))
    assert events == ["udp1", "udp2", "runner"]
    assert model.pop_resources(reactor) == []


def test_resources_are_kept_per_reactor_even_with_the_same_index(loop):
    first = InlineReactor(loop, index=1)
    second = _FakeReactor(1, loop)
    model = ThreadModel(acceptor=first, io=ReactorGroup([first, second]), handle=ReactorGroup([first]))

    model.keep_resource(first, "a", ResourceKind.ENDPOINT)
    model.keep_resource(second, "b", ResourceKind.ENDPOINT)

    assert model.pop_resources(second) == ["b"]
    assert model.pop_resources(first) == ["a"]


@pytest.mark.parametrize("reuse_port,reactor_num,expected", [
    (False, 1, False),
    (True, 1, True),
    (False, 2, True),
])
def test_datagram_reuse_port_rule(monkeypatch, loop, reuse_port, reactor_num, expected):
    reactors = [_FakeReactor(i, loop) for i in range(reactor_num)]
    model = ThreadModel(acceptor=reactors[0],
                        io=ReactorGroup(reactors),
                        handle=ReactorGroup(reactors),
                        reuse_port=reuse_port,
                        ready_timeout=1.5)
    seen = []
    real = _trpc_transport.create_new_socket

    def _create(address, socket_type, reuse_port, stream, backlog):
        seen.append(reuse_port)
        return real(("127.0.0.1", 0), socket_type, reuse_port, stream, backlog)

    monkeypatch.setattr(_trpc_transport, "create_new_socket", _create)
    opts = BaseServerTransportOption()
    opts.set_network("udp")
    opts.set_parser(get_server_parser("trpc"))
    transport = TrpcServerTransport()
    transport.bind(opts)

    with use_thread_model(model):
        transport.listen()

    assert seen == [expected] * reactor_num
    for reactor in reactors:
        assert reactor.submitted == [1.5]
        loop.run_until_complete(model.release_resources(reactor, ResourceKind.ENDPOINT))


def test_main_loop_model_serves_everything_on_one_reactor(loop):
    worker = SimpleNamespace(loop=loop)
    model = ThreadModel.main_loop(loop, worker=worker, reuse_port=True, ready_timeout=2.0)

    assert isinstance(model.acceptor, MainLoopReactor)
    assert list(model.io.reactors) == [model.acceptor] == list(model.handle.reactors)
    assert (model.worker, model.reuse_port, model.ready_timeout) == (worker, True, 2.0)
    assert model.is_single_thread


def test_a_second_reactor_makes_the_model_multi_threaded(loop):
    main = InlineReactor(loop)
    model = ThreadModel(acceptor=main, io=ReactorGroup([main]), handle=ReactorGroup([_FakeReactor(0, loop)]))

    assert not model.is_single_thread


def test_main_loop_only_role_rejects_a_threaded_model(loop):
    main = InlineReactor(loop)
    threaded = ThreadModel(acceptor=main, io=ReactorGroup([_FakeReactor(0, loop)]), handle=ReactorGroup([main]))
    worker = SimpleNamespace(_main_loop_only=True, _build_thread_model=lambda: threaded)

    with pytest.raises(RuntimeError, match="main loop only"):
        BaseWorker._install_thread_model(worker)  # pylint: disable=protected-access


def test_inline_reactor_posts_calls_from_another_thread(loop):
    reactor = InlineReactor(loop)
    seen = []

    async def _work():
        return threading.get_ident()

    def _caller():
        seen.append(reactor.submit(_work, timeout=2.0))
        loop.call_soon_threadsafe(loop.stop)

    caller = threading.Thread(target=_caller)
    caller.start()
    loop.run_forever()
    caller.join()

    assert seen == [threading.get_ident()]


def test_inline_reactor_refuses_an_awaitable_from_its_running_loop(loop):
    reactor = InlineReactor(loop)

    async def _work():
        return 1

    async def _inside():
        assert reactor.submit(lambda: "sync") == "sync"
        with pytest.raises(RuntimeError, match="await it instead"):
            reactor.submit(_work)

    loop.run_until_complete(_inside())


def test_inline_reactor_attach_from_another_thread_after_close_closes_the_socket():
    loop = asyncio.new_event_loop()
    reactor = InlineReactor(loop)
    loop.close()
    left, right = socket.socketpair()
    try:
        caller = threading.Thread(target=reactor.attach_socket, args=(left, asyncio.Protocol))
        caller.start()
        caller.join()
        assert left.fileno() == -1
    finally:
        left.close()
        right.close()


def test_worker_view_follows_the_loop(loop):
    worker = SimpleNamespace(loop=loop)
    other = asyncio.new_event_loop()
    try:
        model = ThreadModel.single(loop, worker=worker)
        model.view_factory = lambda w, l: ("view", w, l)

        assert model.worker_view(loop) is worker
        assert model.worker_view(other) == ("view", worker, other)
    finally:
        other.close()
