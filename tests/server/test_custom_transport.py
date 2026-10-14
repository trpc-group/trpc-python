# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.
"""A user protocol that takes its requests in itself, in every process model.

:class:`FakePullTransport` is what ``register_protocol_handle`` gives a
service: a :class:`~trpc_sdk.transport.UserProtocolTransport` whose starter
has no listen socket and pulls work on ``worker.loop``, the way a
message-queue consumer would. These tests drive the workers in-process,
without forking, and check where ``listen`` runs, that ``stop`` reaches the
transport, and that ``destroy`` runs on the same loop after the ``on_stop``
hooks.
"""

from __future__ import annotations

import asyncio
import threading
from typing import Any
from typing import List
from typing import Optional

import pytest

from trpc_sdk.runtime import MergeWorker
from trpc_sdk.runtime import _merge_worker
from trpc_sdk.runtime._process import _peer_worker
from trpc_sdk.runtime._process import _rpc_task
from trpc_sdk.runtime._process._rpc_task import RpcTaskProcess
from trpc_sdk.runtime._process._separate_worker import SeparateWorker
from trpc_sdk.runtime._threadmodel._thread_hooks import set_current_handler_thread
from trpc_sdk.transport import ListenOption
from trpc_sdk.transport import UserProtocolTransport


class FakePullTransport(UserProtocolTransport):
    """Pulls a work item every few milliseconds on the starter's ``worker.loop``."""

    def __init__(self, events: List) -> None:
        super().__init__(self._start)
        self.events = events
        self.opt: Optional[ListenOption] = None
        self.worker: Any = None
        self.pulled: List[str] = []
        self._stopped = False
        self._task: Optional[asyncio.Task] = None

    def listen(self, opt: Optional[ListenOption] = None) -> None:
        self.opt = opt
        super().listen(opt)

    def _start(self, worker: Any, index: int) -> None:
        del index
        self.worker = worker
        self.events.append(("listen", threading.current_thread().name))
        self._task = worker.loop.create_task(self._pull())

    async def _pull(self) -> None:
        while not self._stopped:
            self.pulled.append(threading.current_thread().name)
            await asyncio.sleep(0.005)

    def stop(self) -> None:
        self._stopped = True
        self.events.append(("stop", threading.current_thread().name))

    async def destroy(self) -> None:
        if self._task is not None:
            await self._task
        self.events.append(("destroy", threading.current_thread().name))


@pytest.fixture(autouse=True)
def reset_main_thread_identity():
    """Serving on the main loop marks this thread as a serving loop; undo it."""
    yield
    set_current_handler_thread(None)


@pytest.fixture
def events():
    return []


@pytest.fixture
def pull_engine(engine_factory, events):
    """An engine with one tRPC TCP endpoint and one FakePullTransport endpoint."""

    def _make(**opts):
        engine = engine_factory([("tcp", "trpc"), ("tcp", "pull")], **opts)
        info = engine.process_info
        transport = FakePullTransport(events)
        transport.bind(info.transport_opt[1])
        info.transports[1] = transport
        return engine, transport

    return _make


@pytest.fixture
def record_stop_hooks(monkeypatch, events):
    async def _stop_hooks(info):
        events.append(("on_stop", threading.current_thread().name))

    monkeypatch.setattr(_merge_worker, "run_stop_hooks", _stop_hooks)


@pytest.fixture
def no_stream(monkeypatch):
    """Keep the tRPC endpoint from binding a real port."""
    monkeypatch.setattr(MergeWorker, "_start_stream", lambda self, index: None)


def _run(loop: asyncio.AbstractEventLoop, seconds: float = 0.05) -> None:
    loop.run_until_complete(asyncio.sleep(seconds))


def _main() -> str:
    return threading.current_thread().name


@pytest.mark.usefixtures("record_stop_hooks", "no_stream")
def test_merge_single_loop_serves_user_protocol_on_the_main_loop(pull_engine, events):
    engine, transport = pull_engine()
    worker = MergeWorker(engine)
    worker.loop = asyncio.new_event_loop()
    try:
        worker._init_main_serving_loop()
        worker.start_worker_process()
        _run(worker.loop)

        assert transport.opt.loop is worker.loop
        assert transport.opt.worker is worker
        assert transport.opt.index == 1
        assert set(transport.pulled) == {_main()}

        worker._stop_accepting()
        worker._on_graceful_exit()
        worker._on_graceful_exit()
    finally:
        worker.loop.close()

    assert events == [("listen", _main()), ("stop", _main()), ("on_stop", _main()), ("destroy", _main())]


@pytest.mark.usefixtures("record_stop_hooks", "no_stream")
def test_merge_threads_serve_user_protocol_on_serving_thread_zero(pull_engine, events):
    engine, transport = pull_engine(io_thread_num=2)
    worker = MergeWorker(engine)
    worker.loop = asyncio.new_event_loop()
    try:
        worker._start_loop_threads()
        worker.start_worker_process()
        thread0 = worker.loop_threads[0]
        _run(worker.loop)

        assert transport.opt.loop is thread0.loop
        assert isinstance(transport.worker, _merge_worker._LoopView)
        assert transport.worker.loop is thread0.loop
        assert transport.worker.addresses is worker.addresses
        assert set(transport.pulled) == {thread0.name}

        worker._stop_accepting()
        worker._on_graceful_exit()
    finally:
        worker._stop_serving_threads()
        worker.loop.close()

    assert events[0] == ("listen", thread0.name)
    assert ("stop", thread0.name) in events
    assert events.index(("on_stop", thread0.name)) < events.index(("destroy", thread0.name))
    assert [event for event in events if event[0] == "listen"] == [("listen", thread0.name)]


@pytest.mark.usefixtures("record_stop_hooks", "no_stream")
def test_peer_worker_serves_user_protocol_on_its_loop(pull_engine, events):
    engine, transport = pull_engine()
    worker = _peer_worker.PeerWorker(engine, 1, 2, "worker:1")
    worker.loop = asyncio.new_event_loop()
    try:
        worker._init_main_serving_loop()
        worker.start_worker_process()
        _run(worker.loop)

        assert transport.opt.worker is worker
        assert transport.pulled

        worker._stop_accepting()
        worker._on_graceful_exit()
    finally:
        worker.loop.close()

    assert [event[0] for event in events] == ["listen", "stop", "on_stop", "destroy"]


def test_separate_leaves_user_protocols_to_the_rpc_tasks(monkeypatch, pull_engine, events):
    engine, transport = pull_engine(worker_num=1)
    worker = SeparateWorker(engine)
    worker._validate_addresses()
    listened = []
    monkeypatch.setattr(type(worker.transports[0]), "listen", lambda self, opt: listened.append(self))
    worker.loop = object()

    worker._listen()

    assert listened == [worker.transports[0]]
    assert transport.opt is None


def test_rpc_task_serves_user_protocol_and_destroys_it_after_on_stop(monkeypatch, pull_engine, events):
    engine, transport = pull_engine(worker_num=1)
    worker = SeparateWorker(engine)
    worker._create_rpc_task_pipes()
    task = RpcTaskProcess(0, worker.rpc_task_pipes, worker._ready_flags, engine)

    async def _stop_hooks(info):
        events.append(("on_stop", _main()))

    monkeypatch.setattr(_rpc_task, "run_stop_hooks", _stop_hooks)
    task.loop = asyncio.new_event_loop()
    task._thread_info = object()
    try:
        task._start_own_listeners()
        _run(task.loop)

        assert transport.opt.worker is task
        assert transport.opt.reuse_port is True
        assert transport.pulled

        task._stop_accepting()
        task._on_graceful_exit()
    finally:
        task.loop.close()
        for pipes in worker.rpc_task_pipes:
            pipes.close()

    assert [event[0] for event in events] == ["listen", "stop", "on_stop", "destroy"]
