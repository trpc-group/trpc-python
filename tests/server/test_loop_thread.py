# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.
"""Thread affinity of :class:`LoopThread` and how a merge worker learns that one died."""

from __future__ import annotations

import asyncio
import socket
import threading
from types import SimpleNamespace
from typing import List

import pytest

from trpc_sdk.runtime._merge_worker import MergeWorker
from trpc_sdk.runtime._threadmodel import LoopThread
from trpc_sdk.runtime._threadmodel import _thread_model

READY = 5.0


@pytest.fixture
def threads():
    started: List[LoopThread] = []
    yield started
    for thread in started:
        thread.stop(READY)


def _start(threads: List[LoopThread], init=None, on_exit=None) -> LoopThread:
    thread = LoopThread(len(threads), name_prefix="test-loop", on_exit=on_exit)
    threads.append(thread)
    thread.start(init)
    return thread


def test_submit_from_init_onto_its_own_thread_runs_inline(threads):
    seen = []

    async def _work():
        return threading.get_ident()

    thread = LoopThread(0, name_prefix="test-loop")
    threads.append(thread)

    def _init(loop):
        seen.append(thread.submit(_work, timeout=0.5))

    thread.start(_init)
    thread.wait_ready(READY)

    assert seen == [thread.owner_ident]


def test_submit_from_its_running_loop_returns_sync_results_and_refuses_awaitables(threads):
    thread = _start(threads)
    thread.wait_ready(READY)

    async def _work():
        return 1

    def _inside():
        sync_result = thread.submit(lambda: "sync")
        with pytest.raises(RuntimeError, match="await it instead"):
            thread.submit(_work)
        return sync_result

    assert thread.submit(_inside, timeout=READY) == "sync"


def test_submit_from_another_thread_crosses_over(threads):
    thread = _start(threads)
    thread.wait_ready(READY)

    assert thread.submit(threading.get_ident, timeout=READY) == thread.owner_ident
    assert thread.owner_ident != threading.get_ident()


def test_attach_socket_before_the_loop_exists_closes_the_socket():
    thread = LoopThread(0)
    left, right = socket.socketpair()
    try:
        thread.attach_socket(left, asyncio.Protocol)
        assert left.fileno() == -1
    finally:
        left.close()
        right.close()


def test_attach_socket_on_its_own_thread_skips_the_post(threads, monkeypatch):
    attached = []
    monkeypatch.setattr(_thread_model, "connect_accepted", lambda loop, sock, *_: attached.append(sock))
    thread = _start(threads)
    thread.wait_ready(READY)
    left, right = socket.socketpair()

    def _inside():
        # A posted hand-off would only run after this callback returns.
        thread.attach_socket(left, asyncio.Protocol)
        return list(attached)

    try:
        assert thread.submit(_inside, timeout=READY) == [left]
    finally:
        left.close()
        right.close()


def test_on_exit_runs_after_a_stray_loop_stop(threads):
    exited = threading.Event()
    seen = []

    def _on_exit(thread):
        seen.append(thread.loop.is_closed())
        exited.set()

    thread = _start(threads, on_exit=_on_exit)
    thread.wait_ready(READY)
    thread.loop.call_soon_threadsafe(thread.loop.stop)

    assert exited.wait(READY)
    assert seen == [True]


def test_on_exit_runs_on_stop_but_not_when_init_failed(threads):
    exits = []
    stopped = _start(threads, on_exit=exits.append)
    stopped.wait_ready(READY)
    assert stopped.stop(READY)
    assert exits == [stopped]

    def _fail(loop):
        raise ValueError("init failed")

    failed = _start(threads, _fail, on_exit=exits.append)
    with pytest.raises(ValueError):
        failed.wait_ready(READY)
    failed._thread.join(READY)  # pylint: disable=protected-access
    assert exits == [stopped]


def _merge_worker(loop: asyncio.AbstractEventLoop) -> MergeWorker:
    worker = MergeWorker.__new__(MergeWorker)
    worker.name = "merge"
    worker.loop = loop
    worker._exiting = False  # pylint: disable=protected-access
    worker._exit_code = 0  # pylint: disable=protected-access
    worker._stopping_threads = False  # pylint: disable=protected-access
    worker.sigterms = 0
    worker._handle_sigterm = lambda: setattr(worker, "sigterms", worker.sigterms + 1)
    return worker


@pytest.mark.parametrize("exiting,stopping", [(True, False), (False, True)])
def test_expected_thread_exit_is_ignored(exiting, stopping):
    loop = asyncio.new_event_loop()
    try:
        worker = _merge_worker(loop)
        worker._exiting = exiting  # pylint: disable=protected-access
        worker._stopping_threads = stopping  # pylint: disable=protected-access

        worker._on_loop_thread_died("t")  # pylint: disable=protected-access

        assert (worker._exit_code, worker.sigterms) == (0, 0)  # pylint: disable=protected-access
    finally:
        loop.close()


def test_dead_thread_while_serving_exits_with_status_1_on_the_main_loop():
    loop = asyncio.new_event_loop()
    try:
        worker = _merge_worker(loop)
        dying = threading.Thread(target=worker._on_loop_thread_exit,  # pylint: disable=protected-access
                                 args=(SimpleNamespace(name="trpc-loop-1"),))
        dying.start()
        dying.join()
        assert worker.sigterms == 0

        loop.call_soon(loop.stop)
        loop.run_forever()

        assert (worker._exit_code, worker.sigterms) == (1, 1)  # pylint: disable=protected-access
    finally:
        loop.close()


def test_exit_reported_to_a_closed_main_loop_is_dropped():
    loop = asyncio.new_event_loop()
    loop.close()
    worker = _merge_worker(loop)

    worker._on_loop_thread_exit(SimpleNamespace(name="t"))  # pylint: disable=protected-access

    assert worker.sigterms == 0
