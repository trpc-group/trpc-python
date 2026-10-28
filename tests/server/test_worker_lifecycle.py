# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.
"""``BaseWorker`` lifecycle: role stamp, I/O wait, ready callbacks, exit status and drain."""

from __future__ import annotations

import asyncio
import multiprocessing
import signal
import time
from multiprocessing import Process
from types import SimpleNamespace

import pytest

from trpc_sdk import common
from trpc_sdk import utils
from trpc_sdk.abc import ProcessType
from trpc_sdk.config import TrpcConfig
from trpc_sdk.runtime._common import _shutdown
from trpc_sdk.runtime import _worker
from trpc_sdk.runtime._process._rpc_task import RpcTaskProcess
from trpc_sdk.runtime._process._separate_worker import SeparateWorker
from trpc_sdk.runtime._process._task_process import TaskProcess
from trpc_sdk.runtime._types import ChildProcessInfo
from trpc_sdk.runtime._worker import BaseWorker


class _Worker(BaseWorker):
    """A worker that records its template steps instead of wiring anything."""

    def __init__(self) -> None:
        super().__init__("test_worker", ProcessType.WORKER)
        self.steps = []

    def _init_plugins(self) -> None:
        self.steps.append("plugins")

    def start_worker_process(self) -> None:
        self.steps.append("listen")

    def _start_timers(self) -> None:
        self.steps.append("timers")


@pytest.fixture
def worker():
    w = _Worker()
    yield w
    if isinstance(w.loop, asyncio.AbstractEventLoop) and not w.loop.is_closed():
        for sig in (signal.SIGTERM, signal.SIGCHLD):
            w.loop.remove_signal_handler(sig)
        w.loop.close()


def test_main_process_worker_is_stamped_as_worker(worker, restore_process_type, monkeypatch):
    monkeypatch.setattr(_Worker, "_prepare_to_serve", lambda self: self.steps.append("prepare"))
    worker.block = False

    worker.run()

    assert common.get_process_type() is ProcessType.WORKER
    assert utils.is_worker_process()


@pytest.mark.parametrize("stamp,check", [(ProcessType.TASK, utils.is_task_process),
                                         (ProcessType.TIMER, utils.is_timer_process)])
def test_utils_role_checks_read_the_common_enum(restore_process_type, stamp, check):
    common.set_current_process_var("type", stamp)

    assert check()
    assert not utils.is_worker_process()


def test_common_still_exports_the_single_process_type():
    from trpc_sdk.abc import ProcessType as AbcProcessType
    from trpc_sdk.common import ProcessType as CommonProcessType

    assert CommonProcessType is AbcProcessType


def test_io_wait_runs_before_the_loop_exists(worker, restore_process_type, monkeypatch):
    seen = []
    monkeypatch.setattr(_Worker, "_wait_until_tasks_ready", lambda self: seen.append(self.loop))
    monkeypatch.setattr(_Worker, "_prepare_to_serve", lambda self: None)
    worker.block = False

    worker.run()

    assert seen == [None]


def test_io_wait_returns_once_counter_reached(worker, monkeypatch):
    monkeypatch.setattr(_worker, "io_ready_reached", lambda cnt: True)
    worker.io_wait_count = 2

    worker._wait_until_tasks_ready()

    assert worker._exit_code == 0


def test_io_wait_gives_up_when_a_child_dies(worker, monkeypatch):
    monkeypatch.setattr(_worker, "io_ready_reached", lambda cnt: False)
    monkeypatch.setattr(_worker.time, "sleep", lambda seconds: pytest.fail("should not sleep with a dead child"))
    stopped = []
    monkeypatch.setattr(_Worker, "_stop_children", lambda self: stopped.append(True))
    worker.children.append(ChildProcessInfo(name="infer:0", factory=lambda: None,
                                       process=SimpleNamespace(is_alive=lambda: False)))
    worker.io_wait_count = 1
    previous = signal.getsignal(signal.SIGTERM)

    with pytest.raises(SystemExit) as exc:
        worker._wait_until_tasks_ready()

    assert exc.value.code == 1
    assert stopped == [True]
    assert signal.getsignal(signal.SIGTERM) == previous


def test_only_the_listening_process_waits(engine_factory):
    engine = engine_factory([("tcp", "trpc")], worker_num=1)
    io_worker = SeparateWorker(engine)
    io_worker.io_wait_count = 3
    io_worker._create_rpc_task_pipes()

    rpc_task = RpcTaskProcess(0, io_worker.rpc_task_pipes, io_worker._ready_flags, engine)

    assert rpc_task.io_wait_count == 0
    for pipes in io_worker.rpc_task_pipes:
        pipes.close()


def test_ready_callbacks_run_before_listen_and_timers(worker):
    """Current order is ready callbacks, then listen, then timers.

    A failing callback is logged and does not stop the later ones.
    """
    worker.loop = asyncio.new_event_loop()
    worker.ready_callbacks.append(lambda: worker.steps.append("register"))
    worker.ready_callbacks.insert(0, lambda: 1 / 0)

    worker._prepare_to_serve()

    assert worker.steps == ["register", "listen", "timers"]


def test_reboot_child_is_forced_off_and_warned(monkeypatch):
    monkeypatch.setattr(TrpcConfig.server_config(), "reboot_child", True)
    warnings = []
    monkeypatch.setattr(_worker.logger, "warning", lambda msg, *args: warnings.append(msg % args))

    w = _Worker()
    w._warn_unsupported_reboot_child()

    assert w.reboot_child is False
    assert any("reboot_child" in message for message in warnings)


def test_dead_child_exits_the_loop_with_status_one(worker):
    worker.loop = asyncio.new_event_loop()
    worker._children_by_pid[4242] = ChildProcessInfo(name="rpc_task-0", factory=lambda: None)
    worker.loop.call_soon(worker._replace_child, 4242)

    with pytest.raises(SystemExit) as exc:
        worker._serve()

    assert exc.value.code == 1


def test_child_exit_during_shutdown_is_not_a_failure(worker):
    worker._exiting = True
    worker._children_by_pid[4242] = ChildProcessInfo(name="rpc_task-0", factory=lambda: None)

    worker._replace_child(4242)

    assert worker._exit_code == 0


def test_sigchld_reaps_only_framework_children(worker, monkeypatch):
    worker._exiting = True
    worker._children_by_pid[11] = ChildProcessInfo(name="alive", factory=lambda: None)
    worker._children_by_pid[22] = ChildProcessInfo(name="dead", factory=lambda: None)
    worker._children_by_pid[33] = ChildProcessInfo(name="gone", factory=lambda: None)
    waited = []

    def fake_waitpid(pid, options):
        waited.append(pid)
        if pid == 33:
            raise ChildProcessError
        return (pid if pid == 22 else 0), 0

    monkeypatch.setattr(_worker.os, "waitpid", fake_waitpid)

    worker._on_child_exit()

    assert sorted(waited) == [11, 22, 33]
    assert list(worker._children_by_pid) == [11]


def test_uvloop_watches_child_sentinels_instead_of_sigchld(worker):
    uvloop = pytest.importorskip("uvloop")
    loop = uvloop.new_event_loop()
    worker.loop = loop
    worker._spawn("short_lived", lambda: Process(target=time.sleep, args=(0.2,)))
    try:
        with pytest.raises(SystemExit) as exc:
            worker._serve()
    finally:
        loop.close()

    assert exc.value.code == 1
    assert not worker._children_by_pid


def _run_until_exit(worker, *later) -> int:
    """Start shutdown on a live loop, schedule ``(delay, callback)`` pairs, return the exit status."""
    worker.loop = asyncio.new_event_loop()
    worker.loop.call_soon(worker._handle_sigterm)
    for delay, callback in later:
        worker.loop.call_later(delay, callback)
    with pytest.raises(SystemExit) as exc:
        worker.loop.run_forever()
    return exc.value.code


def test_sigterm_stops_accepting_then_drains_without_blocking(worker, monkeypatch):
    order = []
    worker.drain_seconds = 0.3
    worker.exit_callbacks.append(lambda: order.append("deregister"))
    worker.post_drain_callbacks.append(lambda: order.append("post_drain"))
    worker.master_exit_callbacks.append(lambda: order.append("master_handle"))
    monkeypatch.setattr(_Worker, "_stop_accepting", lambda self: order.append("stop_accepting"))
    monkeypatch.setattr(_shutdown, "run_worker_signal_handle", lambda: order.append("worker_handle"))
    during_drain = []

    code = _run_until_exit(worker, (0.1, lambda: during_drain.extend(order)))

    assert code == 0
    assert during_drain == ["deregister", "stop_accepting"]
    assert order == ["deregister", "stop_accepting", "post_drain", "master_handle", "worker_handle"]


def test_second_sigterm_cuts_the_drain_and_still_runs_post_drain(worker, monkeypatch):
    worker.drain_seconds = 30
    posted = []
    handled = []
    cuts = []
    worker.post_drain_callbacks.append(lambda: posted.append(True))
    monkeypatch.setattr(_shutdown, "run_worker_signal_handle", lambda: handled.append(True))
    monkeypatch.setattr(_Worker, "_on_drain_cut", lambda self: cuts.append(True))
    began = time.monotonic()

    code = _run_until_exit(worker, (0.05, worker._handle_sigterm))

    assert code == 0
    assert time.monotonic() - began < 5
    assert posted == [True]
    assert handled == [True]
    assert cuts == [True]


def test_failing_drained_hook_still_exits(worker, monkeypatch):
    monkeypatch.setattr(_Worker, "_on_drained", lambda self: 1 / 0)

    with pytest.raises(SystemExit) as exc:
        worker._handle_sigterm()

    assert exc.value.code == 0


def test_failing_shutdown_step_exits_with_status_one(worker, monkeypatch):
    async def _broken(self):
        raise RuntimeError("boom")

    monkeypatch.setattr(_Worker, "_wait_handlers", _broken)

    assert _run_until_exit(worker) == 1


@pytest.mark.parametrize("cls", [SeparateWorker, TaskProcess])
def test_processes_without_handlers_skip_worker_exit_callbacks(monkeypatch, cls):
    handled = []
    monkeypatch.setattr(_shutdown, "run_worker_signal_handle", lambda: handled.append(True))
    instance = object.__new__(cls)
    instance.client_connections = {}

    cls._on_drained(instance)

    assert handled == []


def test_task_graceful_exit_stops_its_consumers_once():
    calls = []
    consumer = SimpleNamespace(terminate=lambda: calls.append("terminate"), join=lambda timeout: calls.append("join"))
    task = object.__new__(TaskProcess)
    task.task_info = SimpleNamespace(name="infer:0")
    task._consumers = [consumer]

    task._on_graceful_exit()
    task._on_graceful_exit()

    assert calls == ["terminate", "join"]


def test_stop_signal_follows_controlled(worker):
    assert worker._stop_signal() == signal.SIGTERM
    worker._controlled = True
    assert worker._stop_signal() == signal.SIGUSR1


def test_stop_signal_of_a_top_level_process_ignores_the_parent_pid(worker, monkeypatch):
    calls = []
    monkeypatch.setattr(_shutdown.os, "getppid", lambda: worker._parent_pid + 1)
    monkeypatch.setattr(_Worker, "_handle_sigterm", lambda self: calls.append("start"))
    monkeypatch.setattr(_Worker, "_arm_exit_killer", lambda self: calls.append("killer"))

    worker._on_stop_signal()

    assert calls == ["start"]


def test_failing_stop_accepting_still_exits(worker, monkeypatch):
    monkeypatch.setattr(_Worker, "_stop_accepting", lambda self: 1 / 0)

    with pytest.raises(SystemExit):
        worker._handle_sigterm()


class _ServingWorker(BaseWorker):
    """One real listen socket, so drain can be observed on a live loop."""

    def __init__(self) -> None:
        super().__init__("drain_check", ProcessType.WORKER)
        self.server: asyncio.AbstractServer = None
        self.events = []

    def _on_drained(self) -> None:
        self.events.append("worker_handle")

    def _stop_accepting(self) -> None:
        self.events.append("stop_accepting")
        self.server.close()

    async def _handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        self.events.append("handler_start")
        await reader.read(16)
        await asyncio.sleep(0.3)
        writer.write(b"ok")
        await writer.drain()
        self.events.append("handler_done")
        writer.close()


def test_inflight_request_finishes_during_drain():
    """A request already being handled still gets its response during drain.

    Shutdown is triggered while the handler is waiting. The listen socket
    closes immediately, and the loop keeps running until ``drain_seconds``
    elapses, which is long enough for that handler to write back.
    """
    worker = _ServingWorker()
    worker.drain_seconds = 1.0
    worker.post_drain_callbacks.append(lambda: worker.events.append("post_drain"))
    worker.loop = asyncio.new_event_loop()
    asyncio.set_event_loop(worker.loop)
    result = {}

    async def client(port: int) -> None:
        reader, writer = await asyncio.open_connection("127.0.0.1", port)
        writer.write(b"ping")
        await writer.drain()
        for _ in range(50):
            if "handler_start" in worker.events:
                break
            await asyncio.sleep(0.01)
        worker._handle_sigterm()
        try:
            extra_reader, extra_writer = await asyncio.wait_for(asyncio.open_connection("127.0.0.1", port), 0.2)
            extra_writer.close()
            extra_reader.feed_eof()
            result["new_conn"] = "accepted"
        except (ConnectionRefusedError, OSError, asyncio.TimeoutError) as err:
            result["new_conn"] = type(err).__name__
        result["body"] = await asyncio.wait_for(reader.read(16), 0.8)
        writer.close()

    try:
        worker.server = worker.loop.run_until_complete(asyncio.start_server(worker._handle_client, "127.0.0.1", 0))
        port = worker.server.sockets[0].getsockname()[1]
        worker.loop.create_task(client(port))
        worker.loop.call_later(3, worker.loop.stop)
        worker._serve()
    finally:
        if isinstance(worker.loop, asyncio.AbstractEventLoop) and not worker.loop.is_closed():
            for sig in (signal.SIGTERM, signal.SIGCHLD):
                worker.loop.remove_signal_handler(sig)
            worker.loop.close()

    assert result.get("body") == b"ok", worker.events
    assert result.get("new_conn") != "accepted", result
    assert worker.events.index("handler_done") < worker.events.index("post_drain")
    assert worker.events == [
        "handler_start",
        "stop_accepting",
        "handler_done",
        "post_drain",
        "worker_handle",
    ]


class _ForkedWorker(BaseWorker, Process):
    def __init__(self) -> None:
        super().__init__("forked", ProcessType.TASK)


def test_parent_sentinel_is_watched_in_forked_children(monkeypatch):
    child = _ForkedWorker()
    readers = []
    child.loop = SimpleNamespace(add_reader=lambda fd, cb, *args: readers.append((fd, cb, args)))
    monkeypatch.setattr(_worker.multiprocessing, "parent_process", lambda: SimpleNamespace(sentinel=77))

    child._watch_parent()

    assert readers == [(77, child._on_parent_exit, (77,))]


def test_main_process_worker_does_not_watch_a_parent(worker, monkeypatch):
    monkeypatch.setattr(_worker.multiprocessing, "parent_process", lambda: SimpleNamespace(sentinel=77))
    worker.loop = SimpleNamespace(add_reader=lambda *args: pytest.fail("not a forked child"))

    worker._watch_parent()


class _FakeTimer:
    started = []

    def __init__(self, interval, function, args):
        self.interval = interval
        self.daemon = False

    def start(self):
        _FakeTimer.started.append(self.interval)


def test_parent_exit_cuts_the_drain_with_a_kill_deadline(monkeypatch):
    child = _ForkedWorker()
    removed = []
    child.loop = SimpleNamespace(remove_reader=removed.append)
    _FakeTimer.started = []
    monkeypatch.setattr(_shutdown.threading, "Timer", _FakeTimer)
    calls = []
    monkeypatch.setattr(_ForkedWorker, "_cut_now", lambda self: calls.append(True))

    child._on_parent_exit(77)

    assert removed == [77]
    assert _FakeTimer.started == [_shutdown.PARENT_EXIT_GRACE_SECONDS]
    assert calls == [True]


def test_stop_signal_after_the_parent_changed_cuts_with_a_kill_deadline(monkeypatch):
    child = _ForkedWorker()
    _FakeTimer.started = []
    monkeypatch.setattr(_shutdown.threading, "Timer", _FakeTimer)
    calls = []
    monkeypatch.setattr(_ForkedWorker, "_cut_now", lambda self: calls.append("cut"))
    monkeypatch.setattr(_ForkedWorker, "_handle_sigterm", lambda self: calls.append("start"))

    monkeypatch.setattr(_shutdown.os, "getppid", lambda: child._parent_pid)
    child._on_stop_signal()
    monkeypatch.setattr(_shutdown.os, "getppid", lambda: child._parent_pid + 1)
    child._on_stop_signal()

    assert calls == ["start", "cut"]
    assert _FakeTimer.started == [_shutdown.PARENT_EXIT_GRACE_SECONDS]


def test_cut_now_starts_and_cuts_once(worker, monkeypatch):
    calls = []

    def _handle(self):
        calls.append(self._exiting)
        self._exiting = True

    monkeypatch.setattr(_Worker, "_handle_sigterm", _handle)

    worker._cut_now()
    worker._cut_now()

    assert calls == [False, True, True]


class _StubbornChild(multiprocessing.get_context("fork").Process):
    """A child that ignores its stop signal, so only SIGKILL takes it down."""

    def _stop_signal(self) -> int:
        return signal.SIGTERM

    def run(self) -> None:
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
        time.sleep(30)


def test_stop_children_kills_a_child_that_ignores_its_stop_signal(worker, monkeypatch):
    monkeypatch.setattr(_shutdown, "CHILD_EXIT_GRACE_SECONDS", 0.3)
    child = _StubbornChild()
    child.start()
    worker.children.append(ChildProcessInfo(name="stubborn", factory=lambda: None, process=child))
    began = time.monotonic()
    try:
        time.sleep(0.2)
        worker._stop_children()
        child.join(2)
    finally:
        if child.is_alive():
            child.kill()
            child.join()

    assert time.monotonic() - began < 3
    assert child.exitcode == -signal.SIGKILL
