# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.
"""Lifecycle of Server.serve and the worker exit template that replaced the master."""

import os
import time
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from trpc_sdk.runtime import MergeWorker
from trpc_sdk.server import Server
from trpc_sdk.server import ServerOption
from trpc_sdk.server import TrpcServerAdapter
from trpc_sdk.server import _server as server_mod


def _server():
    opts = ServerOption()
    opts.worker_num = 1
    opts.service_num = 0
    opts.close_wait_time = 0
    opts.reboot_child = True
    return Server(opts)


def _worker():
    return MergeWorker(TrpcServerAdapter(ServerOption()))


def test_serve_logs_set_start_method_and_runs_the_worker(monkeypatch):
    srv = _server()
    worker = MagicMock()
    worker.ready_callbacks = []
    worker.exit_callbacks = []
    worker.post_drain_callbacks = []
    worker.master_exit_callbacks = []
    monkeypatch.setattr(server_mod, "create_worker", lambda mode, adapter: worker)
    monkeypatch.setattr(server_mod.multiprocessing, "set_start_method", MagicMock(side_effect=RuntimeError("set")))
    monkeypatch.setattr(server_mod.multiprocessing, "get_start_method", lambda *_: "fork")
    logged = MagicMock()
    monkeypatch.setattr(server_mod.logger, "error", logged)

    srv.serve()

    logged.assert_called()
    assert "set" in logged.call_args.args[1]
    worker._spawn_children.assert_called_once()
    worker.run.assert_called_once()
    assert worker.drain_seconds == 0
    assert srv.worker is worker


def test_serve_copies_close_wait_time_onto_the_worker(monkeypatch):
    srv = _server()
    srv.opt.close_wait_time = 2000
    worker = MagicMock()
    worker.ready_callbacks = []
    worker.exit_callbacks = []
    worker.post_drain_callbacks = []
    worker.master_exit_callbacks = []
    monkeypatch.setattr(server_mod, "create_worker", lambda mode, adapter: worker)

    srv.serve()

    assert worker.drain_seconds == 2.0
    assert worker.io_wait_count == srv.opt.io_wait_count


def test_shutdown_runs_exit_then_post_drain_then_master():
    worker = _worker()
    worker.loop = None
    worker.drain_seconds = 0
    order = []
    worker.exit_callbacks.append(lambda: order.append("exit"))
    worker.post_drain_callbacks.append(lambda: order.append("post"))
    worker.master_exit_callbacks.append(lambda: order.append("master"))
    worker._on_drained = lambda: order.append("drained")

    with pytest.raises(SystemExit) as raised:
        worker._handle_sigterm()

    assert raised.value.code == 0
    assert order == ["exit", "post", "master", "drained"]


@pytest.mark.asyncio
async def test_drain_waits_close_wait_time():
    worker = _worker()
    worker.drain_seconds = 0.05
    worker._drain_cut = __import__("asyncio").Event()
    started = time.monotonic()

    await worker._shutdown()

    assert time.monotonic() - started >= 0.04


def test_dead_child_takes_the_worker_down():
    worker = _worker()
    child = SimpleNamespace(name="task", process=SimpleNamespace(pid=42), exited=False)
    worker._children_by_pid[42] = child
    called = []
    worker._handle_sigterm = lambda: called.append("exit")

    worker._replace_child(42)

    assert called == ["exit"]
    assert worker._exit_code == 1
    assert child.exited is True
    assert 42 not in worker._children_by_pid


def test_reboot_child_true_restarts_instead_of_exiting():
    """The flag is ignored in production; the branch still rebuilds the child when forced."""
    worker = _worker()
    worker.reboot_child = True
    started = []
    child = SimpleNamespace(name="task", process=None, exited=False, factory=lambda: SimpleNamespace(start=lambda: started.append("start"), pid=7))
    worker._children_by_pid[42] = child

    worker._replace_child(42)

    assert started == ["start"]
    assert worker._exit_code == 0
    assert child.exited is True


def test_reap_treats_child_process_error_as_gone(monkeypatch):
    monkeypatch.setattr(os, "waitpid", MagicMock(side_effect=ChildProcessError))
    assert MergeWorker._reap(42) is True


def test_exit_killer_uses_os_exit():
    worker = _worker()
    worker._arm_exit_killer()
    try:
        assert worker._exit_killer is not None
        assert worker._exit_killer.daemon is True
    finally:
        worker._exit_killer.cancel()
