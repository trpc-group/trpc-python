# -*- coding: utf-8 -*-
"""Lifecycle-only tests for Server without starting real processes."""
import multiprocessing
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

import trpc_sdk.server.server as server_mod
from trpc_sdk.server.option import ServerOption
from trpc_sdk.server.server import Server


def _server():
    opts = ServerOption()
    opts.worker_num = 1
    opts.service_num = 0
    opts.close_wait_time = 0
    opts.reboot_child = True
    return Server(opts)


def test_serve_logs_set_start_method_runtime_error_and_keyboard_interrupt(monkeypatch):
    srv = _server()
    loop = MagicMock()
    loop.run_forever.side_effect = KeyboardInterrupt
    monkeypatch.setattr(server_mod, "get_or_create_event_loop", lambda: loop)
    monkeypatch.setattr(multiprocessing, "set_start_method", MagicMock(side_effect=RuntimeError("set")))
    monkeypatch.setattr(multiprocessing, "get_start_method", lambda *_: "fork")
    monkeypatch.setattr(server_mod.logger, "error", MagicMock())
    monkeypatch.setattr(server_mod.asyncio, "set_event_loop", MagicMock())
    monkeypatch.setattr(server_mod, "init_healthy_checker", MagicMock())
    monkeypatch.setattr(srv, "_handle_sys_signals", MagicMock())
    srv.server_engine.init_processes = MagicMock()
    srv.server_engine.start_processes = MagicMock()
    srv.server_engine.is_dispatcher = MagicMock(return_value=True)
    srv.server_engine.worker_process_list = []
    srv.server_engine.task_process_list = []

    srv.serve()

    server_mod.logger.error.assert_called_once()
    loop.stop.assert_called_once()


def test_handle_master_exit_collects_service_close_results(monkeypatch):
    srv = _server()
    close_result = object()
    srv.services = {
        "async": SimpleNamespace(close=MagicMock(return_value=close_result)),
        "sync": SimpleNamespace(close=MagicMock(return_value=None)),
    }
    srv.opt.loop = MagicMock()
    srv.server_engine.close = MagicMock()
    srv.kill = MagicMock()
    ensure_future = MagicMock()
    monkeypatch.setattr(server_mod.asyncio, "ensure_future", ensure_future)
    monkeypatch.setattr(server_mod, "run_server_signal_handle", MagicMock())

    srv.handle_master_exit(15, None)

    assert srv.close is True
    queued = ensure_future.call_args.args[0]
    assert queued.cr_frame.f_locals["tasks"] == [close_result]
    queued.close()
    srv.server_engine.close.assert_called_once()
    srv.kill.assert_called_once()


def test_handle_child_exit_reboots_known_child():
    srv = _server()
    srv.get_exit_child_pid = MagicMock(return_value=42)
    srv.server_engine.find_child_process = MagicMock(return_value=True)
    srv.server_engine.restart_child_process = MagicMock()

    srv.handle_child_exit(17, None)

    srv.server_engine.restart_child_process.assert_called_once_with(42)


def test_handle_child_exit_stops_master_when_reboot_disabled():
    srv = _server()
    srv.opt.reboot_child = False
    srv.get_exit_child_pid = MagicMock(return_value=42)
    srv.handle_master_exit = MagicMock()

    srv.handle_child_exit(17, "frame")

    srv.handle_master_exit.assert_called_once_with(17, "frame")


def test_handle_child_exit_childprocesserror_stops_master():
    srv = _server()
    srv.get_exit_child_pid = MagicMock(side_effect=ChildProcessError)
    srv.handle_master_exit = MagicMock()

    srv.handle_child_exit(17, None)

    srv.handle_master_exit.assert_called_once_with(17, None)


def test_kill_uses_os_exit(monkeypatch):
    exit_mock = MagicMock()
    monkeypatch.setattr(server_mod.os, "_exit", exit_mock)
    Server.kill()
    exit_mock.assert_called_once_with(0)
