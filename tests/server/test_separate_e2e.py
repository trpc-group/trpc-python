# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.
"""``server.mode: separate`` end to end: startup order, scheduling, shutdown."""

from __future__ import annotations

import asyncio
import os
import re
import signal
import socket
import subprocess
import time
from typing import List

import pytest

from tests.server._e2e_support import Server
from tests.server._e2e_support import free_port
from tests.server._e2e_support import http_call
from tests.server._e2e_support import parse_info
from tests.server._e2e_support import tcp_call
from tests.server._e2e_support import tcp_pipelined
from tests.server._e2e_support import udp_call

pytestmark = pytest.mark.skipif(os.name != "posix", reason="separate mode needs fork")

SEPARATE = "mode: separate\nworker_num: 2"


def _children(pid: int) -> List[int]:
    """Forked children of ``pid``; the multiprocessing resource tracker is not one."""
    out = subprocess.run(["ps", "-ax", "-o", "pid=,ppid=,command="], capture_output=True, text=True,
                         check=False).stdout
    rows = (line.split(None, 2) for line in out.splitlines())
    return [int(row[0]) for row in rows
            if len(row) == 3 and row[1] == str(pid) and "resource_tracker" not in row[2]]


def _gone(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return True
    return False


def _tcp_refused(port: int) -> bool:
    try:
        with socket.create_connection(("127.0.0.1", port), timeout=0.2):
            return False
    except OSError:
        return True


def test_serves_every_network_and_spreads_tcp_over_rpc_tasks(tmp_path):
    ports = {"tcp": free_port(), "udp": free_port(socket.SOCK_DGRAM), "http": free_port()}
    server = Server(tmp_path, ports, SEPARATE)
    server.wait_ready()

    replies = asyncio.run(tcp_pipelined(ports["tcp"], ["info:0.01"] * 6))
    udp = udp_call(ports["udp"], 1, "info:0")
    http = http_call(ports["http"], "info:0")
    code = server.stop()

    assert code == 0, server.output
    assert {ret for ret, _ in replies.values()} == {0}
    assert {parse_info(msg)[0] for _, msg in replies.values()} == {0, 1}
    assert udp[1] == 0 and "info:" in http
    assert "separate: 2 rpc tasks bound" in server.output
    assert server.output.count("HOOK_START") == 2
    assert server.output.count("HOOK_STOP") == 2


def test_tcp_is_bound_only_after_every_rpc_task_started(tmp_path):
    port = free_port()
    server = Server(tmp_path, {"tcp": port}, SEPARATE, env={"SLOW_HOOK": "1"})
    time.sleep(2)
    refused_while_starting = _tcp_refused(port)
    server.wait_ready()
    code = server.stop()

    assert refused_while_starting
    assert code == 0, server.output


def test_failing_on_start_aborts_startup_with_status_1(tmp_path):
    port = free_port()
    server = Server(tmp_path, {"tcp": port}, SEPARATE, env={"FAIL_HOOK": "1"})

    code = server.wait_exit()

    assert code == 1, server.output
    assert "hook failed on purpose" in server.output
    assert "rpc tasks bound" not in server.output
    forked = [int(pid) for pid in re.findall(r"forked child \S+ as pid (\d+)", server.output)]
    assert len(forked) == 2
    assert all(_gone(pid) for pid in forked)


def test_ready_timeout_aborts_startup(tmp_path):
    server = Server(tmp_path, {"tcp": free_port()}, SEPARATE + "\nready_timeout: 1000", env={"SLOW_HOOK": "0"})
    started = time.monotonic()

    code = server.wait_exit()

    assert code == 1, server.output
    assert time.monotonic() - started < 10


def test_in_flight_request_finishes_within_close_wait_time(tmp_path):
    port = free_port()
    server = Server(tmp_path, {"tcp": port}, SEPARATE + "\nclose_wait_time: 2000")
    server.wait_ready()

    async def call_then_stop():
        call = asyncio.ensure_future(tcp_call(port, 1, "0.5"))
        await asyncio.sleep(0.1)
        server.proc.send_signal(signal.SIGTERM)
        return await call

    reply = asyncio.run(call_then_stop())
    code = server.wait_exit()

    assert reply == (1, 0, "ok:0.5")
    assert code == 0, server.output


def test_second_sigterm_cuts_close_wait_time_short(tmp_path):
    server = Server(tmp_path, {"tcp": free_port()}, SEPARATE + "\nclose_wait_time: 30000")
    server.wait_ready()

    started = time.monotonic()
    server.proc.send_signal(signal.SIGTERM)
    time.sleep(0.5)
    server.proc.send_signal(signal.SIGTERM)
    code = server.wait_exit()

    assert code == 0, server.output
    assert time.monotonic() - started < 10
    assert server.output.count("HOOK_STOP") == 2


def test_sigterm_to_the_process_group_keeps_the_shutdown_order(tmp_path):
    port = free_port()
    server = Server(tmp_path, {"tcp": port}, SEPARATE + "\nclose_wait_time: 2000", start_new_session=True)
    server.wait_ready()

    async def call_then_stop():
        call = asyncio.ensure_future(tcp_call(port, 1, "0.5"))
        await asyncio.sleep(0.1)
        os.killpg(server.proc.pid, signal.SIGTERM)
        return await call

    reply = asyncio.run(call_then_stop())
    code = server.wait_exit()

    assert reply == (1, 0, "ok:0.5")
    assert code == 0, server.output
    assert server.output.count("HOOK_STOP") == 2


def test_dead_rpc_task_shuts_the_server_down_with_status_1(tmp_path):
    server = Server(tmp_path, {"tcp": free_port()}, SEPARATE)
    server.wait_ready()
    children = _children(server.proc.pid)
    assert len(children) == 2

    os.kill(children[0], signal.SIGKILL)
    code = server.wait_exit()

    assert code == 1, server.output
    assert server.output.count("HOOK_STOP") == 1


def test_children_exit_when_the_io_process_is_killed(tmp_path):
    server = Server(tmp_path, {"tcp": free_port()}, SEPARATE)
    server.wait_ready()
    children = _children(server.proc.pid)
    assert len(children) == 2

    server.proc.kill()
    server.wait_exit()
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline and not all(_gone(pid) for pid in children):
        time.sleep(0.1)

    assert all(_gone(pid) for pid in children)
