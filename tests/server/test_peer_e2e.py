# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.
"""``server.mode: peer`` end to end: startup, SO_REUSEPORT serving, shutdown."""

from __future__ import annotations

import asyncio
import os
import re
import signal
import socket
import subprocess
import sys
import time
from typing import List

import pytest

from tests.server import _e2e_support as e2e

pytestmark = pytest.mark.skipif(os.name != "posix", reason="peer mode needs fork")
linux_only = pytest.mark.skipif(sys.platform != "linux", reason="SO_REUSEPORT only balances load on Linux")

PEER = "mode: peer\nworker_num: 2"
BOUND = r"peer: 2 workers bound in (\d+) ms"


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


def _wait_gone(pids: List[int], timeout: float) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if all(_gone(pid) for pid in pids):
            return True
        time.sleep(0.1)
    return all(_gone(pid) for pid in pids)


def _start(tmp_path, ports, extra: str = PEER, **kwargs) -> e2e.Server:
    server = e2e.Server(tmp_path, ports, extra, **kwargs)
    if not server.wait_output(BOUND):
        server.stop()
        raise AssertionError(f"peer workers not bound:\n{server.output}")
    return server


def test_every_network_is_served_by_every_worker(tmp_path):
    ports = {"tcp": e2e.free_port(), "udp": e2e.free_port(socket.SOCK_DGRAM), "http": e2e.free_port()}
    server = _start(tmp_path, ports)

    tcp = asyncio.run(e2e.tcp_call(ports["tcp"], 1, "info:0"))
    udp = e2e.udp_call(ports["udp"], 1, "info:0")
    http = e2e.http_call(ports["http"], "info:0")
    code = server.stop()

    assert code == 0, server.output
    assert tcp[1] == 0 and udp[1] == 0 and "info:" in http
    assert e2e.parse_info(tcp[2])[2], "each worker serves on its main thread"
    assert server.output.count("HOOK_START") == 2
    assert server.output.count("HOOK_STOP") == 2
    assert "a second instance on this host will bind silently" in server.output


@linux_only
def test_connections_spread_over_both_workers(tmp_path):
    port = e2e.free_port()
    server = _start(tmp_path, {"tcp": port})

    replies = asyncio.run(e2e.tcp_calls(port, ["info:0.05"] * 32))
    code = server.stop()

    assert code == 0, server.output
    assert {e2e.parse_info(msg)[0] for _, _, msg in replies} == {0, 1}


def test_worker_zero_waits_for_every_worker_before_registering(tmp_path):
    server = e2e.Server(tmp_path, {"tcp": e2e.free_port()}, PEER, env={"SLOW_HOOK": "1"})
    assert server.wait_output(BOUND, timeout=20), server.output
    elapsed_ms = int(re.search(BOUND, server.output).group(1))
    code = server.stop()

    assert code == 0, server.output
    assert elapsed_ms >= 4000


@pytest.mark.parametrize("failing", ["1", "0"])
def test_failing_on_start_exits_with_status_1_and_leaves_no_child(tmp_path, failing):
    server = e2e.Server(tmp_path, {"tcp": e2e.free_port()}, PEER, env={"FAIL_HOOK": failing})
    time.sleep(1)
    children = _children(server.proc.pid)
    code = server.wait_exit()

    assert code == 1, server.output
    assert "hook failed on purpose" in server.output
    assert not re.search(BOUND, server.output)
    assert _wait_gone(children, 10)


def test_ready_timeout_aborts_startup(tmp_path):
    server = e2e.Server(tmp_path, {"tcp": e2e.free_port()}, PEER + "\nready_timeout: 1000", env={"SLOW_HOOK": "1"})
    began = time.monotonic()
    code = server.wait_exit()

    assert code == 1, server.output
    assert time.monotonic() - began < 10
    assert "not ready within ready_timeout" in server.output


def test_in_flight_request_finishes_within_close_wait_time(tmp_path):
    port = e2e.free_port()
    server = _start(tmp_path, {"tcp": port}, PEER + "\nclose_wait_time: 2000")

    async def _call_then_sigterm():
        call = asyncio.ensure_future(e2e.tcp_call(port, 1, "1"))
        await asyncio.sleep(0.2)
        server.proc.send_signal(signal.SIGTERM)
        return await call

    reply = asyncio.run(_call_then_sigterm())
    code = server.wait_exit()

    assert reply[1] == 0 and reply[2] == "ok:1"
    assert code == 0, server.output


def test_second_sigterm_cuts_close_wait_time_short(tmp_path):
    server = _start(tmp_path, {"tcp": e2e.free_port()}, PEER + "\nclose_wait_time: 30000")
    began = time.monotonic()
    server.proc.send_signal(signal.SIGTERM)
    time.sleep(0.5)
    server.proc.send_signal(signal.SIGTERM)
    code = server.wait_exit()

    assert code == 0, server.output
    assert time.monotonic() - began < 15
    assert server.output.count("HOOK_STOP") == 2


def test_sigterm_to_the_process_group_stops_every_worker(tmp_path):
    server = _start(tmp_path, {"tcp": e2e.free_port()}, start_new_session=True)
    os.killpg(server.proc.pid, signal.SIGTERM)
    code = server.wait_exit()

    assert code == 0, server.output
    assert server.output.count("HOOK_STOP") == 2


@pytest.mark.parametrize("extra", ["", "\nmax_restarts: 3"])
def test_dead_worker_shuts_the_server_down_with_status_1(tmp_path, extra):
    server = _start(tmp_path, {"tcp": e2e.free_port()}, PEER + extra)
    children = _children(server.proc.pid)
    assert len(children) == 1, server.output
    os.kill(children[0], signal.SIGKILL)
    code = server.wait_exit()

    assert code == 1, server.output


def test_workers_exit_when_worker_zero_is_killed(tmp_path):
    server = _start(tmp_path, {"tcp": e2e.free_port()})
    children = _children(server.proc.pid)
    assert children, server.output
    server.proc.kill()
    server.proc.wait()

    assert _wait_gone(children, 5.0 + 5)


def test_task_is_rejected_before_the_fork(tmp_path):
    server = e2e.Server(tmp_path, {"tcp": e2e.free_port()}, PEER, env={"TASK": "1"})
    code = server.wait_exit()

    assert code != 0, server.output
    assert "peer mode does not support Task" in server.output
    assert "forked child" not in server.output


def test_named_timer_runs_on_its_worker_only(tmp_path):
    server = _start(tmp_path, {"tcp": e2e.free_port()}, env={"TIMER_NAME": "worker:1"})
    assert server.wait_output(r"NAMED_TIMER info:1:", timeout=10), server.output
    time.sleep(0.5)
    code = server.stop()

    assert code == 0, server.output
    assert server.output.count("NAMED_TIMER") == 1


def test_single_worker_serves_and_warns(tmp_path):
    port = e2e.free_port()
    server = e2e.Server(tmp_path, {"tcp": port}, "mode: peer\nworker_num: 1")
    assert server.wait_output(r"peer: 1 workers bound"), server.output

    reply = asyncio.run(e2e.tcp_call(port, 1, "info:0"))
    code = server.stop()

    assert code == 0, server.output
    assert reply[1] == 0 and e2e.parse_info(reply[2])[0] == 0
    assert "worker_num=1 in peer mode" in server.output
