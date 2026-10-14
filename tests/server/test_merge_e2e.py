# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.
"""End-to-end checks of merge mode over real sockets: one loop and N loops.

See :mod:`tests.server._e2e_support` for how the subprocess servicer is
driven.
"""

from __future__ import annotations

import asyncio
import re
import socket
import time
from typing import Dict
from typing import Iterator
from typing import List
from typing import Optional

import pytest

from tests.server._e2e_support import Server
from tests.server._e2e_support import free_port
from tests.server._e2e_support import http_call
from tests.server._e2e_support import parse_info
from tests.server._e2e_support import tcp_call
from tests.server._e2e_support import tcp_calls
from tests.server._e2e_support import tcp_pipelined
from tests.server._e2e_support import udp_call

THREADED = "io_thread_num: 3"


@pytest.fixture
def start_server(tmp_path) -> Iterator:
    servers: List[Server] = []

    def start(ports: Dict[str, int], server_extra: str = "", env: Optional[Dict[str, str]] = None) -> Server:
        server = Server(tmp_path, ports, server_extra, env)
        servers.append(server)
        server.wait_ready()
        return server

    yield start
    for server in servers:
        server.stop()


def _all_ports() -> Dict[str, int]:
    return {"tcp": free_port(), "udp": free_port(socket.SOCK_DGRAM), "http": free_port()}


def _assert_clean_exit(server: Server, code: int) -> None:
    assert code == 0, server.output
    for noise in ("Event loop is closed", "Task was destroyed", "Traceback"):
        assert noise not in server.output, server.output


def test_tcp_udp_http_unary_and_hooks(start_server):
    ports = _all_ports()
    server = start_server(ports)

    assert asyncio.run(tcp_call(ports["tcp"], 7, "0")) == (7, 0, "ok:0")
    assert udp_call(ports["udp"], 8, "0") == (8, 0, "ok:0")
    assert "ok:0" in http_call(ports["http"], "0")

    assert server.stop() == 0
    assert "HOOK_START 0 1 1" in server.output
    assert "HOOK_STOP 0" in server.output


def test_unlimited_by_default_runs_slow_requests_concurrently(start_server):
    ports = {"tcp": free_port()}
    start_server(ports)
    began = time.monotonic()
    results = asyncio.run(tcp_calls(ports["tcp"], ["0.5"] * 50))
    elapsed = time.monotonic() - began
    assert [ret for _, ret, _ in results] == [0] * 50
    assert elapsed < 3.0, elapsed


def test_port_in_use_exits_with_status_one(tmp_path):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as holder:
        holder.bind(("127.0.0.1", 0))
        holder.listen()
        port = holder.getsockname()[1]
        server = Server(tmp_path, {"tcp": port})
        try:
            server.proc.wait(timeout=15)
        finally:
            code = server.stop()
    assert code == 1, server.output


def test_threads_spread_connections_and_keep_each_on_one_thread(start_server):
    ports = {"tcp": free_port()}
    start_server(ports, THREADED)

    replies = asyncio.run(tcp_calls(ports["tcp"], ["info:0"] * 9))
    infos = [parse_info(msg) for _, ret, msg in replies if ret == 0]
    assert len(infos) == 9
    assert {index for index, _, _ in infos} == {0, 1, 2}
    assert not any(main for _, _, main in infos)

    pinned = asyncio.run(tcp_pipelined(ports["tcp"], ["info:0.05"] * 10))
    assert len({parse_info(msg)[1] for _, msg in pinned.values()}) == 1


def test_threads_serve_udp_and_http(start_server):
    ports = _all_ports()
    server = start_server(ports, THREADED)

    for i in range(6):
        request_id, ret, msg = udp_call(ports["udp"], i + 1, "info:0")
        assert (request_id, ret) == (i + 1, 0)
        assert 0 <= parse_info(msg)[0] < 3
    http_threads = {parse_info(re.search(r"info:[^\"]+", http_call(ports["http"], "info:0")).group(0))[0]
                    for _ in range(6)}
    assert http_threads <= {0, 1, 2}

    _assert_clean_exit(server, server.stop())
    for index in range(3):
        assert f"HOOK_START {index} 3 0" in server.output
        assert f"HOOK_STOP {index}" in server.output


def test_blocking_handler_only_blocks_its_own_thread(start_server):
    ports = {"tcp": free_port()}
    start_server(ports, THREADED)

    async def run() -> float:
        blocker = asyncio.ensure_future(tcp_call(ports["tcp"], 1, "block:2"))
        await asyncio.sleep(0.2)
        began = time.monotonic()
        # The next two connections land on the other two threads.
        results = await asyncio.gather(tcp_call(ports["tcp"], 2, "0"), tcp_call(ports["tcp"], 3, "0"))
        elapsed = time.monotonic() - began
        assert [ret for _, ret, _ in results] == [0, 0]
        await blocker
        return elapsed

    assert asyncio.run(run()) < 1.0


def test_hook_failure_on_one_thread_exits_and_stops_the_others(tmp_path):
    server = Server(tmp_path, {"tcp": free_port()}, THREADED, env={"FAIL_HOOK": "1"})
    code = server.wait_exit()
    assert code == 1, server.output
    assert "hook failed on purpose" in server.output
    assert "HOOK_STOP 0" in server.output
    assert "HOOK_STOP 2" in server.output
    assert "HOOK_STOP 1" not in server.output


def test_ready_timeout_exceeded_exits(tmp_path):
    server = Server(tmp_path, {"tcp": free_port()}, THREADED + "\nready_timeout: 1000", env={"SLOW_HOOK": "2"})
    began = time.monotonic()
    code = server.wait_exit()
    assert code == 1, server.output
    assert time.monotonic() - began < 15
    assert "ready" in server.output.lower()


def test_timer_runs_once_on_thread_zero(start_server):
    ports = {"tcp": free_port()}
    server = start_server(ports, THREADED, env={"TIMER": "1"})
    time.sleep(0.5)
    _assert_clean_exit(server, server.stop())
    timer_lines = [line for line in server.output.splitlines() if line.startswith("TIMER ")]
    assert len(timer_lines) == 1, server.output
    assert timer_lines[0].startswith("TIMER info:0:"), timer_lines


def test_threaded_sigterm_with_requests_in_flight_exits_cleanly(start_server):
    ports = _all_ports()
    server = start_server(ports, THREADED)

    async def run() -> None:
        pending = [asyncio.ensure_future(tcp_call(ports["tcp"], i + 1, "0.3")) for i in range(6)]
        await asyncio.sleep(0.1)
        server.proc.send_signal(15)
        await asyncio.gather(*pending, return_exceptions=True)

    asyncio.run(run())
    _assert_clean_exit(server, server.wait_exit())
