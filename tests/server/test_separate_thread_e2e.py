# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.
"""End-to-end checks of the separate thread model (``single_process`` + ``separate``) over real sockets.

I/O runs on the main loop (plus ``io_num - 1`` I/O threads) and every
request is handled on one of the ``handle_num`` handle threads. See
:mod:`tests.server._e2e_support` for how the subprocess servicer is driven.
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


def _separate(io: int, handle: int) -> str:
    return f"mode: single_process\nio_handle_type: separate\nio_num: {io}\nhandle_num: {handle}"


@pytest.fixture
def start_server(tmp_path) -> Iterator:
    servers: List[Server] = []

    def start(ports: Dict[str, int], processmodel: str, server_extra: str = "",
              env: Optional[Dict[str, str]] = None) -> Server:
        server = Server(tmp_path, ports, server_extra, env, processmodel=processmodel)
        servers.append(server)
        server.wait_ready()
        return server

    yield start
    for server in servers:
        server.stop()


def _assert_clean_exit(server: Server, code: int) -> None:
    assert code == 0, server.output
    for noise in ("Event loop is closed", "Task was destroyed", "Traceback"):
        assert noise not in server.output, server.output


@pytest.mark.parametrize("io", [1, 2])
def test_one_connection_spreads_over_handle_threads(start_server, io):
    ports = {"tcp": free_port()}
    server = start_server(ports, _separate(io, 3))

    replies = asyncio.run(tcp_pipelined(ports["tcp"], ["info:0.05"] * 30))
    assert sorted(replies) == list(range(1, 31))
    infos = [parse_info(msg) for ret, msg in replies.values() if ret == 0]
    assert len(infos) == 30
    assert len({index for index, _, _ in infos}) >= 2
    assert not any(main for _, _, main in infos)

    _assert_clean_exit(server, server.stop())
    for index in range(3):
        assert f"HOOK_START {index} 3 0 trpc-handle-{index}" in server.output
        assert f"HOOK_STOP {index}" in server.output


def test_blocking_handler_does_not_block_other_requests(start_server):
    ports = {"tcp": free_port()}
    start_server(ports, _separate(1, 3))

    async def run() -> float:
        blocker = asyncio.ensure_future(tcp_call(ports["tcp"], 1, "block:2"))
        await asyncio.sleep(0.2)
        began = time.monotonic()
        # Round-robin puts the next two requests on the other two threads; a
        # third would wait behind the blocked one.
        results = await tcp_calls(ports["tcp"], ["0"] * 2)
        elapsed = time.monotonic() - began
        assert [ret for _, ret, _ in results] == [0] * 2
        await blocker
        return elapsed

    assert asyncio.run(run()) < 1.0


def test_queue_limit_drops_overflow_and_logs(start_server):
    ports = {"tcp": free_port()}
    server = start_server(ports, _separate(1, 2), "handler_max_concurrency: 1\nhandler_queue_size: 1")

    replies = asyncio.run(tcp_pipelined_with_timeout(ports["tcp"], ["0.5"] * 4))
    assert len(replies) == 3, replies
    server.stop()
    assert "queue" in server.output.lower() and "full" in server.output.lower(), server.output


async def tcp_pipelined_with_timeout(port: int, msgs: List[str]) -> Dict[int, str]:
    """Collect replies until the connection is quiet for 2 seconds."""
    from tests.server._e2e_support import encode  # pylint: disable=import-outside-toplevel
    from tests.server._e2e_support import read_frame  # pylint: disable=import-outside-toplevel
    reader, writer = await asyncio.open_connection("127.0.0.1", port)
    replies = {}
    try:
        writer.write(b"".join(encode(i + 1, msg) for i, msg in enumerate(msgs)))
        await writer.drain()
        while len(replies) < len(msgs):
            try:
                request_id, _, msg = await read_frame(reader, timeout=2)
            except asyncio.TimeoutError:
                break
            replies[request_id] = msg
        return replies
    finally:
        writer.close()


def test_udp_and_http_run_on_handle_threads(start_server):
    ports = {"tcp": free_port(), "udp": free_port(socket.SOCK_DGRAM), "http": free_port()}
    server = start_server(ports, _separate(2, 3))

    for i in range(6):
        request_id, ret, msg = udp_call(ports["udp"], i + 1, "info:0")
        assert (request_id, ret) == (i + 1, 0)
        index, _, main = parse_info(msg)
        assert 0 <= index < 3 and not main
    for _ in range(6):
        index, _, main = parse_info(re.search(r"info:[^\"]+", http_call(ports["http"], "info:0")).group(0))
        assert 0 <= index < 3 and not main

    _assert_clean_exit(server, server.stop())


def test_sigterm_with_requests_in_flight_exits_cleanly(start_server):
    ports = {"tcp": free_port(), "udp": free_port(socket.SOCK_DGRAM)}
    server = start_server(ports, _separate(2, 3))

    async def run() -> None:
        pending = [asyncio.ensure_future(tcp_call(ports["tcp"], i + 1, "0.3")) for i in range(6)]
        await asyncio.sleep(0.1)
        server.proc.send_signal(15)
        await asyncio.gather(*pending, return_exceptions=True)

    asyncio.run(run())
    _assert_clean_exit(server, server.wait_exit())


def test_dead_handle_thread_shuts_the_server_down_with_status_1(start_server):
    ports = {"tcp": free_port()}
    server = start_server(ports, _separate(2, 2))

    async def run() -> None:
        try:
            await asyncio.wait_for(tcp_call(ports["tcp"], 1, "stop_loop"), 2)
        except (asyncio.TimeoutError, OSError, asyncio.IncompleteReadError):
            pass

    asyncio.run(run())
    assert server.wait_exit() == 1, server.output
    assert "exited unexpectedly" in server.output, server.output
