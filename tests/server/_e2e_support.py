# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.
"""Real-socket server harness shared by the merge-mode end-to-end tests.

Each test starts a real server in a subprocess. ``HelloRequest.msg`` drives
the servicer:

* ``"<seconds>"``: ``await asyncio.sleep``, reply ``ok:<msg>``;
* ``"info:<seconds>"``: same sleep, reply ``info:<loop index>:<thread id>:<on main thread>``;
* ``"block:<seconds>"``: ``time.sleep`` (blocks the loop), reply like ``info:``.

Environment variables let a test inject failures: ``FAIL_HOOK=<index>`` makes
that loop's ``on_start`` raise, ``SLOW_HOOK=<index>`` makes it sleep 5 s,
``TIMER=1`` registers a one-shot timer that prints the loop it ran on,
``TIMER_NAME=<name>`` registers a one-shot timer with that name, and
``TASK=1`` registers a Task. The client hand-rolls tRPC frames so these tests
do not depend on the client stack.
"""

from __future__ import annotations

import asyncio
import os
import re
import signal
import socket
import struct
import subprocess
import sys
import textwrap
import threading
import time
from pathlib import Path
from typing import Dict
from typing import List
from typing import Optional
from typing import Tuple

from trpc_sdk._inner.pb import trpc_pb2

REPO_ROOT = Path(__file__).resolve().parents[2]
STUB_DIR = REPO_ROOT / "examples" / "helloworld" / "stub"
SERVICE = "trpc.trpc_python_group.helloworld.Greeter"
# Service names key the server's service table, so each listener gets its
# own; ``fix_rpc_name`` rewrites the method paths to match.
SERVICES = {"tcp": SERVICE + "Tcp", "udp": SERVICE + "Udp", "http": SERVICE + "Http"}

sys.path.insert(0, str(STUB_DIR))
from trpc_trpc_python_group_helloworld import pb  # noqa: E402  pylint: disable=wrong-import-position

SERVER_SCRIPT = textwrap.dedent(
    """
    import asyncio
    import os
    import sys
    import threading
    import time

    sys.path.insert(0, {stub!r})

    from trpc_sdk.server import create_server
    from trpc_sdk.runtime import current_handler_thread
    from trpc_sdk.runtime import register_handler_thread_hook
    from trpc_sdk.utils import TimerTask
    from trpc_sdk.utils import TimerType
    from trpc_sdk.utils import register_timer_task
    from trpc_trpc_python_group_helloworld import pb
    from trpc_trpc_python_group_helloworld import rpc

    MAIN = threading.get_ident()


    def _info():
        info = current_handler_thread()
        ident = threading.get_ident()
        return f"info:{{info.index if info else -1}}:{{ident}}:{{int(ident == MAIN)}}"


    class Servicer(rpc.GreeterServicer):
        async def SayHello(self, ctx, request):
            msg = request.msg or "0"
            if msg.startswith("info:"):
                await asyncio.sleep(float(msg[5:] or 0))
                return pb.HelloReply(msg=_info())
            if msg.startswith("block:"):
                time.sleep(float(msg[6:]))
                return pb.HelloReply(msg=_info())
            await asyncio.sleep(float(msg))
            return pb.HelloReply(msg="ok:" + request.msg)

        async def SayHi(self, ctx, request):
            return pb.HelloReply(msg="hi")


    def on_start(info):
        print("HOOK_START", info.index, info.count, int(threading.get_ident() == MAIN),
              threading.current_thread().name, flush=True)
        if os.environ.get("FAIL_HOOK") == str(info.index):
            raise RuntimeError("hook failed on purpose")
        if os.environ.get("SLOW_HOOK") == str(info.index):
            time.sleep(5)


    def on_stop(info):
        print("HOOK_STOP", info.index, flush=True)


    register_handler_thread_hook(on_start=on_start, on_stop=on_stop)

    if os.environ.get("TIMER"):
        @register_timer_task(type=TimerType.ONCE, interval=0)
        class OnceTimer(TimerTask):
            async def handle(self):
                print("TIMER", _info(), threading.current_thread().name, flush=True)

    if os.environ.get("TIMER_NAME"):
        @register_timer_task(type=TimerType.ONCE, interval=0, name=os.environ["TIMER_NAME"])
        class NamedTimer(TimerTask):
            async def handle(self):
                print("NAMED_TIMER", _info(), flush=True)

    if os.environ.get("TASK"):
        from trpc_sdk.abc import TaskInfo
        from trpc_sdk.plugin import register_task
        register_task("e2e_task", TaskInfo(name="e2e_task", handler=lambda reqs: reqs))

    server = create_server(sys.argv[1])
    rpc.register_GreeterServicer_server(server, Servicer(), fix_rpc_name=True)
    server.serve()
    """)


def free_port(kind: int = socket.SOCK_STREAM) -> int:
    with socket.socket(socket.AF_INET, kind) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def make_config(ports: Dict[str, int], server_extra: str = "", service_extra: str = "") -> str:
    services = []
    for network, protocol in (("tcp", "trpc"), ("udp", "trpc"), ("http", "http")):
        if network not in ports:
            continue
        services.append(f"""
    - name: {SERVICES[network]}
      ip: 127.0.0.1
      port: {ports[network]}
      network: {"udp" if network == "udp" else "tcp"}
      protocol: {protocol}
      timeout: 10000
{textwrap.indent(service_extra.strip(), " " * 6)}""")
    return f"""
global:
  namespace: Development
  env_name: test
server:
  app: test
  server: Greeter
  mode: merge
  close_wait_time: 100
{textwrap.indent(server_extra.strip(), "  ")}
  service:{"".join(services)}
plugins:
  log:
    default:
      - writer: console
        level: info
"""


class Server:
    """One server subprocess; ``output`` holds its stdout+stderr once stopped."""

    def __init__(self,
                 tmp_path: Path,
                 ports: Dict[str, int],
                 server_extra: str = "",
                 env: Optional[Dict[str, str]] = None,
                 script_template: str = SERVER_SCRIPT,
                 service_extra: str = "",
                 start_new_session: bool = False) -> None:
        self.ports = ports
        script = tmp_path / "server_main.py"
        script.write_text(script_template.format(stub=str(STUB_DIR)))
        conf = tmp_path / "trpc_python.yaml"
        conf.write_text(make_config(ports, server_extra, service_extra))
        self.proc = subprocess.Popen(  # pylint: disable=consider-using-with
            [sys.executable, str(script), str(conf)],
            cwd=str(tmp_path),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            env={**os.environ, **(env or {})},
            start_new_session=start_new_session,
        )
        self.output = ""
        # Read stdout line by line while the server runs, so a test can wait
        # for a log line (``wait_output``) and not only for a port.
        self._reader = threading.Thread(target=self._read_output, daemon=True)
        self._reader.start()

    def _read_output(self) -> None:
        for line in self.proc.stdout:
            self.output += line

    def _finish_output(self) -> None:
        self._reader.join(timeout=5)

    def wait_output(self, pattern: str, timeout: float = 15.0) -> bool:
        """Whether ``pattern`` (a regex) shows up in the output within ``timeout`` seconds."""
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if re.search(pattern, self.output):
                return True
            if self.proc.poll() is not None:
                self._finish_output()
                return re.search(pattern, self.output) is not None
            time.sleep(0.05)
        return False

    def wait_ready(self, timeout: float = 15.0) -> None:
        deadline = time.monotonic() + timeout
        port = self.ports.get("tcp") or self.ports["http"]
        while time.monotonic() < deadline:
            if self.proc.poll() is not None:
                break
            try:
                with socket.create_connection(("127.0.0.1", port), timeout=0.2):
                    return
            except OSError:
                time.sleep(0.1)
        self.stop()
        raise AssertionError(f"server not ready:\n{self.output}")

    def wait_exit(self, timeout: float = 20.0) -> int:
        try:
            self.proc.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            return self.stop()
        self._finish_output()
        return self.proc.returncode

    def stop(self) -> int:
        if self.proc.poll() is None:
            self.proc.send_signal(signal.SIGTERM)
        try:
            self.proc.wait(timeout=15)
        except subprocess.TimeoutExpired:
            self.proc.kill()
            self.proc.wait()
        self._finish_output()
        return self.proc.returncode


def func(network: str) -> str:
    return f"/{SERVICES[network]}/SayHello"


def encode(request_id: int, msg: str, network: str = "tcp") -> bytes:
    head = trpc_pb2.RequestProtocol(request_id=request_id,
                                    callee=SERVICES[network].encode(),
                                    func=func(network).encode(),
                                    timeout=10000).SerializeToString()
    body = pb.HelloRequest(msg=msg).SerializeToString()
    total = 16 + len(head) + len(body)
    return struct.pack(">HBBIHIH", 0x930, 0, 0, total, len(head), 0, 0) + head + body


def decode(frame: bytes) -> Tuple[int, int, str]:
    _, _, _, total, head_len, _, _ = struct.unpack(">HBBIHIH", frame[:16])
    head = trpc_pb2.ResponseProtocol()
    head.ParseFromString(frame[16:16 + head_len])
    reply = pb.HelloReply()
    reply.ParseFromString(frame[16 + head_len:total])
    return head.request_id, head.ret, reply.msg


async def read_frame(reader: asyncio.StreamReader, timeout: float = 10) -> Tuple[int, int, str]:
    head = await asyncio.wait_for(reader.readexactly(16), timeout)
    total = struct.unpack(">I", head[4:8])[0]
    rest = await asyncio.wait_for(reader.readexactly(total - 16), timeout)
    return decode(head + rest)


async def tcp_call(port: int, request_id: int, msg: str) -> Tuple[int, int, str]:
    reader, writer = await asyncio.open_connection("127.0.0.1", port)
    try:
        writer.write(encode(request_id, msg))
        await writer.drain()
        return await read_frame(reader)
    finally:
        writer.close()


async def tcp_calls(port: int, msgs: List[str], stagger: float = 0.0) -> List[Tuple[int, int, str]]:
    tasks = []
    for i, msg in enumerate(msgs):
        tasks.append(asyncio.ensure_future(tcp_call(port, i + 1, msg)))
        if stagger:
            await asyncio.sleep(stagger)
    return list(await asyncio.gather(*tasks))


async def tcp_pipelined(port: int, msgs: List[str], timeout: float = 10) -> Dict[int, Tuple[int, str]]:
    """Send every request on one connection, return ``request_id -> (ret, msg)``."""
    reader, writer = await asyncio.open_connection("127.0.0.1", port)
    try:
        writer.write(b"".join(encode(i + 1, msg) for i, msg in enumerate(msgs)))
        await writer.drain()
        replies = {}
        for _ in msgs:
            request_id, ret, reply = await read_frame(reader, timeout)
            replies[request_id] = (ret, reply)
        return replies
    finally:
        writer.close()


def udp_call(port: int, request_id: int, msg: str, timeout: float = 10) -> Tuple[int, int, str]:
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
        sock.settimeout(timeout)
        sock.sendto(encode(request_id, msg, "udp"), ("127.0.0.1", port))
        data, _ = sock.recvfrom(65536)
    return decode(data)


def http_call(port: int, msg: str) -> str:
    import urllib.request  # pylint: disable=import-outside-toplevel
    req = urllib.request.Request(f"http://127.0.0.1:{port}{func('http')}",
                                 data=('{"msg": "%s"}' % msg).encode(),
                                 headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=10) as rsp:
        assert rsp.status == 200
        return rsp.read().decode()


def parse_info(reply: str) -> Tuple[int, int, bool]:
    """``info:<index>:<ident>:<main>`` -> ``(index, ident, on_main_thread)``."""
    _, index, ident, main = reply.split(":")
    return int(index), int(ident), main == "1"
