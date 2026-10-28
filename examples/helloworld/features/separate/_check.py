# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Start the separate-mode example and check the behavior it actually has.

One normal start covers round-robin, address identity, stream stickiness,
socket ownership, the Task process and the timers. Further starts cover
the scheduler pin, startup barriers and shutdown. In-flight limits, the
queue and overload replies are not implemented, so nothing here asks for
them. Drain waits out ``close_wait_time``; it does not watch the in-flight
count.

macOS ``SO_REUSEPORT`` does not spread UDP, HTTP or the user protocol, so
those replies only have to come from an rpc task. Socket counts are not
relaxed.
"""

from __future__ import annotations

import argparse
import asyncio
import http.client
import os
import re
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Callable
from typing import Dict
from typing import List
from typing import Optional
from typing import Set
from typing import Tuple

import yaml

_ROOT = Path(__file__).resolve().parents[2]
_STUB = str(_ROOT / "stub")
_ROOT_S = str(_ROOT)
if _STUB not in sys.path:
    sys.path.insert(0, _STUB)
if _ROOT_S not in sys.path:
    sys.path.insert(0, _ROOT_S)

from trpc_sdk.abc import TrpcContext  # noqa: E402
from trpc_sdk.abc import get_message  # noqa: E402
from trpc_sdk.client import Options  # noqa: E402
from trpc_sdk.config import TrpcConfig  # noqa: E402
from trpc_sdk.plugin import init_plugins  # noqa: E402
from trpc_sdk.serialization import SerializationType  # noqa: E402
from trpc_sdk.stream import ClientStreamDesc  # noqa: E402
from trpc_sdk.stream import default_stream_client  # noqa: E402
from trpc_sdk.transport import get_default_client_transport  # noqa: E402
from trpc_sdk.transport import ClientConn  # noqa: E402

from features.separate._servicer import HTTP_PATH  # noqa: E402
from features.separate._servicer import STREAM_METHOD  # noqa: E402
from features.thread._frame import encode  # noqa: E402
from features.thread._frame import read_frame  # noqa: E402
from features.thread._frame import tcp_pipelined  # noqa: E402
from trpc_trpc_python_group_helloworld import pb  # noqa: E402
from trpc_trpc_python_group_helloworld import rpc  # noqa: E402

_DIR = Path(__file__).resolve().parent
_CONF = _DIR / "conf" / "trpc_python.yaml"
_SCRIPT = _DIR / "server.py"

_BOUND = "rpc tasks bound"
_UNNAMED_TIMER = "have no name, so each of the"
_HOOK_RE = re.compile(r"HOOK_START (\d+) (\d+) (\d+)")
_STOP_RE = re.compile(r"HOOK_STOP (\d+) (\d+)")
_WORKER_RE = re.compile(r"WORKER_EXIT (\d+)")
_MASTER_RE = re.compile(r"MASTER_EXIT (\d+)")
_TASK_RE = re.compile(r"TASK_READY (\d+)")
_TIMER_ALL_RE = re.compile(r"TIMER_ALL (\d+) (\d+)")
_TIMER_NAMED_RE = re.compile(r"TIMER_NAMED (\d+) (\d+)")
_RPC_RE = re.compile(r"rpc:(-?\d+):(\d+):(\d+):(\S+)")
_STREAM_RE = re.compile(r"stream:(-?\d+):(\d+)")
_USER_RE = re.compile(r"user:(-?\d+):(\d+)")
_TASK_REPLY_RE = re.compile(r"task:.+:(\d+)")
_CLIENT_READY = False

# Samples on UDP / HTTP / the user protocol. Linux must see both rpc tasks;
# a handful is enough once the kernel balances SO_REUSEPORT.
_SPREAD_SAMPLES = 16


@dataclass(frozen=True)
class Endpoint:
    """One YAML ``server.service[]`` entry."""

    name: str
    port: int
    network: str
    protocol: str


@dataclass(frozen=True)
class Spec:
    """What the example YAML says the process tree should look like."""

    rpc_task_num: int
    close_wait_time: int
    endpoints: Tuple[Endpoint, ...]

    def find_all(self, network: str, protocol: str) -> List[Endpoint]:
        """Return every endpoint with this network and protocol, YAML order.

        Args:
            network: ``tcp`` or ``udp``.
            protocol: ``trpc``, ``http`` or ``user``.

        Returns:
            Matching endpoints.
        """
        return [item for item in self.endpoints if item.network == network and item.protocol == protocol]

    def find(self, network: str, protocol: str) -> Endpoint:
        """Return the first endpoint with this network and protocol.

        Args:
            network: ``tcp`` or ``udp``.
            protocol: ``trpc``, ``http`` or ``user``.

        Returns:
            The matching endpoint.

        Raises:
            AssertionError: The YAML has no such endpoint.
        """
        found = self.find_all(network, protocol)
        if not found:
            raise AssertionError(f"no {network}/{protocol} endpoint in the YAML")
        return found[0]


@dataclass(frozen=True)
class RpcReply:
    """One ``rpc:<index>:<count>:<pid>:<service>`` reply."""

    index: int
    count: int
    pid: int
    service: str


def load_spec(path: Path) -> Spec:
    """Read rpc task count and listen endpoints out of a YAML file.

    Args:
        path: Example ``trpc_python.yaml``.

    Returns:
        The expectations the running process is checked against.
    """
    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    server = data["server"]
    endpoints = tuple(
        Endpoint(name=item["name"], port=int(item["port"]), network=item["network"], protocol=item["protocol"])
        for item in server["service"])
    return Spec(rpc_task_num=int(data["global"]["processmodel"]["handle_num"]),
                close_wait_time=int(server.get("close_wait_time") or 0),
                endpoints=endpoints)


class ManagedServer:
    """One ``server.py`` subprocess, with its stdout collected while it runs."""

    def __init__(self, conf: Path, env: Optional[Dict[str, str]] = None, new_session: bool = False) -> None:
        self.conf = conf
        self.env_extra = env or {}
        self.new_session = new_session
        self.output = ""
        self.returncode: Optional[int] = None
        self.proc: Optional[subprocess.Popen] = None
        self._reader: Optional[threading.Thread] = None
        self._lock = threading.Lock()

    @property
    def pid(self) -> int:
        """OS pid of the I/O process."""
        if self.proc is None:
            raise RuntimeError("server is not started")
        return self.proc.pid

    def start(self) -> None:
        """Spawn ``server.py``. ``PYTHONUNBUFFERED`` so hooks show up at once."""
        env = os.environ.copy()
        env["PYTHONUNBUFFERED"] = "1"
        env.update(self.env_extra)
        self.proc = subprocess.Popen(  # pylint: disable=consider-using-with
            [sys.executable, "-u", str(_SCRIPT), "--conf", str(self.conf)],
            cwd=str(_ROOT),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            env=env,
            start_new_session=self.new_session,
        )
        self._reader = threading.Thread(target=self._read_stdout, name="server-stdout", daemon=True)
        self._reader.start()

    def _read_stdout(self) -> None:
        """Append the server's combined stdout/stderr until the pipe closes."""
        assert self.proc is not None and self.proc.stdout is not None
        for line in self.proc.stdout:
            with self._lock:
                self.output += line

    def text(self) -> str:
        """Return the output collected so far."""
        with self._lock:
            return self.output

    def exited(self) -> bool:
        """Whether the I/O process has already exited."""
        return self.proc is not None and self.proc.poll() is not None

    def wait(self, timeout: float) -> int:
        """Wait until the I/O process exits.

        Args:
            timeout: Seconds before the process is killed.

        Returns:
            Process exit code.

        Raises:
            AssertionError: It was still alive after ``timeout``.
        """
        if self.proc is None:
            raise AssertionError("server was not started")
        if self.returncode is not None:
            return self.returncode
        try:
            self.proc.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            self.proc.kill()
            self.proc.wait(timeout=5)
            if self._reader is not None:
                self._reader.join(timeout=5)
            self.returncode = self.proc.returncode if self.proc.returncode is not None else -1
            raise AssertionError(f"server still running after {timeout:.0f}s (killed):\n{self.text()}") from None
        if self._reader is not None:
            self._reader.join(timeout=5)
        self.returncode = self.proc.returncode if self.proc.returncode is not None else -1
        return self.returncode

    def signal(self, sig: int) -> None:
        """Send ``sig`` to the I/O process.

        Args:
            sig: Signal number.
        """
        if self.proc is None or self.proc.poll() is not None:
            return
        self.proc.send_signal(sig)

    def stop(self, sig: Optional[int] = None, timeout: float = 20) -> int:
        """Signal the server if it is still up, and wait for it.

        Args:
            sig: Signal sent when the process is still running. Defaults to
                SIGTERM. The default is applied in the body: a default
                argument would see this class's ``signal`` method, not the
                ``signal`` module.
            timeout: Seconds to wait after the signal.

        Returns:
            Process exit code. ``-1`` when the process was never started.
        """
        if sig is None:
            sig = signal.SIGTERM
        if self.proc is None:
            return -1
        if self.returncode is not None:
            return self.returncode
        if self.proc.poll() is None:
            try:
                self.proc.send_signal(sig)
            except ProcessLookupError:
                pass
        try:
            return self.wait(timeout)
        except AssertionError:
            if self.returncode is not None:
                return self.returncode
            raise


def _variant(updates: Dict[str, object],
             processmodel_updates: Optional[Dict[str, object]] = None) -> Tuple[tempfile.TemporaryDirectory, Path]:
    """Write a copy of the example YAML with ``server`` fields replaced.

    Args:
        updates: Keys merged into the ``server`` mapping.
        processmodel_updates: Keys merged into ``global.processmodel``.

    Returns:
        The temporary directory (keep it alive) and the YAML path.
    """
    data = yaml.safe_load(_CONF.read_text(encoding="utf-8"))
    data["server"].update(updates)
    data["global"]["processmodel"].update(processmodel_updates or {})
    directory = tempfile.TemporaryDirectory(prefix="trpc-separate-")
    path = Path(directory.name) / "trpc_python.yaml"
    path.write_text(yaml.safe_dump(data, sort_keys=False), encoding="utf-8")
    return directory, path


def _can_connect(port: int) -> bool:
    """Return True when something accepts TCP on ``127.0.0.1:port``."""
    try:
        with socket.create_connection(("127.0.0.1", port), timeout=0.2):
            return True
    except OSError:
        return False


def _alive(pid: int) -> bool:
    """Return True when ``pid`` still exists (a zombie counts as alive)."""
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def _child_pids(pid: int) -> List[int]:
    """Return live children of ``pid``, skipping the multiprocessing tracker.

    Args:
        pid: Parent pid.

    Returns:
        Child pids. Empty when ``pgrep`` sees none.
    """
    proc = subprocess.run(["pgrep", "-P", str(pid)], capture_output=True, text=True, check=False)
    if proc.returncode not in (0, 1):
        raise AssertionError(f"pgrep failed ({proc.returncode}): {proc.stderr}")
    children = []
    for child in proc.stdout.split():
        command = subprocess.run(["ps", "-o", "command=", "-p", child], capture_output=True, text=True, check=False)
        if "resource_tracker" in command.stdout:
            continue
        children.append(int(child))
    return children


def _wait_dead(pids: List[int], timeout: float) -> None:
    """Block until none of ``pids`` exist.

    Args:
        pids: Processes that should exit.
        timeout: Seconds before giving up.

    Raises:
        AssertionError: Any pid was still alive at the deadline.
    """
    deadline = time.monotonic() + timeout
    pending = list(pids)
    while pending and time.monotonic() < deadline:
        pending = [pid for pid in pending if _alive(pid)]
        if pending:
            time.sleep(0.1)
    if pending:
        raise AssertionError(f"processes still alive after {timeout:.0f}s: {pending}")


def _socket_fds(pid: int, port: int, tcp_listen: bool) -> Set[str]:
    """Return the lsof FD column of sockets on ``port`` for ``pid``.

    Args:
        pid: Process to inspect.
        port: Port to filter on.
        tcp_listen: Count TCP LISTEN sockets. Otherwise count UDP sockets.

    Returns:
        Unique FD names. Empty when ``lsof`` sees nothing.
    """
    proto = "TCP" if tcp_listen else "UDP"
    cmd = ["lsof", "-nP", "-a", "-p", str(pid), f"-i{proto}:{port}"]
    if tcp_listen:
        cmd.append("-sTCP:LISTEN")
    proc = subprocess.run(cmd, capture_output=True, text=True, check=False)
    if proc.returncode not in (0, 1):
        raise AssertionError(f"lsof failed ({proc.returncode}): {proc.stderr}")
    fds: Set[str] = set()
    for line in proc.stdout.splitlines():
        if f":{port}" not in line:
            continue
        parts = line.split()
        if len(parts) < 4 or parts[0] == "COMMAND":
            continue
        fds.add(parts[3])
    return fds


def _parse_rpc(text: str) -> RpcReply:
    """Pull one unary report out of a reply or HTTP body.

    Args:
        text: Any string that contains one ``rpc:...`` report.

    Returns:
        The parsed report.

    Raises:
        AssertionError: ``text`` has no report.
    """
    match = _RPC_RE.search(text)
    if match is None:
        raise AssertionError(f"no rpc report in {text!r}")
    index, count, pid, service = match.groups()
    return RpcReply(int(index), int(count), int(pid), service)


def _hooks(output: str) -> Dict[int, int]:
    """Map rpc-task index to pid from ``HOOK_START`` lines.

    Args:
        output: Server stdout collected so far.

    Returns:
        ``index -> pid``.
    """
    table: Dict[int, int] = {}
    for index_s, _count_s, pid_s in _HOOK_RE.findall(output):
        table[int(index_s)] = int(pid_s)
    return table


def _ensure_client(conf: Path) -> None:
    """Load the example YAML so the public client has a log plugin."""
    global _CLIENT_READY  # pylint: disable=global-statement
    if _CLIENT_READY:
        return
    TrpcConfig.load(str(conf), "utf-8")
    init_plugins()
    _CLIENT_READY = True


def _wait_output(server: ManagedServer, needle: str, timeout: float) -> None:
    """Block until ``needle`` shows up in the server log.

    Args:
        server: Running server.
        needle: Substring to wait for.
        timeout: Seconds before giving up.

    Raises:
        AssertionError: The process exited, or the deadline passed.
    """
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if server.exited():
            raise AssertionError(f"server exited early ({server.proc.returncode}):\n{server.text()}")
        if needle in server.text():
            return
        time.sleep(0.05)
    raise AssertionError(f"log missing {needle!r} after {timeout:.0f}s:\n{server.text()}")


def _wait_ready(server: ManagedServer, spec: Spec, timeout: float = 20) -> Dict[int, int]:
    """Block until both rpc tasks, the Task and the timers have reported in.

    Args:
        server: Running server.
        spec: Expected rpc task count.
        timeout: Seconds before giving up.

    Returns:
        ``HOOK_START`` index to pid.

    Raises:
        AssertionError: The process exited, or the deadline passed.
    """
    port = spec.find("tcp", "trpc").port
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if server.exited():
            raise AssertionError(f"server exited early ({server.proc.returncode}):\n{server.text()}")
        output = server.text()
        hooks = _hooks(output)
        timers = {int(index) for index, _pid in _TIMER_ALL_RE.findall(output)}
        named = _TIMER_NAMED_RE.findall(output)
        task_ready = len(_TASK_RE.findall(output)) == 1
        bound = f"separate: {spec.rpc_task_num} {_BOUND}" in output
        if (bound and task_ready and hooks.keys() == set(range(spec.rpc_task_num)) and timers == set(
                range(spec.rpc_task_num)) and len(named) == 1 and _can_connect(port)):
            return hooks
        time.sleep(0.05)
    raise AssertionError(f"server not ready within {timeout:.0f}s:\n{server.text()}")


def _assert_identity(server: ManagedServer, spec: Spec, hooks: Dict[int, int]) -> int:
    """Check hook lines, the task pid and that the I/O process is not a handler.

    Args:
        server: Ready server.
        spec: Expected counts.
        hooks: ``HOOK_START`` table.

    Returns:
        Pid of the Task process.

    Raises:
        AssertionError: A pid or a count disagrees with the process tree.
    """
    output = server.text()
    counts = {int(count) for _index, count, _pid in _HOOK_RE.findall(output)}
    if counts != {spec.rpc_task_num}:
        raise AssertionError(f"HOOK_START count {counts}, want {spec.rpc_task_num}")
    if server.pid in hooks.values():
        raise AssertionError(f"I/O pid {server.pid} printed HOOK_START")
    children = set(_child_pids(server.pid))
    if set(hooks.values()) - children:
        raise AssertionError(f"HOOK_START pids {set(hooks.values())} are not children of {server.pid} ({children})")
    tasks = [int(pid) for pid in _TASK_RE.findall(output)]
    if len(tasks) != 1:
        raise AssertionError(f"expected one TASK_READY, found {tasks}")
    task_pid = tasks[0]
    if task_pid not in children or task_pid in hooks.values():
        raise AssertionError(f"task pid {task_pid} is not a distinct child of {children}")
    if children != set(hooks.values()) | {task_pid}:
        raise AssertionError(f"children {children}, want 2 rpc tasks + 1 task")
    return task_pid


def _assert_timers(output: str, spec: Spec, hooks: Dict[int, int]) -> None:
    """Check unnamed timers on every rpc task and the named one only on 0.

    Args:
        output: Server stdout.
        spec: Expected rpc task count.
        hooks: ``HOOK_START`` table the timer pids must match.
    """
    if _UNNAMED_TIMER not in output:
        raise AssertionError(f"log missing the unnamed-timer warning:\n{output}")
    all_timers = [(int(index), int(pid)) for index, pid in _TIMER_ALL_RE.findall(output)]
    if {index for index, _pid in all_timers} != set(range(spec.rpc_task_num)):
        raise AssertionError(f"TIMER_ALL indexes {all_timers}, want {set(range(spec.rpc_task_num))}")
    for index, pid in all_timers:
        if hooks.get(index) != pid:
            raise AssertionError(f"TIMER_ALL {index} pid {pid} is not hook pid {hooks.get(index)}")
    named = [(int(index), int(pid)) for index, pid in _TIMER_NAMED_RE.findall(output)]
    if named != [(0, hooks[0])]:
        raise AssertionError(f"TIMER_NAMED {named}, want one line for index 0 pid {hooks[0]}")


def _assert_listen(io_pid: int, rpc_pids: List[int], spec: Spec) -> None:
    """Check which process owns each listen socket.

    TCP listen sockets belong to the I/O process, one each. UDP, HTTP and
    the user protocol each have ``rpc_task_num`` sockets, all on rpc tasks.

    Args:
        io_pid: I/O process pid.
        rpc_pids: Rpc task pids.
        spec: Endpoints and rpc task count.
    """
    for endpoint in spec.find_all("tcp", "trpc"):
        found = _socket_fds(io_pid, endpoint.port, tcp_listen=True)
        if len(found) != 1:
            raise AssertionError(f"TCP {endpoint.port} has {len(found)} LISTEN sockets on the I/O process, want 1")
        for pid in rpc_pids:
            extra = _socket_fds(pid, endpoint.port, tcp_listen=True)
            if extra:
                raise AssertionError(f"rpc task {pid} listens on TCP {endpoint.port}: {extra}")

    owned = [spec.find("udp", "trpc"), spec.find("tcp", "http"), spec.find("udp", "user")]
    for endpoint in owned:
        tcp = endpoint.protocol == "http"
        io_fds = _socket_fds(io_pid, endpoint.port, tcp_listen=tcp)
        if io_fds:
            raise AssertionError(f"I/O process owns {endpoint.protocol} {endpoint.port}: {io_fds}")
        per_task = {pid: _socket_fds(pid, endpoint.port, tcp_listen=tcp) for pid in rpc_pids}
        total = sum(len(fds) for fds in per_task.values())
        if total != spec.rpc_task_num or any(len(fds) != 1 for fds in per_task.values()):
            raise AssertionError(f"{endpoint.protocol} {endpoint.port} sockets {per_task}, "
                                 f"want one on each of {spec.rpc_task_num} rpc tasks")


async def _unary(port: int, service: str, msg: str, callee: Optional[str] = None) -> Tuple[int, str]:
    """Send one unary request on its own TCP connection.

    Args:
        port: Server port.
        service: Service name in the method path.
        msg: ``HelloRequest.msg``.
        callee: Request-head callee. Defaults to ``service``.

    Returns:
        ``(ret, reply.msg)``.
    """
    reader, writer = await asyncio.open_connection("127.0.0.1", port)
    try:
        writer.write(encode(1, msg, service, callee=callee))
        await writer.drain()
        _request_id, ret, reply = await read_frame(reader)
        return ret, reply
    finally:
        writer.close()


def _assert_round_robin(spec: Spec) -> None:
    """Six unary calls on one connection alternate across the two rpc tasks.

    The cursor increments before it is used, so the first frame goes to
    index 1 when nothing has been scheduled yet.

    Args:
        spec: Endpoints. The first tRPC TCP port is the one called.
    """
    endpoint = spec.find("tcp", "trpc")
    replies = asyncio.run(tcp_pipelined(endpoint.port, endpoint.name, ["ping"] * 6))
    if len(replies) != 6:
        raise AssertionError(f"pipeline returned {len(replies)} replies, want 6")
    indexes = []
    for request_id in range(1, 7):
        ret, msg = replies[request_id]
        if ret != 0:
            raise AssertionError(f"pipeline ret {ret}: {msg}")
        report = _parse_rpc(msg)
        if report.count != spec.rpc_task_num or report.service != endpoint.name:
            raise AssertionError(f"reply {report} does not match {endpoint.name} count {spec.rpc_task_num}")
        indexes.append(report.index)
    if indexes != [1, 0, 1, 0, 1, 0]:
        raise AssertionError(f"round-robin indexes {indexes}, want [1, 0, 1, 0, 1, 0]")


def _assert_address_index(spec: Spec) -> None:
    """The reply service is the listen address, not the request-head callee.

    Both calls write the first service's name into the callee field. The
    method path still names the port being called, which is what the
    handler is registered under.

    Args:
        spec: The two tRPC TCP endpoints.
    """
    first, second = spec.find_all("tcp", "trpc")

    async def _both() -> Tuple[RpcReply, RpcReply]:
        _ret_a, msg_a = await _unary(first.port, first.name, "ping", callee=first.name)
        _ret_b, msg_b = await _unary(second.port, second.name, "ping", callee=first.name)
        return _parse_rpc(msg_a), _parse_rpc(msg_b)

    got_a, got_b = asyncio.run(_both())
    if got_a.service != first.name or got_b.service != second.name:
        raise AssertionError(f"services {got_a.service} and {got_b.service}, want {first.name} and {second.name}")


class _SharedFrames:
    """One TCP connection whose frames are split by stream id.

    ``default_stream_client`` dials one socket per stream. The stickiness
    check needs both streams on one session, so this connection is what
    both client streams read and write. A single reader task appends each
    frame to the buffer of the stream id in its head.
    """

    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        self.writer = writer
        self._reader = reader
        self._bufs: Dict[int, bytearray] = {}
        self._cond = asyncio.Condition()
        self._closed = False
        self._task: Optional[asyncio.Task] = None

    def start(self) -> None:
        """Start the reader that demuxes frames."""
        self._task = asyncio.ensure_future(self._demux())

    async def _demux(self) -> None:
        """Read whole frames and wake the stream whose id they carry."""
        try:
            while not self._closed:
                head = await self._reader.readexactly(16)
                total = int.from_bytes(head[4:8], "big")
                rest = await self._reader.readexactly(total - 16)
                stream_id = int.from_bytes(head[10:14], "big")
                async with self._cond:
                    self._bufs.setdefault(stream_id, bytearray()).extend(head + rest)
                    self._cond.notify_all()
        except (asyncio.IncompleteReadError, ConnectionError):
            async with self._cond:
                self._closed = True
                self._cond.notify_all()

    def conn(self, stream_id: int) -> "_FrameConn":
        """Return the read/write handle for one stream id.

        Args:
            stream_id: tRPC stream id this handle reads frames for.

        Returns:
            A connection the client transport can ``read`` and ``write``.
        """
        return _FrameConn(self, stream_id)

    async def close(self) -> None:
        """Stop the reader and close the socket."""
        self._closed = True
        self.writer.close()
        if self._task is not None:
            self._task.cancel()
            await asyncio.wait([self._task], timeout=2)
        async with self._cond:
            self._cond.notify_all()


class _FrameConn:
    """Per-stream view of a :class:`_SharedFrames` socket."""

    def __init__(self, shared: _SharedFrames, stream_id: int) -> None:
        self._shared = shared
        self._stream_id = stream_id

    async def read(self, num: int) -> bytes:
        """Read exactly ``num`` bytes of this stream's frames.

        Args:
            num: Byte count.

        Returns:
            The next ``num`` bytes buffered for this stream id.

        Raises:
            ConnectionError: The socket closed before ``num`` bytes arrived.
        """
        async with self._shared._cond:  # pylint: disable=protected-access
            buf = self._shared._bufs.setdefault(self._stream_id, bytearray())  # pylint: disable=protected-access
            while len(buf) < num and not self._shared._closed:  # pylint: disable=protected-access
                await self._shared._cond.wait()  # pylint: disable=protected-access
            if len(buf) < num:
                raise ConnectionError(f"stream {self._stream_id} closed with {len(buf)} bytes buffered")
            data = bytes(buf[:num])
            del buf[:num]
            return data

    def write(self, data: bytes) -> int:
        """Write ``data`` on the shared socket.

        Args:
            data: One encoded frame.

        Returns:
            How many bytes were handed to the writer.
        """
        self._shared.writer.write(data)
        return len(data)

    def close(self, force: bool = False) -> None:
        """Ignore per-stream close; the shared socket outlives one stream.

        Args:
            force: Unused. The pool asks for a force-close when a stream ends.
        """
        del force

    def check_remote_error(self) -> bool:
        """Report the socket as healthy. The demuxer is the one that sees EOF."""
        return False


async def _open_two_streams(port: int, service: str) -> List[Tuple[int, int]]:
    """Open two streams on one connection and read one reply from each.

    Args:
        port: tRPC TCP port.
        service: Service name. The method is ``/<service>/StreamSay``.

    Returns:
        ``(index, stream_id)`` for each stream, in the order they were opened.
    """
    method = f"/{service}/{STREAM_METHOD}"
    reader, writer = await asyncio.open_connection("127.0.0.1", port)
    shared = _SharedFrames(reader, writer)
    shared.start()
    transport = get_default_client_transport()
    previous = transport.start_stream

    async def _start_stream(ctx, options) -> None:
        stream_id = get_message(ctx).stream_id
        transport.stream_id_to_conn[stream_id] = ClientConn(shared.conn(stream_id), options)

    transport.start_stream = _start_stream
    try:
        reports = []
        for _ in range(2):
            ctx = TrpcContext()
            msg = get_message(ctx)
            msg.set_callee_service_name(service)
            msg.set_client_rpc_name(method)
            msg.set_serialization_type(SerializationType.PB)
            options = (Options().set_target(f"ip://127.0.0.1:{port}").set_network("tcp").set_protocol("trpc").
                       set_serialization_type(SerializationType.PB).set_namespace("Development").set_timeout(8000))
            desc = ClientStreamDesc(method, False)
            stream = await default_stream_client.create_client_stream(ctx, desc, method, [options])
            reply = pb.HelloReply()
            await stream.recv_msg(reply)
            match = _STREAM_RE.search(reply.msg)
            if match is None:
                raise AssertionError(f"no stream report in {reply.msg!r}")
            index, stream_id = int(match.group(1)), int(match.group(2))
            if stream_id != stream.stream_id:
                raise AssertionError(f"reply stream_id {stream_id} is not the client's {stream.stream_id}")
            reports.append((index, stream_id))
            # The handler sends one DATA frame and then CLOSE. Dispatch has
            # to read that CLOSE before this socket goes away, or its next
            # read fails and the client logs an error on a check that passed.
            deadline = asyncio.get_running_loop().time() + 2
            while not stream.server_closed and asyncio.get_running_loop().time() < deadline:
                await asyncio.sleep(0.01)
            if not stream.server_closed:
                await stream.close_send()
        return reports
    finally:
        transport.start_stream = previous
        await shared.close()


def _assert_streams(spec: Spec) -> None:
    """Two streams on one connection stick by ``(session_id + stream_id)``.

    The session id cancels out of the difference, so the indexes and the
    stream ids are congruent modulo ``rpc_task_num``. The pin scheduler is
    not consulted for stream frames; a pin that affected them would put
    both on index 0 and fail this.

    Args:
        spec: Rpc task count and the first tRPC TCP endpoint.
    """
    endpoint = spec.find("tcp", "trpc")
    reports = asyncio.run(_open_two_streams(endpoint.port, endpoint.name))
    (index_a, id_a), (index_b, id_b) = reports
    count = spec.rpc_task_num
    if (index_a - index_b) % count != (id_a - id_b) % count:
        raise AssertionError(f"streams {reports} are not congruent mod {count}")
    if not {index_a, index_b} <= set(range(count)):
        raise AssertionError(f"stream indexes {reports} are outside {set(range(count))}")


def _client_options(port: int, network: str, protocol: str) -> Options:
    """Build options for one unary call to ``127.0.0.1``.

    Args:
        port: Server port.
        network: ``tcp`` or ``udp``.
        protocol: ``trpc`` or ``http``.

    Returns:
        Options aimed at that address.
    """
    return (Options().set_target(f"ip://127.0.0.1:{port}").set_network(network).set_protocol(protocol).
            set_serialization_type(SerializationType.PB).set_namespace("Development").set_timeout(5000))


async def _udp_once(service: str, port: int) -> str:
    """One unary ``SayHello`` over UDP.

    Args:
        service: Callee service name.
        port: UDP port.

    Returns:
        ``HelloReply.msg``.
    """
    proxy = rpc.GreeterClientProxyImpl(service, fix_rpc_name=True, need_clone_ctx=False)
    req = pb.HelloRequest()
    req.msg = "ping"
    reply = await proxy.asyncSayHello(TrpcContext(), req, [_client_options(port, "udp", "trpc")])
    return reply.msg


def _http_once(port: int) -> str:
    """GET the example route.

    Args:
        port: HTTP port.

    Returns:
        Response body.
    """
    conn = http.client.HTTPConnection("127.0.0.1", port, timeout=5)
    try:
        conn.request("GET", HTTP_PATH)
        resp = conn.getresponse()
        body = resp.read()
        if resp.status != 200:
            raise AssertionError(f"{HTTP_PATH} status {resp.status}: {body!r}")
        return body.decode()
    finally:
        conn.close()


def _user_once(port: int) -> str:
    """Send one datagram to the user protocol and read the reply.

    Args:
        port: UDP port.

    Returns:
        Reply text.
    """
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        sock.settimeout(2)
        sock.sendto(b"ping", ("127.0.0.1", port))
        data, _addr = sock.recvfrom(256)
        return data.decode()
    finally:
        sock.close()


def _assert_spread(label: str, indexes_and_pids: List[Tuple[int, int]], rpc_pids: Set[int], count: int) -> None:
    """Require every reply to come from an rpc task.

    On Linux, also require both rpc tasks to show up. macOS
    ``SO_REUSEPORT`` does not have to deliver to more than one socket.

    Args:
        label: What was called, for the failure text.
        indexes_and_pids: One ``(index, pid)`` per reply.
        rpc_pids: Pids that are allowed to answer.
        count: ``rpc_task_num``, the legal index range.
    """
    if not indexes_and_pids:
        raise AssertionError(f"{label} returned nothing")
    pids = {pid for _index, pid in indexes_and_pids}
    indexes = {index for index, _pid in indexes_and_pids}
    if not pids <= rpc_pids or not indexes <= set(range(count)):
        raise AssertionError(f"{label} hit {indexes_and_pids}, rpc tasks are {rpc_pids} indexes {set(range(count))}")
    if sys.platform != "darwin" and pids != rpc_pids:
        raise AssertionError(f"{label} hit pids {pids}, want both {rpc_pids}")


def _assert_udp_http_user(spec: Spec, rpc_pids: Set[int]) -> None:
    """Call UDP, HTTP and the user protocol and check who answered.

    Args:
        spec: Endpoints and rpc task count.
        rpc_pids: Pids from ``HOOK_START``.
    """
    udp = spec.find("udp", "trpc")
    http = spec.find("tcp", "http")
    user = spec.find("udp", "user")
    samples = _SPREAD_SAMPLES if sys.platform != "darwin" else 4

    udp_reports = []
    http_reports = []
    for msg in asyncio.run(_gather_udp(udp.name, udp.port, samples)):
        report = _parse_rpc(msg)
        if report.service != udp.name or report.count != spec.rpc_task_num:
            raise AssertionError(f"udp reply {report} does not match {udp.name}")
        udp_reports.append((report.index, report.pid))
    _assert_spread("udp", udp_reports, rpc_pids, spec.rpc_task_num)

    for _ in range(samples):
        report = _parse_rpc(_http_once(http.port))
        if report.service != http.name or report.count != spec.rpc_task_num:
            raise AssertionError(f"http reply {report} does not match {http.name}")
        http_reports.append((report.index, report.pid))
    _assert_spread("http", http_reports, rpc_pids, spec.rpc_task_num)

    user_reports = []
    for _ in range(samples):
        match = _USER_RE.search(_user_once(user.port))
        if match is None:
            raise AssertionError("user protocol reply did not match user:<index>:<pid>")
        user_reports.append((int(match.group(1)), int(match.group(2))))
    _assert_spread("user", user_reports, rpc_pids, spec.rpc_task_num)


async def _gather_udp(service: str, port: int, samples: int) -> List[str]:
    """Send ``samples`` UDP calls one after another.

    Args:
        service: Callee service name.
        port: UDP port.
        samples: How many calls.

    Returns:
        Reply texts, in order.
    """
    replies = []
    for _ in range(samples):
        replies.append(await _udp_once(service, port))
    return replies


def _assert_task(spec: Spec, task_pid: int, rpc_pids: Set[int]) -> None:
    """A ``task:`` request must be answered by the Task process, not an rpc task.

    Args:
        spec: First tRPC TCP endpoint.
        task_pid: Pid from ``TASK_READY``.
        rpc_pids: Pids that must not appear as the task pid.
    """
    endpoint = spec.find("tcp", "trpc")
    _ret, msg = asyncio.run(_unary(endpoint.port, endpoint.name, "task:hello"))
    match = _TASK_REPLY_RE.search(msg)
    if match is None:
        raise AssertionError(f"task reply {msg!r} does not carry a pid")
    pid = int(match.group(1))
    if pid != task_pid or pid in rpc_pids:
        raise AssertionError(f"task reply pid {pid}, want {task_pid} and not {rpc_pids}")


def _assert_pinned(spec: Spec, rpc_pids: Set[int]) -> None:
    """Unary TCP stays on index 0; UDP and HTTP are not pinned that way.

    Args:
        spec: Endpoints.
        rpc_pids: Legal reply pids.
    """
    endpoint = spec.find("tcp", "trpc")
    replies = asyncio.run(tcp_pipelined(endpoint.port, endpoint.name, ["ping"] * 6))
    indexes = []
    for request_id in range(1, 7):
        ret, msg = replies[request_id]
        if ret != 0:
            raise AssertionError(f"pinned ret {ret}: {msg}")
        indexes.append(_parse_rpc(msg).index)
    if indexes != [0, 0, 0, 0, 0, 0]:
        raise AssertionError(f"pinned unary indexes {indexes}, want all 0")

    # The pin runs only for tRPC-over-TCP frames. On Linux a spread that is
    # entirely index 0 would mean the pin leaked onto UDP/HTTP. macOS may
    # deliver every datagram to one socket, so index 0 there proves nothing.
    udp = spec.find("udp", "trpc")
    http = spec.find("tcp", "http")
    udp_indexes = set()
    for msg in asyncio.run(_gather_udp(udp.name, udp.port, _SPREAD_SAMPLES)):
        report = _parse_rpc(msg)
        if report.pid not in rpc_pids:
            raise AssertionError(f"pinned-mode udp pid {report.pid} is not an rpc task")
        udp_indexes.add(report.index)
    http_indexes = set()
    for _ in range(_SPREAD_SAMPLES):
        report = _parse_rpc(_http_once(http.port))
        if report.pid not in rpc_pids:
            raise AssertionError(f"pinned-mode http pid {report.pid} is not an rpc task")
        http_indexes.add(report.index)
    if sys.platform != "darwin":
        if udp_indexes == {0} or http_indexes == {0}:
            raise AssertionError(f"scheduler pin reached udp {udp_indexes} or http {http_indexes}")


def _assert_shutdown(output: str, code: int, stop_indexes: Set[int], rpc_pids: Set[int], io_pid: int) -> None:
    """Require a clean exit and one stop callback per expected rpc task.

    Args:
        output: Full server output.
        code: Process exit code.
        stop_indexes: ``HOOK_STOP`` indexes that must have fired.
        rpc_pids: Worker-exit pids that must have fired. Same set as the
            rpc tasks that got a stop hook.
        io_pid: Pid that must have printed ``MASTER_EXIT``.
    """
    if code != 0:
        raise AssertionError(f"server exit {code}, want 0:\n{output}")
    if "Traceback" in output:
        raise AssertionError(f"server log contains a traceback:\n{output}")
    stops = {int(index) for index, _pid in _STOP_RE.findall(output)}
    if stops != stop_indexes:
        raise AssertionError(f"HOOK_STOP indexes {stops}, want {stop_indexes}")
    workers = {int(pid) for pid in _WORKER_RE.findall(output)}
    if workers != rpc_pids:
        raise AssertionError(f"WORKER_EXIT pids {workers}, want {rpc_pids}")
    masters = [int(pid) for pid in _MASTER_RE.findall(output)]
    if masters != [io_pid]:
        raise AssertionError(f"MASTER_EXIT {masters}, want [{io_pid}]")


async def _inflight(port: int, service: str, on_the_way: Callable[[], None]) -> str:
    """Sleep inside the handler, run ``on_the_way``, then read the reply.

    Args:
        port: tRPC TCP port.
        service: Service name.
        on_the_way: Called after the request is written and before the reply.

    Returns:
        Reply text.

    Raises:
        AssertionError: The call failed or the reply never came.
    """
    reader, writer = await asyncio.open_connection("127.0.0.1", port)
    try:
        writer.write(encode(1, "info:0.5", service))
        await writer.drain()
        await asyncio.sleep(0.05)
        on_the_way()
        _request_id, ret, msg = await read_frame(reader, timeout=3)
        if ret != 0:
            raise AssertionError(f"in-flight ret {ret}: {msg}")
        return msg
    finally:
        writer.close()


def _run(server: ManagedServer, body: Callable[[], None]) -> None:
    """Start ``server``, run ``body``, and always stop it.

    Args:
        server: Not yet started.
        body: Checks. May stop the server itself.

    Raises:
        Exception: Whatever ``body`` raised, after the server output is printed.
    """
    try:
        server.start()
        body()
    except Exception:
        sys.stderr.write(server.text())
        if not server.exited():
            server.stop()
        elif server.returncode is None:
            server.wait(5)
        raise


def _happy(spec: Spec) -> None:
    """One clean start: scheduling, sockets, task, timers, then a clean stop."""
    server = ManagedServer(_CONF)
    task_pid = 0
    hooks: Dict[int, int] = {}

    def body() -> None:
        nonlocal task_pid, hooks
        hooks = _wait_ready(server, spec)
        task_pid = _assert_identity(server, spec, hooks)
        _assert_timers(server.text(), spec, hooks)
        _assert_listen(server.pid, list(hooks.values()), spec)
        _ensure_client(_CONF)
        _assert_round_robin(spec)
        _assert_address_index(spec)
        _assert_streams(spec)
        _assert_udp_http_user(spec, set(hooks.values()))
        _assert_task(spec, task_pid, set(hooks.values()))
        if _STOP_RE.search(server.text()) or _WORKER_RE.search(server.text()):
            raise AssertionError("stop hooks ran before shutdown")
        code = server.stop()
        _assert_shutdown(server.text(), code, set(hooks), set(hooks.values()), server.pid)

    _run(server, body)


def _pinned(spec: Spec) -> None:
    """``SCHEDULER=pin0`` pins unary TCP and does not pin streams, UDP or HTTP."""
    server = ManagedServer(_CONF, env={"SCHEDULER": "pin0"})

    def body() -> None:
        hooks = _wait_ready(server, spec)
        _ensure_client(_CONF)
        _assert_pinned(spec, set(hooks.values()))
        _assert_streams(spec)
        server.stop()

    _run(server, body)


def _slow_task(spec: Spec) -> None:
    """TCP stays closed until the Task initializer finishes its sleep."""
    server = ManagedServer(_CONF, env={"SLOW_TASK": "1"})
    port = spec.find("tcp", "trpc").port

    def body() -> None:
        deadline = time.monotonic() + 15
        refused = False
        while "TASK_READY" not in server.text():
            if time.monotonic() > deadline:
                raise AssertionError(f"no TASK_READY:\n{server.text()}")
            if server.exited():
                raise AssertionError(f"server exited during SLOW_TASK:\n{server.text()}")
            if _can_connect(port):
                raise AssertionError("TCP accepted before the task initializer finished")
            refused = True
            time.sleep(0.1)
        if not refused:
            raise AssertionError("never observed TCP refusing during SLOW_TASK")
        _wait_ready(server, spec)
        server.stop()

    _run(server, body)


def _slow_hook(spec: Spec) -> None:
    """TCP stays closed until ``on_start`` returns."""
    server = ManagedServer(_CONF, env={"SLOW_HOOK": "1"})
    port = spec.find("tcp", "trpc").port

    def body() -> None:
        deadline = time.monotonic() + 1.5
        while time.monotonic() < deadline:
            if server.exited():
                raise AssertionError(f"server exited during the slow hook:\n{server.text()}")
            if _BOUND in server.text() or _can_connect(port):
                raise AssertionError(f"server accepted before the hook finished:\n{server.text()}")
            time.sleep(0.1)
        _wait_ready(server, spec, timeout=20)
        server.stop()

    _run(server, body)


def _fail_hook() -> None:
    """A raising ``on_start`` exits 1 and leaves no child behind."""
    server = ManagedServer(_CONF, env={"FAIL_HOOK": "1"})
    seen: Set[int] = set()

    def body() -> None:
        server.start()
        deadline = time.monotonic() + 15
        while not server.exited() and time.monotonic() < deadline:
            seen.update(_child_pids(server.pid))
            time.sleep(0.05)
        code = server.wait(10) if not server.exited() else server.wait(5)
        if code != 1:
            raise AssertionError(f"FAIL_HOOK exit {code}, want 1:\n{server.text()}")
        if _BOUND in server.text():
            raise AssertionError(f"FAIL_HOOK still bound rpc tasks:\n{server.text()}")
        _wait_dead(list(seen), 10)

    try:
        body()
    except Exception:
        sys.stderr.write(server.text())
        if not server.exited():
            server.stop()
        raise


def _ready_timeout() -> None:
    """``ready_timeout: 1000`` plus a slow hook exits 1 without binding.

    The hook sleeps longer than the timeout. The process also waits out
    the child-stop grace before it can return, so the wall clock is a few
    seconds rather than exactly one. The log line is what records the 1s
    deadline.
    """
    directory, path = _variant({"ready_timeout": 1000})
    server = ManagedServer(path, env={"SLOW_HOOK": "1"})
    try:
        began = time.monotonic()
        server.start()
        code = server.wait(15)
        elapsed = time.monotonic() - began
        if code != 1:
            raise AssertionError(f"ready_timeout exit {code}, want 1:\n{server.text()}")
        if "not ready within ready_timeout=1000" not in server.text() or _BOUND in server.text():
            raise AssertionError(f"ready_timeout log is wrong:\n{server.text()}")
        if _HOOK_RE.search(server.text()):
            raise AssertionError(f"slow hook finished despite ready_timeout:\n{server.text()}")
        if elapsed >= 15:
            raise AssertionError(f"ready_timeout took {elapsed:.1f}s")
    except Exception:
        sys.stderr.write(server.text())
        if not server.exited():
            server.stop()
        raise
    finally:
        directory.cleanup()


def _sigterm_inflight(spec: Spec) -> None:
    """An in-flight request finishes inside ``close_wait_time``, then exit 0."""
    server = ManagedServer(_CONF)
    endpoint = spec.find("tcp", "trpc")

    def body() -> None:
        hooks = _wait_ready(server, spec)

        def _signal() -> None:
            server.signal(signal.SIGTERM)

        msg = asyncio.run(_inflight(endpoint.port, endpoint.name, _signal))
        if _parse_rpc(msg).count != spec.rpc_task_num:
            raise AssertionError(f"in-flight reply {msg!r}")
        code = server.wait(15)
        _assert_shutdown(server.text(), code, set(hooks), set(hooks.values()), server.pid)

    _run(server, body)


def _double_sigterm(spec: Spec) -> None:
    """A second SIGTERM cuts a long drain short. Hooks still run once each."""
    directory, path = _variant({"close_wait_time": 30000})
    variant = load_spec(path)
    server = ManagedServer(path)
    try:

        def body() -> None:
            hooks = _wait_ready(server, variant)
            began = time.monotonic()
            server.signal(signal.SIGTERM)
            time.sleep(0.2)
            server.signal(signal.SIGTERM)
            code = server.wait(10)
            elapsed = time.monotonic() - began
            if elapsed >= 10:
                raise AssertionError(f"second SIGTERM did not cut the drain ({elapsed:.1f}s)")
            _assert_shutdown(server.text(), code, set(hooks), set(hooks.values()), server.pid)

        _run(server, body)
    finally:
        directory.cleanup()
    del spec


def _kill_group(spec: Spec) -> None:
    """SIGTERM to the process group still lets an in-flight request finish.

    Rpc tasks and the Task process ignore SIGTERM. Only the I/O process,
    which is the session leader, starts the drain.
    """
    server = ManagedServer(_CONF, new_session=True)
    endpoint = spec.find("tcp", "trpc")

    def body() -> None:
        hooks = _wait_ready(server, spec)

        def _signal() -> None:
            os.killpg(server.pid, signal.SIGTERM)

        msg = asyncio.run(_inflight(endpoint.port, endpoint.name, _signal))
        if _parse_rpc(msg).count != spec.rpc_task_num:
            raise AssertionError(f"group-signal reply {msg!r}")
        code = server.wait(15)
        _assert_shutdown(server.text(), code, set(hooks), set(hooks.values()), server.pid)

    _run(server, body)


def _kill_rpc_task(spec: Spec) -> None:
    """Killing one rpc task exits 1, and only the other one runs ``on_stop``."""
    server = ManagedServer(_CONF)

    def body() -> None:
        hooks = _wait_ready(server, spec)
        victim = hooks[0]
        os.kill(victim, signal.SIGKILL)
        code = server.wait(15)
        if code != 1:
            raise AssertionError(f"dead rpc task exit {code}, want 1:\n{server.text()}")
        stops = {int(index) for index, _pid in _STOP_RE.findall(server.text())}
        if stops != {1}:
            raise AssertionError(f"HOOK_STOP indexes {stops}, want {{1}}")

    _run(server, body)
    del spec


def _kill_io(spec: Spec) -> None:
    """SIGKILL of the I/O process is followed by every child exiting."""
    server = ManagedServer(_CONF)

    def body() -> None:
        hooks = _wait_ready(server, spec)
        children = _child_pids(server.pid)
        if set(children) != set(hooks.values()) | set(int(pid) for pid in _TASK_RE.findall(server.text())):
            raise AssertionError(f"children {children} do not match hooks {hooks}")
        os.kill(server.pid, signal.SIGKILL)
        _wait_dead(children, 15)
        server.wait(5)

    _run(server, body)
    del spec


def _io_num_warns(spec: Spec) -> None:
    """``multi_process`` separate with ``io_num: 2`` warns and still runs one I/O process."""
    directory, path = _variant({}, {"io_num": 2})
    server = ManagedServer(path)
    try:

        def body() -> None:
            _wait_ready(server, spec)
            if "single I/O process" not in server.text():
                raise AssertionError(f"io_num warning missing:\n{server.text()}")
            code = server.stop()
            if code != 0:
                raise AssertionError(f"io_num exit {code}:\n{server.text()}")

        _run(server, body)
    finally:
        directory.cleanup()


def main() -> None:
    """Run every separate-mode check against the example YAML.

    Raises:
        AssertionError: A check failed. The server output is on stderr.
    """
    parser = argparse.ArgumentParser(description="Check the separate-mode example")
    parser.parse_args()
    spec = load_spec(_CONF)
    _happy(spec)
    print("ok separate happy", flush=True)
    _pinned(spec)
    print("ok separate pin0", flush=True)
    _slow_task(spec)
    print("ok separate slow task", flush=True)
    _slow_hook(spec)
    print("ok separate slow hook", flush=True)
    _fail_hook()
    print("ok separate fail hook", flush=True)
    _ready_timeout()
    print("ok separate ready timeout", flush=True)
    _sigterm_inflight(spec)
    print("ok separate sigterm inflight", flush=True)
    _double_sigterm(spec)
    print("ok separate double sigterm", flush=True)
    _kill_group(spec)
    print("ok separate process group", flush=True)
    _kill_rpc_task(spec)
    print("ok separate kill rpc task", flush=True)
    _kill_io(spec)
    print("ok separate kill io", flush=True)
    _io_num_warns(spec)
    print("ok separate io_num warns", flush=True)


if __name__ == "__main__":
    main()
