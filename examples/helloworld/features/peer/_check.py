# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Start the peer-mode example and check it against the design.

One normal start covers process identity, ``SO_REUSEPORT`` sockets, timer
order, connection stickiness and a clean shutdown. Further starts cover
the startup barrier, signals, and the configurations peer rejects or
ignores. In-flight limits are configured and not exercised.

macOS ``SO_REUSEPORT`` does not spread connections, so a reply only has
to come from some worker. Socket counts are not relaxed. A dead worker
takes the whole tree down (design section 9.4 is not implemented). A
second instance is allowed to bind (design section 15 is not implemented).
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
from trpc_sdk.config import default_worker_num  # noqa: E402
from trpc_sdk.plugin import init_plugins  # noqa: E402
from trpc_sdk.serialization import SerializationType  # noqa: E402
from trpc_sdk.stream import ClientStreamDesc  # noqa: E402
from trpc_sdk.stream import default_stream_client  # noqa: E402
from trpc_sdk.transport import get_default_client_transport  # noqa: E402
from trpc_sdk.transport import ClientConn  # noqa: E402

from features.peer._servicer import HTTP_PATH  # noqa: E402
from features.peer._servicer import STREAM_METHOD  # noqa: E402
from features.thread._frame import encode  # noqa: E402
from features.thread._frame import read_frame  # noqa: E402
from features.thread._frame import tcp_calls  # noqa: E402
from features.thread._frame import tcp_pipelined  # noqa: E402
from trpc_trpc_python_group_helloworld import pb  # noqa: E402
from trpc_trpc_python_group_helloworld import rpc  # noqa: E402

_DIR = Path(__file__).resolve().parent
_CONF = _DIR / "conf" / "trpc_python.yaml"
_SCRIPT = _DIR / "server.py"

_BOUND = "workers bound"
_UNNAMED_TIMER = "have no name, so each of the"
_SECOND_INSTANCE = "a second instance on this host will bind silently"
_MACOS_BALANCE = "does not balance load"
_HOOK_RE = re.compile(r"HOOK_START (\d+) (\d+) (\d+) (\d+)")
_STOP_RE = re.compile(r"HOOK_STOP (\d+) (\d+)")
_WORKER_RE = re.compile(r"WORKER_EXIT (\d+)")
_MASTER_RE = re.compile(r"MASTER_EXIT (\d+)")
_TIMER_ALL_RE = re.compile(r"TIMER_ALL (\d+) (\d+)")
_TIMER_NAMED_RE = re.compile(r"TIMER_NAMED (\d+) (\d+)")
_RPC_RE = re.compile(r"rpc:(-?\d+):(\d+):(\d+):(\d+):(\S+)")
_STREAM_RE = re.compile(r"stream:(-?\d+):(\d+)")
_USER_RE = re.compile(r"user:(-?\d+):(\d+)")
_BOUND_RE = re.compile(r"peer: (\d+) workers bound in (\d+) ms")
_CLIENT_READY = False

# Linux must see both workers. A handful is enough once SO_REUSEPORT balances.
_SPREAD_SAMPLES = 32


def _fallback_worker_num() -> int:
    """Return the worker count ``global.processmodel.handle_num: 0`` resolves to.

    Returns:
        :func:`trpc_sdk.config.default_worker_num`: the CPU count, or
        ``DEFAULT_WORKER_NUM_FALLBACK`` when that is unknown.
    """
    return default_worker_num()


@dataclass(frozen=True)
class Endpoint:
    """One YAML ``server.service[]`` entry."""

    name: str
    port: int
    network: str
    protocol: str


@dataclass(frozen=True)
class Spec:
    """What one YAML says the process tree should look like."""

    worker_num: int
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
    """One ``rpc:<index>:<count>:<pid>:<on_main>:<service>`` reply."""

    index: int
    count: int
    pid: int
    on_main: int
    service: str


def load_spec(path: Path) -> Spec:
    """Read worker count and listen endpoints out of a YAML file.

    ``global.processmodel.handle_num: 0`` is left as 0. The running process replaces that with
    the CPU count; callers that need the resolved value use
    :func:`_fallback_worker_num`.

    Args:
        path: Example ``trpc_python.yaml``, or a variant of it.

    Returns:
        The expectations the running process is checked against.
    """
    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    server = data["server"]
    endpoints = tuple(
        Endpoint(name=item["name"], port=int(item["port"]), network=item["network"], protocol=item["protocol"])
        for item in server["service"])
    return Spec(worker_num=int(data["global"]["processmodel"]["handle_num"]),
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
        """OS pid of worker-0, which is the process this object started."""
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
        """Whether worker-0 has already exited."""
        return self.proc is not None and self.proc.poll() is not None

    def wait(self, timeout: float) -> int:
        """Wait until worker-0 exits.

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
        """Send ``sig`` to worker-0.

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
             service_index: Optional[int] = None,
             service_updates: Optional[Dict[str, object]] = None,
             processmodel_updates: Optional[Dict[str, object]] = None) -> Tuple[tempfile.TemporaryDirectory, Path]:
    """Write a copy of the example YAML with ``server`` fields replaced.

    Args:
        updates: Keys merged into the ``server`` mapping.
        service_index: When set with ``service_updates``, which service to edit.
        service_updates: Keys merged into that one service.
        processmodel_updates: Keys merged into ``global.processmodel``.

    Returns:
        The temporary directory (keep it alive) and the YAML path.
    """
    data = yaml.safe_load(_CONF.read_text(encoding="utf-8"))
    data["server"].update(updates)
    data["global"]["processmodel"].update(processmodel_updates or {})
    if service_index is not None and service_updates:
        data["server"]["service"][service_index].update(service_updates)
    directory = tempfile.TemporaryDirectory(prefix="trpc-peer-")
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

    A child that exits before ``ps`` runs is ignored. A worker the server
    actually forked also leaves a ``forked child`` line in the log.

    Args:
        pid: Parent pid. For peer this is worker-0.

    Returns:
        Child pids. Empty when ``pgrep`` sees none.
    """
    proc = subprocess.run(["pgrep", "-P", str(pid)], capture_output=True, text=True, check=False)
    if proc.returncode not in (0, 1):
        raise AssertionError(f"pgrep failed ({proc.returncode}): {proc.stderr}")
    children = []
    for child in proc.stdout.split():
        command = subprocess.run(["ps", "-o", "command=", "-p", child], capture_output=True, text=True, check=False)
        text = command.stdout
        # A tracker (or any child) can exit between pgrep and ps. An empty
        # command is not a worker the server forked.
        if command.returncode != 0 or not text.strip() or "resource_tracker" in text:
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
    index, count, pid, on_main, service = match.groups()
    return RpcReply(int(index), int(count), int(pid), int(on_main), service)


def _hooks(output: str) -> Dict[int, int]:
    """Map worker index to pid from ``HOOK_START`` lines.

    Args:
        output: Server stdout collected so far.

    Returns:
        ``index -> pid``. A repeated index keeps the latest pid, which is
        how a restarted worker would show up.
    """
    table: Dict[int, int] = {}
    for index_s, _count_s, pid_s, _on_main_s in _HOOK_RE.findall(output):
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


def _wait_ready(server: ManagedServer, worker_num: int, port: int, timeout: float = 20) -> Dict[int, int]:
    """Block until every worker has hooked, bound and fired both timers.

    Args:
        server: Running server.
        worker_num: How many ``HOOK_START`` indexes are required.
        port: A tRPC TCP port that must accept once workers are bound.
        timeout: Seconds before giving up.

    Returns:
        ``HOOK_START`` index to pid.

    Raises:
        AssertionError: The process exited, or the deadline passed.
    """
    needle = f"peer: {worker_num} {_BOUND}"
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if server.exited():
            raise AssertionError(f"server exited early ({server.proc.returncode}):\n{server.text()}")
        output = server.text()
        hooks = _hooks(output)
        timers = {int(index) for index, _pid in _TIMER_ALL_RE.findall(output)}
        named = _TIMER_NAMED_RE.findall(output)
        if (needle in output and hooks.keys() == set(range(worker_num)) and timers == set(range(worker_num))
                and len(named) == 1 and _can_connect(port)):
            return hooks
        time.sleep(0.05)
    raise AssertionError(f"server not ready within {timeout:.0f}s:\n{server.text()}")


def _assert_identity(server: ManagedServer, worker_num: int, hooks: Dict[int, int]) -> None:
    """Check that worker-0 is this process and the others are its children.

    Args:
        server: Ready server.
        worker_num: Expected ``count`` on every hook line.
        hooks: ``HOOK_START`` table.

    Raises:
        AssertionError: A pid, a count, or ``on_main`` disagrees.
    """
    output = server.text()
    rows = [(int(index), int(count), int(pid), int(on_main))
            for index, count, pid, on_main in _HOOK_RE.findall(output)]
    if len(rows) != worker_num:
        raise AssertionError(f"HOOK_START lines {rows}, want {worker_num}")
    if {count for _index, count, _pid, _on_main in rows} != {worker_num}:
        raise AssertionError(f"HOOK_START count is not {worker_num}: {rows}")
    if {on_main for _index, _count, _pid, on_main in rows} != {1}:
        raise AssertionError(f"HOOK_START on_main is not 1: {rows}")
    if hooks[0] != server.pid:
        raise AssertionError(f"worker-0 pid {hooks[0]} is not the started process {server.pid}")
    children = set(_child_pids(server.pid))
    want = {pid for index, pid in hooks.items() if index != 0}
    if children != want:
        raise AssertionError(f"children {children}, want the non-zero workers {want}")


def _assert_startup_warnings(output: str, worker_num: int) -> None:
    """Check the fixed startup line, the timer warning, and the macOS warning.

    Args:
        output: Server stdout.
        worker_num: Number interpolated into the startup line. Greater than
            1, so the unnamed-timer warning is required.
    """
    startup = (f"peer mode: {worker_num} workers, all endpoints use SO_REUSEPORT; {_SECOND_INSTANCE}")
    if startup not in output:
        raise AssertionError(f"log missing {startup!r}")
    if _UNNAMED_TIMER not in output:
        raise AssertionError("log missing the unnamed-timer warning")
    if sys.platform == "darwin":
        if _MACOS_BALANCE not in output:
            raise AssertionError("macOS load-balance warning missing")
    elif _MACOS_BALANCE in output:
        raise AssertionError("macOS load-balance warning on a non-macOS host")


def _assert_timers(output: str, worker_num: int, hooks: Dict[int, int]) -> None:
    """Check timer coverage, and that worker-0's timers start after the bind.

    Children start their timers before setting the ready flag, so their
    lines may precede ``workers bound``. Worker-0 starts its timers only
    after that line (and after ``ready_callbacks``).

    Args:
        output: Server stdout.
        worker_num: How many workers run the unnamed timer.
        hooks: ``HOOK_START`` table the timer pids must match.
    """
    all_timers = [(int(index), int(pid)) for index, pid in _TIMER_ALL_RE.findall(output)]
    if {index for index, _pid in all_timers} != set(range(worker_num)):
        raise AssertionError(f"TIMER_ALL indexes {all_timers}, want {set(range(worker_num))}")
    for index, pid in all_timers:
        if hooks.get(index) != pid:
            raise AssertionError(f"TIMER_ALL {index} pid {pid} is not hook pid {hooks.get(index)}")
    named = [(int(index), int(pid)) for index, pid in _TIMER_NAMED_RE.findall(output)]
    if named != [(0, hooks[0])]:
        raise AssertionError(f"TIMER_NAMED {named}, want one line for index 0 pid {hooks[0]}")

    bound_at = output.find(_BOUND)
    if bound_at < 0:
        raise AssertionError("log has no workers-bound line")
    for match in _HOOK_RE.finditer(output):
        if match.start() > bound_at:
            raise AssertionError(f"HOOK_START after workers bound: {match.group(0)}")
    for match in _TIMER_ALL_RE.finditer(output):
        if int(match.group(2)) == hooks[0] and match.start() < bound_at:
            raise AssertionError("worker-0 TIMER_ALL is before workers bound")
    for match in _TIMER_NAMED_RE.finditer(output):
        if match.start() < bound_at:
            raise AssertionError("TIMER_NAMED is before workers bound")


def _assert_listen(pids: List[int], spec: Spec) -> None:
    """Require one listen socket per endpoint on every worker.

    ``tcp_reuse_port: false`` does not change this: peer always sets
    ``SO_REUSEPORT``, so worker-0 and each child own their own socket.

    Args:
        pids: Every worker pid, worker-0 included.
        spec: Endpoints. ``worker_num`` is how many sockets each port has.
    """
    watched = list(spec.endpoints)
    for endpoint in watched:
        tcp = endpoint.network == "tcp" or endpoint.protocol == "http"
        per_pid = {pid: _socket_fds(pid, endpoint.port, tcp_listen=tcp) for pid in pids}
        if any(len(fds) != 1 for fds in per_pid.values()):
            raise AssertionError(f"{endpoint.protocol} {endpoint.port} sockets {per_pid}, "
                                 f"want one on each of {len(pids)} workers")


def _assert_reply(report: RpcReply, worker_num: int, hooks: Dict[int, int], service: str) -> None:
    """Require one reply to name a real worker on its main thread.

    Args:
        report: Parsed reply.
        worker_num: Expected ``count``.
        hooks: Legal ``index -> pid``.
        service: Listen address's service name.

    Raises:
        AssertionError: The reply does not match this server.
    """
    if report.count != worker_num or report.on_main != 1 or report.service != service:
        raise AssertionError(f"reply {report}, want count {worker_num} on_main 1 service {service}")
    if report.index not in hooks or hooks[report.index] != report.pid:
        raise AssertionError(f"reply {report} is not in {hooks}")


def _assert_sticky(spec: Spec, hooks: Dict[int, int]) -> None:
    """Several unary calls on one TCP connection stay on one worker.

    Args:
        spec: First tRPC TCP endpoint and the worker count.
        hooks: Legal workers.
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
        _assert_reply(report, spec.worker_num, hooks, endpoint.name)
        indexes.append(report.index)
    if len(set(indexes)) != 1:
        raise AssertionError(f"one connection hit indexes {indexes}")


def _assert_address_index(spec: Spec, hooks: Dict[int, int]) -> None:
    """The reply service is the listen address, not the request-head callee.

    Args:
        spec: The two tRPC TCP endpoints.
        hooks: Legal workers.
    """
    first, second = spec.find_all("tcp", "trpc")

    async def _both() -> Tuple[RpcReply, RpcReply]:
        _ret_a, msg_a = await _unary(first.port, first.name, "ping", callee=first.name)
        _ret_b, msg_b = await _unary(second.port, second.name, "ping", callee=first.name)
        return _parse_rpc(msg_a), _parse_rpc(msg_b)

    got_a, got_b = asyncio.run(_both())
    _assert_reply(got_a, spec.worker_num, hooks, first.name)
    _assert_reply(got_b, spec.worker_num, hooks, second.name)


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


class _SharedFrames:
    """One TCP connection whose frames are split by stream id.

    Both client streams read and write this socket, so the kernel has
    only one connection to assign to a worker.
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
    """Two streams on one connection stay on the same worker.

    Args:
        spec: Worker count and the first tRPC TCP endpoint.
    """
    endpoint = spec.find("tcp", "trpc")
    reports = asyncio.run(_open_two_streams(endpoint.port, endpoint.name))
    (index_a, _id_a), (index_b, _id_b) = reports
    if index_a != index_b or index_a not in set(range(spec.worker_num)):
        raise AssertionError(f"streams {reports} are not one worker in {set(range(spec.worker_num))}")


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


def _assert_spread(label: str, indexes: Set[int], worker_num: int) -> None:
    """Require Linux to hit every worker. macOS may hit only one.

    Args:
        label: What was called, for the failure text.
        indexes: Worker indexes that answered.
        worker_num: Legal index range, and the set Linux must fill.
    """
    legal = set(range(worker_num))
    if not indexes or not indexes <= legal:
        raise AssertionError(f"{label} hit {indexes}, legal indexes are {legal}")
    if sys.platform != "darwin" and indexes != legal:
        raise AssertionError(f"{label} hit {indexes}, want every worker {legal}")


def _assert_other_protocols(spec: Spec, hooks: Dict[int, int]) -> None:
    """Call UDP, HTTP and the user protocol and check who answered.

    Args:
        spec: Endpoints and worker count.
        hooks: Legal workers.
    """
    udp = spec.find("udp", "trpc")
    http_ep = spec.find("tcp", "http")
    user = spec.find("udp", "user")
    samples = _SPREAD_SAMPLES if sys.platform != "darwin" else 4

    udp_indexes = set()
    for msg in asyncio.run(_gather_udp(udp.name, udp.port, samples)):
        report = _parse_rpc(msg)
        _assert_reply(report, spec.worker_num, hooks, udp.name)
        udp_indexes.add(report.index)
    _assert_spread("udp", udp_indexes, spec.worker_num)

    http_indexes = set()
    for _ in range(samples):
        report = _parse_rpc(_http_once(http_ep.port))
        _assert_reply(report, spec.worker_num, hooks, http_ep.name)
        http_indexes.add(report.index)
    _assert_spread("http", http_indexes, spec.worker_num)

    user_indexes = set()
    user_pids = set(hooks.values())
    for _ in range(samples):
        text = _user_once(user.port)
        match = _USER_RE.search(text)
        if match is None:
            raise AssertionError(f"user protocol reply {text!r}")
        index, pid = int(match.group(1)), int(match.group(2))
        if index not in hooks or hooks[index] != pid or pid not in user_pids:
            raise AssertionError(f"user reply {text!r} is not in {hooks}")
        user_indexes.add(index)
    _assert_spread("user", user_indexes, spec.worker_num)


def _assert_tcp_spread(spec: Spec, hooks: Dict[int, int]) -> None:
    """Open many TCP connections and require Linux to hit every worker.

    Args:
        spec: First tRPC TCP endpoint.
        hooks: Legal workers.
    """
    endpoint = spec.find("tcp", "trpc")
    samples = _SPREAD_SAMPLES if sys.platform != "darwin" else 4
    replies = asyncio.run(tcp_calls(endpoint.port, endpoint.name, ["ping"] * samples))
    indexes = set()
    for request_id, ret, msg in replies:
        if ret != 0:
            raise AssertionError(f"tcp spread ret {ret}: {msg}")
        report = _parse_rpc(msg)
        _assert_reply(report, spec.worker_num, hooks, endpoint.name)
        indexes.add(report.index)
        del request_id
    _assert_spread("tcp", indexes, spec.worker_num)


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


def _assert_exit_order(output: str, code: int, hooks: Dict[int, int]) -> None:
    """Require a clean exit and the design's callback order.

    Other workers print ``WORKER_EXIT`` and then ``HOOK_STOP`` before
    worker-0 prints ``MASTER_EXIT``. Worker-0 then prints its own
    ``WORKER_EXIT`` and, after that, its ``HOOK_STOP``.

    Args:
        output: Full server output.
        code: Process exit code.
        hooks: Worker index to pid.
    """
    if code != 0:
        raise AssertionError(f"server exit {code}, want 0:\n{output}")
    masters = [int(pid) for pid in _MASTER_RE.findall(output)]
    if masters != [hooks[0]]:
        raise AssertionError(f"MASTER_EXIT {masters}, want [{hooks[0]}]")
    master_at = output.find(f"MASTER_EXIT {hooks[0]}")
    for index, pid in hooks.items():
        worker_token = f"WORKER_EXIT {pid}"
        stop_token = f"HOOK_STOP {index} {pid}"
        if output.count(worker_token) != 1 or output.count(stop_token) != 1:
            raise AssertionError(f"expected one {worker_token} and one {stop_token}:\n{output}")
        worker_at = output.find(worker_token)
        stop_at = output.find(stop_token)
        if index == 0:
            if not master_at < worker_at < stop_at:
                raise AssertionError("worker-0 exit order is not MASTER_EXIT, WORKER_EXIT, HOOK_STOP")
        elif not worker_at < stop_at < master_at:
            raise AssertionError(f"worker {index} exit order is not WORKER_EXIT, HOOK_STOP, then MASTER_EXIT")


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
        _request_id, ret, msg = await read_frame(reader, timeout=5)
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
    """One clean start: identity, sockets, traffic, timers, then a clean stop."""
    server = ManagedServer(_CONF)
    port = spec.find("tcp", "trpc").port

    def body() -> None:
        hooks = _wait_ready(server, spec.worker_num, port)
        _assert_identity(server, spec.worker_num, hooks)
        _assert_startup_warnings(server.text(), spec.worker_num)
        _assert_timers(server.text(), spec.worker_num, hooks)
        _assert_listen(list(hooks.values()), spec)
        _ensure_client(_CONF)
        _assert_sticky(spec, hooks)
        _assert_address_index(spec, hooks)
        _assert_streams(spec)
        _assert_tcp_spread(spec, hooks)
        _assert_other_protocols(spec, hooks)
        if _STOP_RE.search(server.text()) or _WORKER_RE.search(server.text()):
            raise AssertionError("stop hooks ran before shutdown")
        code = server.stop()
        _assert_exit_order(server.text(), code, hooks)
        # Design 9.1: after the loop stops, release HTTP runners and UDP.
        # The release uses run_until_complete on the loop _leave_loop already
        # stopped, so this is reported rather than treated as success.
        if "Event loop stopped before Future completed" in server.text():
            raise AssertionError("worker-0 _on_graceful_exit failed to release HTTP/UDP listeners: "
                                 "Event loop stopped before Future completed")

    _run(server, body)


def _slow_hook(spec: Spec) -> None:
    """``SLOW_HOOK=1`` keeps ``workers bound`` waiting, and the line follows both hooks."""
    server = ManagedServer(_CONF, env={"SLOW_HOOK": "1"})
    port = spec.find("tcp", "trpc").port

    def body() -> None:
        hooks = _wait_ready(server, spec.worker_num, port, timeout=20)
        output = server.text()
        match = _BOUND_RE.search(output)
        if match is None:
            raise AssertionError(f"no bound timing:\n{output}")
        elapsed_ms = int(match.group(2))
        if elapsed_ms < 4000:
            raise AssertionError(f"workers bound in {elapsed_ms} ms, want at least 4000")
        if int(match.group(1)) != spec.worker_num:
            raise AssertionError(f"bound count {match.group(1)}, want {spec.worker_num}")
        bound_at = output.find(_BOUND)
        hook_count = 0
        for found in _HOOK_RE.finditer(output):
            hook_count += 1
            if found.start() > bound_at:
                raise AssertionError(f"{found.group(0)} is after workers bound")
        if hook_count < spec.worker_num:
            raise AssertionError(f"only {hook_count} HOOK_START lines before workers bound")
        del hooks
        server.stop()

    _run(server, body)


def _fail_hook(index: str) -> None:
    """A raising ``on_start`` exits 1, does not bind, and leaves no child."""
    server = ManagedServer(_CONF, env={"FAIL_HOOK": index})
    seen: Set[int] = set()

    def body() -> None:
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            if _alive(server.pid):
                seen.update(_child_pids(server.pid))
            if server.exited():
                break
            time.sleep(0.05)
        code = server.wait(10)
        if code != 1:
            raise AssertionError(f"FAIL_HOOK={index} exit {code}, want 1:\n{server.text()}")
        if _BOUND in server.text():
            raise AssertionError(f"FAIL_HOOK={index} still bound:\n{server.text()}")
        if "hook failed on purpose" not in server.text():
            raise AssertionError(f"FAIL_HOOK={index} log missing the raise:\n{server.text()}")
        _wait_dead(list(seen), 10)

    _run(server, body)


def _ready_timeout() -> None:
    """``ready_timeout: 1000`` plus a slow hook exits 1 without binding."""
    directory, path = _variant({"ready_timeout": 1000})
    server = ManagedServer(path, env={"SLOW_HOOK": "1"})
    try:

        def body() -> None:
            began = time.monotonic()
            code = server.wait(15)
            elapsed = time.monotonic() - began
            if code != 1:
                raise AssertionError(f"ready_timeout exit {code}, want 1:\n{server.text()}")
            if "not ready within ready_timeout" not in server.text() or _BOUND in server.text():
                raise AssertionError(f"ready_timeout log is wrong:\n{server.text()}")
            if elapsed >= 15:
                raise AssertionError(f"ready_timeout took {elapsed:.1f}s")

        _run(server, body)
    finally:
        directory.cleanup()


def _sigterm_inflight(spec: Spec) -> None:
    """An in-flight request finishes inside ``close_wait_time``, then exit 0."""
    server = ManagedServer(_CONF)
    endpoint = spec.find("tcp", "trpc")

    def body() -> None:
        hooks = _wait_ready(server, spec.worker_num, endpoint.port)

        def _signal() -> None:
            server.signal(signal.SIGTERM)

        msg = asyncio.run(_inflight(endpoint.port, endpoint.name, _signal))
        report = _parse_rpc(msg)
        _assert_reply(report, spec.worker_num, hooks, endpoint.name)
        code = server.wait(15)
        _assert_exit_order(server.text(), code, hooks)

    _run(server, body)


def _double_sigterm(spec: Spec) -> None:
    """A second SIGTERM cuts a long drain short. Both workers still stop."""
    directory, path = _variant({"close_wait_time": 30000})
    server = ManagedServer(path)
    try:

        def body() -> None:
            endpoint = spec.find("tcp", "trpc")
            hooks = _wait_ready(server, spec.worker_num, endpoint.port)
            began = time.monotonic()
            server.signal(signal.SIGTERM)
            time.sleep(0.5)
            server.signal(signal.SIGTERM)
            code = server.wait(15)
            elapsed = time.monotonic() - began
            if elapsed >= 15:
                raise AssertionError(f"second SIGTERM did not cut the drain ({elapsed:.1f}s)")
            _assert_exit_order(server.text(), code, hooks)

        _run(server, body)
    finally:
        directory.cleanup()


def _kill_group(spec: Spec) -> None:
    """SIGTERM to the process group still stops every worker through worker-0."""
    server = ManagedServer(_CONF, new_session=True)
    endpoint = spec.find("tcp", "trpc")

    def body() -> None:
        hooks = _wait_ready(server, spec.worker_num, endpoint.port)
        os.killpg(server.pid, signal.SIGTERM)
        code = server.wait(15)
        _assert_exit_order(server.text(), code, hooks)

    _run(server, body)


def _kill_child(spec: Spec) -> None:
    """SIGKILL of one child exits 1. ``max_restarts`` and ``reboot_child`` do not replace it."""
    directory, path = _variant({"reboot_child": True, "max_restarts": 3})
    server = ManagedServer(path)
    try:

        def body() -> None:
            endpoint = spec.find("tcp", "trpc")
            hooks = _wait_ready(server, spec.worker_num, endpoint.port)
            victim = hooks[1]
            os.kill(victim, signal.SIGKILL)
            code = server.wait(15)
            output = server.text()
            if code != 1:
                raise AssertionError(f"dead worker exit {code}, want 1:\n{output}")
            if "treated as false" not in output:
                raise AssertionError(f"reboot_child warning missing:\n{output}")
            if len(_HOOK_RE.findall(output)) != spec.worker_num:
                raise AssertionError(f"HOOK_START changed after the kill, a worker was replaced:\n{output}")

        _run(server, body)
    finally:
        directory.cleanup()


def _kill_worker_zero(spec: Spec) -> None:
    """SIGKILL of worker-0 is followed by the other workers exiting."""
    server = ManagedServer(_CONF)
    endpoint = spec.find("tcp", "trpc")

    def body() -> None:
        hooks = _wait_ready(server, spec.worker_num, endpoint.port)
        children = [pid for index, pid in hooks.items() if index != 0]
        if set(children) != set(_child_pids(server.pid)):
            raise AssertionError(f"children {_child_pids(server.pid)} are not {children}")
        try:
            os.kill(server.pid, signal.SIGKILL)
            _wait_dead(children, 10)
            server.wait(5)
        finally:
            for pid in children:
                if _alive(pid):
                    try:
                        os.kill(pid, signal.SIGKILL)
                    except ProcessLookupError:
                        pass

    _run(server, body)


def _reject_before_fork(env: Optional[Dict[str, str]], updates: Dict[str, object], needle: str,
                        service_updates: Optional[Dict[str, object]] = None) -> None:
    """Start a server that must fail with ``needle`` and must not fork.

    Args:
        env: Extra environment for ``server.py``.
        updates: YAML ``server`` replacements. Empty keeps the example file
            when ``service_updates`` is also empty; a copy is still written
            when a service field changes.
        needle: Text the log must contain.
        service_updates: Applied to service 0 when set.
    """
    directory: Optional[tempfile.TemporaryDirectory] = None
    conf = _CONF
    if updates or service_updates:
        directory, conf = _variant(updates, 0 if service_updates else None, service_updates)
    server = ManagedServer(conf, env=env)
    seen: Set[int] = set()
    try:

        def body() -> None:
            deadline = time.monotonic() + 15
            while time.monotonic() < deadline:
                if server.proc is not None and _alive(server.pid):
                    seen.update(_child_pids(server.pid))
                if server.exited():
                    break
                time.sleep(0.05)
            code = server.wait(10)
            output = server.text()
            if code == 0:
                raise AssertionError(f"exit 0, want failure ({needle}):\n{output}")
            if needle not in output:
                raise AssertionError(f"log missing {needle!r}:\n{output}")
            if _BOUND in output:
                raise AssertionError(f"bound despite {needle!r}:\n{output}")
            if seen or "forked child" in output:
                raise AssertionError(f"forked before {needle!r}: children {seen}\n{output}")

        _run(server, body)
    finally:
        if directory is not None:
            directory.cleanup()


def _worker_num_one(spec: Spec) -> None:
    """``handle_num: 1`` warns, serves as the only worker, and forks nobody."""
    directory, path = _variant({}, processmodel_updates={"handle_num": 1})
    server = ManagedServer(path)
    try:

        def body() -> None:
            port = spec.find("tcp", "trpc").port
            hooks = _wait_ready(server, 1, port)
            output = server.text()
            if "merge mode with a supervisor" not in output:
                raise AssertionError(f"worker_num=1 warning missing:\n{output}")
            if _UNNAMED_TIMER in output:
                raise AssertionError(f"unnamed-timer warning with a single worker:\n{output}")
            if _MACOS_BALANCE in output:
                raise AssertionError(f"macOS balance warning with a single worker:\n{output}")
            _assert_identity(server, 1, hooks)
            _assert_listen([hooks[0]], load_spec(path))
            _ensure_client(_CONF)
            ret, msg = asyncio.run(_unary(port, spec.find("tcp", "trpc").name, "ping"))
            if ret != 0:
                raise AssertionError(f"worker_num=1 ret {ret}: {msg}")
            report = _parse_rpc(msg)
            _assert_reply(report, 1, hooks, spec.find("tcp", "trpc").name)
            if report.index != 0:
                raise AssertionError(f"worker_num=1 reply index {report.index}")
            code = server.stop()
            _assert_exit_order(server.text(), code, hooks)

        _run(server, body)
    finally:
        directory.cleanup()


def _worker_num_zero() -> None:
    """``handle_num: 0`` starts one worker per CPU, then stops.

    The zero itself is not warned about. On macOS the balance warning still
    names the resolved count when that count is greater than 1.
    """
    expect = _fallback_worker_num()
    directory, path = _variant({}, processmodel_updates={"handle_num": 0})
    server = ManagedServer(path)
    try:

        def body() -> None:
            port = load_spec(path).find("tcp", "trpc").port
            hooks = _wait_ready(server, expect, port, timeout=40)
            output = server.text()
            warned = [
                line for line in output.splitlines()
                if "processmodel.handle_num" in line and "on macOS:" not in line
            ]
            if warned:
                raise AssertionError(f"worker_num=0 should not warn:\n{output}")
            match = _BOUND_RE.search(output)
            if match is None or int(match.group(1)) != expect:
                raise AssertionError(f"bound line is not {expect} workers:\n{output}")
            if hooks.keys() != set(range(expect)):
                raise AssertionError(f"hooks {hooks}, want {expect} workers")
            code = server.stop(timeout=40)
            if code != 0:
                raise AssertionError(f"worker_num=0 exit {code}:\n{server.text()}")

        _run(server, body)
    finally:
        directory.cleanup()


def _ignored_io_wait(spec: Spec) -> None:
    """``io_wait_count`` is warned about and does not block startup."""
    server = ManagedServer(_CONF, env={"IO_WAIT": "1"})
    port = spec.find("tcp", "trpc").port

    def body() -> None:
        began = time.monotonic()
        _wait_ready(server, spec.worker_num, port, timeout=15)
        if time.monotonic() - began >= 15:
            raise AssertionError("io_wait_count blocked startup")
        if "io_wait_count=1 ignored" not in server.text():
            raise AssertionError(f"io_wait_count warning missing:\n{server.text()}")
        server.stop()

    _run(server, body)


def _ignored_scheduler(spec: Spec) -> None:
    """A dispatcher scheduler is warned about and does not pin connections."""
    server = ManagedServer(_CONF, env={"SCHEDULER": "1"})
    endpoint = spec.find("tcp", "trpc")

    def body() -> None:
        hooks = _wait_ready(server, spec.worker_num, endpoint.port)
        if "only applies to separate mode" not in server.text():
            raise AssertionError(f"scheduler warning missing:\n{server.text()}")
        _ensure_client(_CONF)
        _assert_tcp_spread(spec, hooks)
        server.stop()

    _run(server, body)


def _sync_handler(spec: Spec) -> None:
    """A plain ``def`` handler is warned about. ``SayHello`` still answers."""
    server = ManagedServer(_CONF, env={"SYNC": "1"})
    endpoint = spec.find("tcp", "trpc")

    def body() -> None:
        hooks = _wait_ready(server, spec.worker_num, endpoint.port)
        if "is not 'async def'" not in server.text():
            raise AssertionError(f"sync-handler warning missing:\n{server.text()}")
        _ensure_client(_CONF)
        ret, msg = asyncio.run(_unary(endpoint.port, endpoint.name, "ping"))
        if ret != 0:
            raise AssertionError(f"sync-mode SayHello ret {ret}: {msg}")
        _assert_reply(_parse_rpc(msg), spec.worker_num, hooks, endpoint.name)
        server.stop()

    _run(server, body)


def _second_instance(spec: Spec) -> None:
    """A second process on the same ports binds instead of failing."""
    first = ManagedServer(_CONF)
    second = ManagedServer(_CONF)
    port = spec.find("tcp", "trpc").port
    try:
        first.start()
        _wait_ready(first, spec.worker_num, port)
        second.start()
        _wait_ready(second, spec.worker_num, port)
        if _SECOND_INSTANCE not in second.text():
            raise AssertionError(f"second instance missing the startup warning:\n{second.text()}")
        code2 = second.stop()
        code1 = first.stop()
        if code1 != 0 or code2 != 0:
            raise AssertionError(f"second-instance exits {code1}, {code2}")
    except Exception:
        sys.stderr.write("---- first ----\n")
        sys.stderr.write(first.text())
        sys.stderr.write("---- second ----\n")
        sys.stderr.write(second.text())
        raise
    finally:
        if second.proc is not None and not second.exited():
            second.stop()
        if first.proc is not None and not first.exited():
            first.stop()


def main() -> None:
    """Run every peer-mode check against the example YAML.

    A failed check is recorded and the rest still run, so one mismatch
    does not hide the others. The process exits 1 when any check failed.

    Raises:
        SystemExit: One or more checks failed. Each check already wrote
            its server log to stderr.
    """
    parser = argparse.ArgumentParser(description="Check the peer-mode example")
    parser.parse_args()
    spec = load_spec(_CONF)
    failures: List[str] = []

    def step(name: str, func: Callable[[], None]) -> None:
        try:
            func()
        except Exception as err:  # pylint: disable=broad-except
            failures.append(name)
            print(f"FAIL peer {name}: {err.__class__.__name__}: {err}", flush=True)
            return
        print(f"ok peer {name}", flush=True)

    step("happy", lambda: _happy(spec))
    step("slow hook", lambda: _slow_hook(spec))
    step("fail hook 0", lambda: _fail_hook("0"))
    step("fail hook 1", lambda: _fail_hook("1"))
    step("ready timeout", _ready_timeout)
    step("sigterm inflight", lambda: _sigterm_inflight(spec))
    step("double sigterm", lambda: _double_sigterm(spec))
    step("process group", lambda: _kill_group(spec))
    step("kill child", lambda: _kill_child(spec))
    step("kill worker-0", lambda: _kill_worker_zero(spec))
    step("reject task", lambda: _reject_before_fork({"TASK": "1"}, {}, "peer mode does not support Task"))
    step("reject unix", lambda: _reject_before_fork(None, {}, "unsupported network", {"network": "unix"}))
    step("worker_num 1", lambda: _worker_num_one(spec))
    step("worker_num 0", _worker_num_zero)
    step("io_wait ignored", lambda: _ignored_io_wait(spec))
    step("scheduler ignored", lambda: _ignored_scheduler(spec))
    step("sync handler", lambda: _sync_handler(spec))
    step("second instance", lambda: _second_instance(spec))
    if failures:
        print("failed: " + ", ".join(failures), flush=True)
        raise SystemExit(1)


if __name__ == "__main__":
    main()
