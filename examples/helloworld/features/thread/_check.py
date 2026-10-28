# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Start one example server and check the thread model it claims to be.

Hooks, reply identities and ``lsof`` are checked against the YAML:

* serving-thread count, names and idents come from ``HOOK_START``;
* every reply's ``(index, ident, name)`` must match that table;
* TCP and HTTP each have one LISTEN socket;
* UDP socket count equals ``io_thread_num`` (the loops that bind the
  built-in ``udp`` + ``trpc`` port), not the handle-thread count. macOS
  ``SO_REUSEPORT`` does not have to deliver a datagram to every socket, so
  that UDP is not required to hit every thread;
* a ``udp`` + ``user`` service, when present, gets one raw datagram. The
  reply must be ``user:0:<pid>`` of this process. It is not a ``SayHello``.
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
import threading
import time
from dataclasses import dataclass
from pathlib import Path
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
from trpc_sdk.client import Options  # noqa: E402
from trpc_sdk.config import TrpcConfig  # noqa: E402
from trpc_sdk.plugin import init_plugins  # noqa: E402
from trpc_sdk.serialization import SerializationType  # noqa: E402

from features.thread._frame import tcp_calls  # noqa: E402
from features.thread._frame import tcp_pipelined  # noqa: E402
from features.thread._servicer import ThreadReport  # noqa: E402
from features.thread._servicer import parse_thread_report  # noqa: E402
from trpc_trpc_python_group_helloworld import pb  # noqa: E402
from trpc_trpc_python_group_helloworld import rpc  # noqa: E402

_MAIN_RE = re.compile(r"MAIN_IDENT (\d+) (\S+)")
_HOOK_RE = re.compile(r"HOOK_START (\d+) (\d+) ([01]) (\S+) (\d+)")
_STOP_RE = re.compile(r"HOOK_STOP (\d+)")
_USER_RE = re.compile(r"user:(-?\d+):(\d+)")
_NOISE = ("Event loop is closed", "Task was destroyed", "Traceback")
_CLIENT_READY = False


@dataclass(frozen=True)
class Endpoint:
    """One YAML ``server.service[]`` entry the checker calls."""

    name: str
    port: int
    network: str
    protocol: str


@dataclass(frozen=True)
class Spec:
    """What the YAML says the process should look like once it is listening."""

    io_thread_num: int
    handle_thread_num: int
    endpoints: Tuple[Endpoint, ...]

    @property
    def separate(self) -> bool:
        """True for the separate thread model, ``handle_thread_num > 0``."""
        return self.handle_thread_num > 0

    @property
    def serving(self) -> int:
        """Threads that run handlers. Handle threads under separate, else I/O threads."""
        if self.separate:
            return self.handle_thread_num
        return self.io_thread_num

    @property
    def thread_names(self) -> Set[str]:
        """OS names of every serving thread."""
        prefix = "trpc-handle" if self.separate else "trpc-loop"
        return {f"{prefix}-{index}" for index in range(self.serving)}

    @property
    def io_ready_line(self) -> Optional[str]:
        """Log fragment for the extra I/O threads, or None when there are none.

        The main thread already counts as one I/O loop, so the worker logs
        ``io_thread_num - 1``. Merge has no extra I/O threads.
        """
        if not self.separate:
            return None
        extra = self.io_thread_num - 1
        if extra <= 0:
            return None
        return f"{extra} I/O thread(s) ready"

    def find(self, network: str, protocol: str) -> Optional[Endpoint]:
        """Return the endpoint with this network and protocol, if configured.

        Args:
            network: ``tcp`` or ``udp``.
            protocol: Protocol name from the YAML, such as ``trpc``, ``http``
                or ``user``.

        Returns:
            The matching endpoint, or None.
        """
        for endpoint in self.endpoints:
            if endpoint.network == network and endpoint.protocol == protocol:
                return endpoint
        return None

    @property
    def listen_ports(self) -> List[int]:
        """Stream ports. Each one is a single accept socket on the main thread."""
        return [endpoint.port for endpoint in self.endpoints if endpoint.network != "udp"]

    @property
    def udp_socket_count(self) -> Optional[int]:
        """UDP sockets bound for the built-in UDP port, or None when it is absent.

        Both models bind one datagram socket per I/O loop for ``udp`` +
        ``trpc``. Separate does not bind that port on handle threads, so this
        is ``io_thread_num``, not ``serving``. A ``udp`` + ``user`` service is
        another port and is not counted here: its starter binds once.
        """
        if self.find("udp", "trpc") is None:
            return None
        return self.io_thread_num


def load_spec(path: Path) -> Spec:
    """Read the thread model and the listen endpoints out of a YAML file.

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
    model = data["global"]["processmodel"]
    io_num = int(model.get("io_num") or 0)
    handle_num = int(model.get("handle_num") or 0)
    if model.get("io_handle_type") == "separate":
        return Spec(io_thread_num=io_num or 1, handle_thread_num=handle_num, endpoints=endpoints)
    # Merged I/O and handle threads: one count, handle_num winning a disagreement.
    return Spec(io_thread_num=handle_num or io_num or 1, handle_thread_num=0, endpoints=endpoints)


class ManagedServer:
    """One ``server.py`` subprocess, with its stdout collected while it runs."""

    def __init__(self, script: Path, conf: Path) -> None:
        self.script = script
        self.conf = conf
        self.output = ""
        self.returncode: Optional[int] = None
        self.proc: Optional[subprocess.Popen] = None
        self._reader: Optional[threading.Thread] = None
        self._lock = threading.Lock()

    @property
    def pid(self) -> int:
        """OS pid of the server process."""
        if self.proc is None:
            raise RuntimeError("server is not started")
        return self.proc.pid

    def start(self) -> None:
        """Spawn ``server.py``. ``PYTHONUNBUFFERED`` so hooks and logs show up at once."""
        env = os.environ.copy()
        env["PYTHONUNBUFFERED"] = "1"
        self.proc = subprocess.Popen(  # pylint: disable=consider-using-with
            [sys.executable, "-u", str(self.script), "--conf", str(self.conf)],
            cwd=str(_ROOT),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            env=env,
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

    def wait_ready(self, spec: Spec, timeout: float = 20.0) -> None:
        """Block until hooks, the listen sockets and (for separate) the I/O line are up.

        Args:
            spec: Expected thread model.
            timeout: Seconds before giving up.

        Raises:
            AssertionError: The process exited, or the deadline passed.
        """
        port = spec.listen_ports[0]
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if self.proc is not None and self.proc.poll() is not None:
                raise AssertionError(f"server exited early ({self.proc.returncode}):\n{self.text()}")
            output = self.text()
            hooks_ready = len(_HOOK_RE.findall(output)) >= spec.serving
            io_line = spec.io_ready_line
            io_ready = io_line is None or io_line in output
            if hooks_ready and io_ready and _can_connect(port) and _listen_matches(self.pid, spec):
                return
            time.sleep(0.1)
        raise AssertionError(f"server not ready within {timeout:.0f}s:\n{self.text()}")

    def stop(self) -> int:
        """SIGTERM the server and wait until the stdout reader finishes.

        Returns:
            Process exit code. ``-1`` when the process was never started.
        """
        if self.proc is None:
            return -1
        if self.returncode is not None:
            return self.returncode
        if self.proc.poll() is None:
            self.proc.send_signal(signal.SIGTERM)
        try:
            self.proc.wait(timeout=15)
        except subprocess.TimeoutExpired:
            self.proc.kill()
            self.proc.wait(timeout=5)
        if self._reader is not None:
            self._reader.join(timeout=5)
        self.returncode = self.proc.returncode if self.proc.returncode is not None else -1
        return self.returncode


def _can_connect(port: int) -> bool:
    """Return True when something accepts TCP on ``127.0.0.1:port``."""
    try:
        with socket.create_connection(("127.0.0.1", port), timeout=0.2):
            return True
    except OSError:
        return False


def _socket_fds(pid: int, port: int, tcp_listen: bool) -> Set[str]:
    """Return the lsof FD column of sockets on ``port`` for ``pid``.

    Args:
        pid: Server pid.
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


def _listen_matches(pid: int, spec: Spec) -> bool:
    """Return True when every listen socket count already matches ``spec``."""
    try:
        for port in spec.listen_ports:
            if len(_socket_fds(pid, port, tcp_listen=True)) != 1:
                return False
        udp = spec.find("udp", "trpc")
        if udp is not None and spec.udp_socket_count is not None:
            if len(_socket_fds(pid, udp.port, tcp_listen=False)) != spec.udp_socket_count:
                return False
    except AssertionError:
        return False
    return True


def _parse_hooks(output: str, spec: Spec) -> Tuple[int, Dict[int, ThreadReport]]:
    """Build the serving-thread table from ``MAIN_IDENT`` and ``HOOK_START`` lines.

    Args:
        output: Server stdout collected so far.
        spec: Expected counts and thread names.

    Returns:
        ``(main_ident, index -> report)``. The report's ``ident`` and ``name``
        are what later replies must repeat.

    Raises:
        AssertionError: A line is missing, duplicated, or disagrees with ``spec``.
    """
    mains = _MAIN_RE.findall(output)
    if len(mains) != 1:
        raise AssertionError(f"expected one MAIN_IDENT line, found {mains!r}")
    main_ident = int(mains[0][0])
    if mains[0][1] != "MainThread":
        raise AssertionError(f"main thread is named {mains[0][1]!r}, want MainThread")

    table: Dict[int, ThreadReport] = {}
    for index_s, count_s, on_main_s, name, ident_s in _HOOK_RE.findall(output):
        report = ThreadReport(int(index_s), int(count_s), int(ident_s), on_main_s == "1", name)
        previous = table.get(report.index)
        if previous is not None and previous != report:
            raise AssertionError(f"HOOK_START {report.index} changed from {previous} to {report}")
        table[report.index] = report

    indexes = set(table)
    if indexes != set(range(spec.serving)):
        raise AssertionError(f"HOOK_START indexes {indexes}, want {set(range(spec.serving))}")
    idents = {report.ident for report in table.values()}
    if len(idents) != spec.serving:
        raise AssertionError(f"serving idents are not unique: {table}")
    if main_ident in idents:
        raise AssertionError(f"main ident {main_ident} is also a serving thread")
    names = {report.name for report in table.values()}
    if names != spec.thread_names:
        raise AssertionError(f"thread names {names}, want {spec.thread_names}")
    for report in table.values():
        if report.count != spec.serving or report.on_main:
            raise AssertionError(f"hook report {report} does not match serving={spec.serving} off the main thread")
        if report.name.startswith("trpc-io-") or report.name == "MainThread":
            raise AssertionError(f"handler thread has an I/O name: {report.name}")
    return main_ident, table


def _assert_report(report: ThreadReport, table: Dict[int, ThreadReport], spec: Spec) -> None:
    """Require ``report`` to be one of the hooked serving threads.

    Args:
        report: Parsed from a reply.
        table: Hook table from :func:`_parse_hooks`.
        spec: Expected serving count.

    Raises:
        AssertionError: The reply names a thread the hooks did not start.
    """
    if report.count != spec.serving or report.on_main or report.index not in table:
        raise AssertionError(f"reply {report} is outside serving threads {spec.thread_names}")
    hooked = table[report.index]
    if report.ident != hooked.ident or report.name != hooked.name:
        raise AssertionError(f"reply {report} does not match hook {hooked}")
    if report.name.startswith("trpc-io-") or report.name == "MainThread":
        raise AssertionError(f"reply ran on {report.name}")


def _assert_no_worker_children(pid: int) -> None:
    """Fail when the server has forked a worker child.

    macOS ``pgrep -a`` does not print the command line (that flag includes
    ancestors). Look the command up with ``ps``. A multiprocessing resource
    tracker is not a worker and is ignored.

    Args:
        pid: Server pid.
    """
    proc = subprocess.run(["pgrep", "-P", str(pid)], capture_output=True, text=True, check=False)
    if proc.returncode not in (0, 1):
        raise AssertionError(f"pgrep failed ({proc.returncode}): {proc.stderr}")
    unexpected = []
    for child in proc.stdout.split():
        command = subprocess.run(["ps", "-o", "command=", "-p", child], capture_output=True, text=True, check=False)
        text = command.stdout.strip() or child
        if "resource_tracker" in text:
            continue
        unexpected.append(f"{child} {text}")
    if unexpected:
        raise AssertionError("unexpected child processes:\n" + "\n".join(unexpected))


def _assert_listen(pid: int, spec: Spec) -> None:
    """Check LISTEN and UDP socket counts.

    Args:
        pid: Server pid.
        spec: Expected counts.
    """
    for port in spec.listen_ports:
        found = _socket_fds(pid, port, tcp_listen=True)
        if len(found) != 1:
            raise AssertionError(f"TCP {port} has {len(found)} LISTEN sockets {found}, want 1")
    udp = spec.find("udp", "trpc")
    if udp is None or spec.udp_socket_count is None:
        return
    found = _socket_fds(pid, udp.port, tcp_listen=False)
    if len(found) != spec.udp_socket_count:
        raise AssertionError(f"UDP {udp.port} has {len(found)} sockets {found}, want {spec.udp_socket_count} "
                             f"(one per I/O loop, io_thread_num={spec.io_thread_num})")


def _ensure_client(conf: Path) -> None:
    """Load the example YAML so the public client has a log plugin."""
    global _CLIENT_READY  # pylint: disable=global-statement
    if _CLIENT_READY:
        return
    TrpcConfig.load(str(conf), "utf-8")
    init_plugins()
    _CLIENT_READY = True


def _proxy_call(service: str, port: int, network: str, protocol: str) -> str:
    """One unary ``SayHello`` through ``GreeterClientProxy``.

    Args:
        service: Callee service name (``fix_rpc_name`` rewrites the method path).
        port: ``127.0.0.1`` port.
        network: ``tcp`` or ``udp``.
        protocol: ``trpc`` or ``http``.

    Returns:
        ``HelloReply.msg``.
    """
    proxy = rpc.GreeterClientProxyImpl(service, fix_rpc_name=True, need_clone_ctx=False)
    req = pb.HelloRequest()
    req.msg = "info:0"
    options = (Options().set_target(f"ip://127.0.0.1:{port}").set_network(network).set_protocol(protocol).
               set_serialization_type(SerializationType.PB).set_namespace("Development").set_timeout(5000))
    reply = proxy.SayHello(TrpcContext(), req, options)
    return reply.msg


def _assert_tcp(spec: Spec, table: Dict[int, ThreadReport]) -> None:
    """Check connection spread (merge) or same-connection spread (separate).

    Args:
        spec: Thread model. No TCP endpoint means this is a no-op.
        table: Hook table replies must match.
    """
    endpoint = spec.find("tcp", "trpc")
    if endpoint is None:
        return
    report = parse_thread_report(_proxy_call(endpoint.name, endpoint.port, "tcp", "trpc"))
    _assert_report(report, table, spec)

    if spec.separate:
        piped = asyncio.run(tcp_pipelined(endpoint.port, endpoint.name, ["info:0.05"] * 30))
        reports = []
        if len(piped) != 30:
            raise AssertionError(f"separate pipeline returned {len(piped)} replies, want 30")
        for ret, msg in piped.values():
            if ret != 0:
                raise AssertionError(f"separate pipeline ret {ret}: {msg}")
            item = parse_thread_report(msg)
            _assert_report(item, table, spec)
            reports.append(item)
        indexes = {item.index for item in reports}
        if len(indexes) < 2:
            raise AssertionError(f"one connection stayed on handle threads {indexes}, want at least 2")
        return

    replies = asyncio.run(tcp_calls(endpoint.port, endpoint.name, ["info:0"] * 9))
    reports = []
    for request_id, ret, msg in replies:
        if ret != 0:
            raise AssertionError(f"tcp connection {request_id} ret {ret}: {msg}")
        item = parse_thread_report(msg)
        _assert_report(item, table, spec)
        reports.append(item)
    indexes = {item.index for item in reports}
    if indexes != set(range(spec.serving)):
        raise AssertionError(f"9 connections hit {indexes}, want every serving thread {set(range(spec.serving))}")

    pinned = asyncio.run(tcp_pipelined(endpoint.port, endpoint.name, ["info:0.05"] * 10))
    if len(pinned) != 10:
        raise AssertionError(f"merge pipeline returned {len(pinned)} replies, want 10")
    pinned_reports = []
    for ret, msg in pinned.values():
        if ret != 0:
            raise AssertionError(f"merge pipeline ret {ret}: {msg}")
        item = parse_thread_report(msg)
        _assert_report(item, table, spec)
        pinned_reports.append(item)
    if len({item.index for item in pinned_reports}) != 1 or len({item.ident for item in pinned_reports}) != 1:
        raise AssertionError(f"one merge connection ran on {pinned_reports}")


def _user_once(port: int) -> str:
    """Send one datagram to the user protocol and read the reply.

    Not a ``GreeterClientProxy`` call. Any payload gets the same reply.

    Args:
        port: UDP port of the ``protocol: user`` service.

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


def _assert_user(pid: int, spec: Spec) -> None:
    """One datagram to ``udp`` + ``user`` must be answered by this process as index 0.

    Built-in UDP stays on :func:`_assert_udp`, which only looks up
    ``find("udp", "trpc")``.

    Args:
        pid: Server process pid. The reply must name this process.
        spec: Endpoints. No ``udp`` + ``user`` service means this is a no-op.
    """
    endpoint = spec.find("udp", "user")
    if endpoint is None:
        return
    text = _user_once(endpoint.port)
    match = _USER_RE.fullmatch(text)
    if match is None or int(match.group(1)) != 0 or int(match.group(2)) != pid:
        raise AssertionError(f"user protocol reply {text!r}, want user:0:{pid}")


def _assert_udp(spec: Spec, table: Dict[int, ThreadReport]) -> None:
    """Six public-client UDP calls must land on hooked serving threads.

    Only the built-in ``udp`` + ``trpc`` endpoint. ``protocol: user`` is
    checked by :func:`_assert_user` and is not a ``SayHello``.

    Args:
        spec: Thread model. No built-in UDP endpoint means this is a no-op.
        table: Hook table replies must match.
    """
    endpoint = spec.find("udp", "trpc")
    if endpoint is None:
        return
    for _ in range(6):
        report = parse_thread_report(_proxy_call(endpoint.name, endpoint.port, "udp", "trpc"))
        _assert_report(report, table, spec)


def _http_get(port: int, conn: http.client.HTTPConnection) -> ThreadReport:
    """GET ``/thread_info`` on an existing connection.

    Args:
        port: Unused. The connection already targets the port. Kept so callers
            read as a request to that server.
        conn: Open HTTP/1.1 connection.

    Returns:
        The serving thread that handled the request.
    """
    del port
    conn.request("GET", "/thread_info")
    resp = conn.getresponse()
    body = resp.read()
    if resp.status != 200:
        raise AssertionError(f"/thread_info status {resp.status}: {body!r}")
    return parse_thread_report(body.decode())


def _assert_http(spec: Spec, table: Dict[int, ThreadReport]) -> None:
    """Check HTTP RPC, several connections, and one keep-alive connection.

    HTTP is dispatched per connection, so requests that share a connection
    share a serving thread in both models.

    Args:
        spec: Thread model. No HTTP endpoint means this is a no-op.
        table: Hook table replies must match.
    """
    endpoint = spec.find("tcp", "http")
    if endpoint is None:
        return
    report = parse_thread_report(_proxy_call(endpoint.name, endpoint.port, "tcp", "http"))
    _assert_report(report, table, spec)

    for _ in range(6):
        conn = http.client.HTTPConnection("127.0.0.1", endpoint.port, timeout=10)
        try:
            _assert_report(_http_get(endpoint.port, conn), table, spec)
        finally:
            conn.close()

    conn = http.client.HTTPConnection("127.0.0.1", endpoint.port, timeout=10)
    try:
        first = _http_get(endpoint.port, conn)
        local = conn.sock.getsockname() if conn.sock is not None else None
        reports = [first]
        for _ in range(4):
            if conn.sock is None or conn.sock.getsockname() != local:
                raise AssertionError("keep-alive connection was replaced")
            reports.append(_http_get(endpoint.port, conn))
    finally:
        conn.close()
    for item in reports:
        _assert_report(item, table, spec)
    if len({item.index for item in reports}) != 1 or len({item.ident for item in reports}) != 1:
        raise AssertionError(f"one HTTP connection ran on {reports}")


def _assert_shutdown(output: str, code: int, spec: Spec) -> None:
    """Require a clean exit and one ``HOOK_STOP`` per serving thread.

    Args:
        output: Full server output, including the drain.
        code: Process exit code.
        spec: Expected serving threads. Merge must not log an I/O-thread line.
    """
    if code != 0:
        raise AssertionError(f"server exit {code}, want 0")
    for noise in _NOISE:
        if noise in output:
            raise AssertionError(f"server log contains {noise!r}")
    stops = {int(index) for index in _STOP_RE.findall(output)}
    if stops != set(range(spec.serving)):
        raise AssertionError(f"HOOK_STOP indexes {stops}, want {set(range(spec.serving))}")
    io_line = spec.io_ready_line
    if io_line is None:
        if "I/O thread(s) ready" in output:
            raise AssertionError("merge log contains an I/O thread ready line")
    elif io_line not in output:
        raise AssertionError(f"separate log missing {io_line!r}")


def run(conf: Path) -> None:
    """Launch the server next to ``conf`` and run every check that applies.

    ``conf`` lives in a ``conf/`` directory beside ``server.py``.

    Args:
        conf: Absolute or relative path to the example YAML.

    Raises:
        AssertionError: A thread, socket or reply check failed. The server
            output is printed first.
    """
    conf = conf.resolve()
    script = conf.parent.parent / "server.py"
    spec = load_spec(conf)
    server = ManagedServer(script, conf)
    try:
        server.start()
        server.wait_ready(spec)
        _main_ident, table = _parse_hooks(server.text(), spec)
        del _main_ident
        _assert_no_worker_children(server.pid)
        _assert_listen(server.pid, spec)
        _ensure_client(conf)
        _assert_tcp(spec, table)
        _assert_udp(spec, table)
        _assert_user(server.pid, spec)
        _assert_http(spec, table)
        code = server.stop()
        _assert_shutdown(server.text(), code, spec)
    except Exception:
        sys.stderr.write(server.text())
        server.stop()
        raise
    print(f"ok {'separate' if spec.separate else 'merge'} serving={spec.serving} io_thread_num={spec.io_thread_num} {conf.name}")


def main(default_conf: Path) -> None:
    """Parse ``--conf`` and :func:`run` it.

    Args:
        default_conf: YAML used when ``--conf`` is omitted.
    """
    parser = argparse.ArgumentParser(description="Check one single-process thread-model example")
    parser.add_argument("--conf", default=str(default_conf), help="path to trpc_python.yaml")
    args = parser.parse_args()
    run(Path(args.conf))
