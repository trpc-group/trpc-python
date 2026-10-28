# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Run the three stream shapes against each hellostream process layout.

Starts one server at a time, calls it, then signals the whole process group
so multi-process children do not keep the port. Layouts that bind one Unix
socket also get a unary call and one server stream on that socket. Peer does
not listen there: each worker would unlink the socket file. A failure prints
the tail of that server's log and continues with the remaining layouts.
"""

from __future__ import annotations

import asyncio
import os
import signal
import socket
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import List
from typing import Optional
from typing import Sequence
from typing import Tuple

from trpc_sdk.abc import TrpcContext
from trpc_sdk.client import Options
from trpc_sdk.serialization import SerializationType

from examples.hellostream.test_service_rpc import run_shapes
from trpc_py_stream_helloworld import pb
from trpc_py_stream_helloworld import rpc

_ROOT = Path(__file__).resolve().parents[2]
_HERE = Path(__file__).resolve().parent
_READY_TIMEOUT_S = 15
# YAML close_wait_time is 2000 ms. Leave extra time for the children to exit.
_STOP_TIMEOUT_S = 7
_LOG_TAIL_LINES = 80
_UNIX_SERVICE = "trpc.py_stream.helloworld.UnixGreeter"
# SayHelloRsp sleeps one second between three frames.
_CALL_TIMEOUT_MS = 15000

# name, conf, tcp port, unix path. Peer has no unix path.
_MODES: Sequence[Tuple[str, Path, int, Optional[str]]] = (
    ("merge", _HERE / "trpc_python.yaml", 8000, "/tmp/hellostream-merge.sock"),
    ("single_process_merge", _HERE / "conf" / "single_process_merge.yaml", 8010,
     "/tmp/hellostream-threads.sock"),
    ("single_process_separate", _HERE / "conf" / "single_process_separate.yaml", 8020,
     "/tmp/hellostream-separate-thread.sock"),
    ("multi_process_separate", _HERE / "conf" / "multi_process_separate.yaml", 8030,
     "/tmp/hellostream-separate.sock"),
    ("multi_process_merge", _HERE / "conf" / "multi_process_merge.yaml", 8040, None),
)


def _wait_ready(port: int, proc: subprocess.Popen, timeout_s: float) -> bool:
    """Return whether ``port`` accepts a connection before the server exits.

    Args:
        port: TCP port from the layout YAML.
        proc: Server process. A non-zero poll means it already exited.
        timeout_s: How long to keep trying.

    Returns:
        True when a connect succeeds while the process is still alive.
    """
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            return False
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(0.2)
        try:
            sock.connect(("127.0.0.1", port))
        except OSError:
            time.sleep(0.1)
        else:
            return True
        finally:
            sock.close()
    return False


def _wait_unix(path: str, proc: subprocess.Popen, timeout_s: float) -> bool:
    """Return whether ``path`` accepts an ``AF_UNIX`` connection.

    Args:
        path: Filesystem path of the listening Unix socket.
        proc: Server process. A non-zero poll means it already exited.
        timeout_s: How long to keep trying.

    Returns:
        True when a connect succeeds while the process is still alive.
    """
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            return False
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        sock.settimeout(0.2)
        try:
            sock.connect(path)
        except OSError:
            time.sleep(0.1)
        else:
            return True
        finally:
            sock.close()
    return False


def _unix_options(path: str) -> Options:
    """Build call options for one Unix tRPC call.

    ``socket_type == unix`` rewrites the target to ``ip://<path>:0``.
    The path must not contain a colon, or that rewrite splits the address
    in the wrong place.

    Args:
        path: Filesystem path of the listening Unix socket.

    Returns:
        Options for one unary or server-stream call.
    """
    return (Options().set_unix_path(path).set_socket_type("unix").set_network("tcp").set_protocol(
        "trpc").set_serialization_type(SerializationType.PB).set_timeout(_CALL_TIMEOUT_MS))


async def _run_unix(path: str) -> None:
    """Call ``SayHello`` and one ``SayHelloRsp`` on the Unix socket.

    Args:
        path: Filesystem path of the listening Unix socket.

    Raises:
        AssertionError: The unary reply is not the request name, or the
            server stream is not ``rsp: 0``, ``rsp: 1``, ``rsp: 2``.
    """
    proxy = rpc.GreeterClientProxyImpl(_UNIX_SERVICE)
    options = _unix_options(path)
    request = pb.HelloRequest()
    request.name = "unix"
    reply = await proxy.asyncSayHello(TrpcContext(), request, options)
    if reply.message != "unix":
        raise AssertionError(f"SayHello reply {reply.message!r}")

    stream = await proxy.asyncSayHelloRsp(TrpcContext(), request, options)
    if stream is None:
        raise AssertionError("SayHelloRsp returned no stream")
    received = []
    while True:
        try:
            frame = await stream.recv()
        except EOFError:
            break
        received.append(frame.message)
    if received != ["rsp: 0", "rsp: 1", "rsp: 2"]:
        raise AssertionError(f"SayHelloRsp replies {received}")


def _stop(proc: subprocess.Popen) -> None:
    """SIGTERM the server process group, then SIGKILL if it is still alive.

    Args:
        proc: Server started with ``start_new_session=True``. Its pid is the
            process-group id, which includes forked children.
    """
    if proc.poll() is not None:
        return
    try:
        os.killpg(proc.pid, signal.SIGTERM)
    except ProcessLookupError:
        return
    try:
        proc.wait(timeout=_STOP_TIMEOUT_S)
    except subprocess.TimeoutExpired:
        try:
            os.killpg(proc.pid, signal.SIGKILL)
        except ProcessLookupError:
            return
        proc.wait(timeout=5)


def _log_tail(path: Path) -> str:
    """Return the last lines of a server log.

    Args:
        path: Log file written by the server subprocess.

    Returns:
        Up to ``_LOG_TAIL_LINES`` lines, or a note when the file is missing.
    """
    try:
        lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
    except OSError as err:
        return f"(no log: {err})"
    tail = lines[-_LOG_TAIL_LINES:]
    return "\n".join(tail)


def _run_mode(name: str, conf: Path, port: int, unix_path: Optional[str]) -> Optional[str]:
    """Start one layout, run its checks, and always stop the server.

    TCP shapes run first. A layout with ``unix_path`` then waits for that
    socket and runs the Unix unary call and server stream. The socket file
    is removed after the process group has exited.

    Args:
        name: Short layout name used in the failure report.
        conf: YAML passed to ``trpc_main --conf``.
        port: Port the TCP client dials.
        unix_path: Unix socket to call after the TCP shapes, or ``None``
            for the peer layout.

    Returns:
        ``None`` on success, otherwise an error that includes the log tail.
    """
    log_path = Path(tempfile.gettempdir()) / f"hellostream-{name}.log"
    log_file = log_path.open("w", encoding="utf-8")
    proc = subprocess.Popen(
        [sys.executable, "-m", "examples.hellostream.trpc_main", "--conf", str(conf)],
        cwd=str(_ROOT),
        stdout=log_file,
        stderr=subprocess.STDOUT,
        start_new_session=True,
    )
    error: Optional[str] = None
    try:
        if not _wait_ready(port, proc, _READY_TIMEOUT_S):
            error = f"{name}: server did not become ready on 127.0.0.1:{port}\n{_log_tail(log_path)}"
        else:
            try:
                asyncio.run(run_shapes(port))
                if unix_path is not None:
                    if not _wait_unix(unix_path, proc, _READY_TIMEOUT_S):
                        error = f"{name}: unix socket did not become ready at {unix_path}\n{_log_tail(log_path)}"
                    else:
                        asyncio.run(_run_unix(unix_path))
            except Exception as exc:  # noqa: BLE001 - report the shape and keep going
                error = f"{name}: {exc}\n{_log_tail(log_path)}"
    finally:
        _stop(proc)
        log_file.close()
        if unix_path is not None:
            Path(unix_path).unlink(missing_ok=True)
    return error


def main(argv: List[str] | None = None) -> int:
    """Run every layout and return 0 only when all of them pass.

    Args:
        argv: Unused. Accepted so the module can be called like a script.

    Returns:
        0 when every layout passes, 1 when any layout fails.
    """
    del argv
    failures: List[str] = []
    for name, conf, port, unix_path in _MODES:
        where = f":{port}" if unix_path is None else f":{port} {unix_path}"
        print(f"== {name} {conf.name} {where}", flush=True)
        error = _run_mode(name, conf, port, unix_path)
        if error is None:
            print(f"ok {name}", flush=True)
        else:
            print(error, flush=True)
            failures.append(name)
    if failures:
        print("failed: " + ", ".join(failures), flush=True)
        return 1
    print("all layouts passed", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
