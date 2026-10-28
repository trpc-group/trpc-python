# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Check the default single-loop server's user protocol.

    python features/check_user.py

Starts ``trpc_main.py`` with ``trpc_python.yaml`` (``task_num: 1`` still
forks a task child; this check does not call it). Sends one datagram to
the ``udp`` + ``user`` port and requires ``user:0:<pid>`` of that process,
then signals the process group so the task child exits with the server.
"""

from __future__ import annotations

import os
import signal
import socket
import subprocess
import sys
import threading
import time
from pathlib import Path

import yaml

_ROOT = Path(__file__).resolve().parents[1]
_CONF = _ROOT / "trpc_python.yaml"
_SCRIPT = _ROOT / "trpc_main.py"
_SERVICE = "trpc.trpc_python_group.helloworld.GreeterUser"
_PORT = 8090
_READY_TIMEOUT_S = 20.0
_STOP_TIMEOUT_S = 8.0


def _user_port(conf: Path) -> int:
    """Return the ``udp`` + ``user`` port from the default YAML.

    Args:
        conf: ``trpc_python.yaml`` next to ``trpc_main.py``.

    Returns:
        The port the datagram is sent to.

    Raises:
        AssertionError: The file does not have exactly one ``GreeterUser``
            service on ``_PORT``.
    """
    data = yaml.safe_load(conf.read_text(encoding="utf-8"))
    matches = [
        item for item in data["server"]["service"]
        if item.get("name") == _SERVICE and item.get("network") == "udp" and item.get("protocol") == "user"
    ]
    if len(matches) != 1 or int(matches[0]["port"]) != _PORT:
        raise AssertionError(f"{conf} should serve {_SERVICE} on udp {_PORT}, found {matches}")
    return _PORT


def _user_once(port: int, timeout: float) -> str:
    """Send one datagram and return the reply text.

    Args:
        port: User-protocol UDP port.
        timeout: Seconds to wait for the reply.

    Returns:
        Reply text.

    Raises:
        OSError: The port is closed or the reply did not arrive.
    """
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        sock.settimeout(timeout)
        sock.sendto(b"ping", ("127.0.0.1", port))
        data, _addr = sock.recvfrom(256)
        return data.decode()
    finally:
        sock.close()


def _group_members(pgid: int) -> list[str]:
    """Return pids still in ``pgid``.

    Args:
        pgid: Process group id. With ``start_new_session=True`` this is the
            server pid, and it includes the task child.

    Returns:
        Pid strings. Empty when the group is gone.
    """
    proc = subprocess.run(["pgrep", "-g", str(pgid)], capture_output=True, text=True, check=False)
    if proc.returncode == 1:
        return []
    if proc.returncode != 0:
        raise AssertionError(f"pgrep failed ({proc.returncode}): {proc.stderr}")
    return [line for line in proc.stdout.split() if line]


def _stop_group(proc: subprocess.Popen) -> int:
    """SIGTERM the server process group, then SIGKILL if anyone remains.

    Args:
        proc: Server started with ``start_new_session=True``.

    Returns:
        The server process exit code, or ``-1`` when it never exited.
    """
    try:
        os.killpg(proc.pid, signal.SIGTERM)
    except ProcessLookupError:
        pass
    deadline = time.monotonic() + _STOP_TIMEOUT_S
    while time.monotonic() < deadline:
        if proc.poll() is not None and not _group_members(proc.pid):
            return proc.returncode if proc.returncode is not None else -1
        time.sleep(0.05)
    try:
        os.killpg(proc.pid, signal.SIGKILL)
    except ProcessLookupError:
        pass
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        pass
    return proc.returncode if proc.returncode is not None else -1


def _read_stdout(proc: subprocess.Popen, chunks: list[str]) -> None:
    """Append the server's combined stdout until the pipe closes."""
    assert proc.stdout is not None
    for line in proc.stdout:
        chunks.append(line)


def main() -> None:
    """Start the default server, check one user-protocol reply, and stop the group.

    Raises:
        AssertionError: The reply was not ``user:0:<pid>``, or the group
            did not exit cleanly.
    """
    port = _user_port(_CONF)
    env = os.environ.copy()
    env["PYTHONUNBUFFERED"] = "1"
    proc = subprocess.Popen(  # pylint: disable=consider-using-with
        [sys.executable, "-u", str(_SCRIPT), "--conf", str(_CONF)],
        cwd=str(_ROOT),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        env=env,
        start_new_session=True,
    )
    chunks: list[str] = []
    reader = threading.Thread(target=_read_stdout, args=(proc, chunks), name="server-stdout", daemon=True)
    reader.start()
    reply = ""
    error: str | None = None
    try:
        deadline = time.monotonic() + _READY_TIMEOUT_S
        while time.monotonic() < deadline:
            if proc.poll() is not None:
                error = f"server exited early ({proc.returncode})"
                break
            try:
                reply = _user_once(port, 0.4)
            except OSError:
                time.sleep(0.05)
                continue
            if reply == f"user:0:{proc.pid}":
                break
            error = f"user protocol reply {reply!r}, want user:0:{proc.pid}"
            break
        else:
            error = f"no user protocol reply on 127.0.0.1:{port} within {_READY_TIMEOUT_S:.0f}s"
        if error is None and reply != f"user:0:{proc.pid}":
            error = f"user protocol reply {reply!r}, want user:0:{proc.pid}"
    finally:
        code = _stop_group(proc)
        reader.join(timeout=5)
    output = "".join(chunks)
    if error is not None:
        raise AssertionError(f"{error}\n{output}")
    if code != 0:
        raise AssertionError(f"server exit {code}, want 0\n{output}")
    if _group_members(proc.pid):
        raise AssertionError(f"process group {proc.pid} still has members\n{output}")
    if "Traceback" in output:
        raise AssertionError(f"server log contains a traceback\n{output}")
    print(f"ok user 127.0.0.1:{port} pid={proc.pid}")


if __name__ == "__main__":
    main()
