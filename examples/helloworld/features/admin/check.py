# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Check the admin example on this machine.

    python features/admin/check.py
    python features/admin/check.py merge

Run from anywhere. For each requested mode this process starts
``server.py`` with that mode's config, curls the admin HTTP API and exits
non-zero when a check fails. With no argument it checks merge, peer and
separate in turn.
"""

from __future__ import annotations

import json
import signal
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import List
from typing import Optional

_DIR = Path(__file__).resolve().parent
_SCRIPT = _DIR / "server.py"
_REPO = _DIR.parents[2]

_READY_TIMEOUT_S = 20.0
_STOP_TIMEOUT_S = 10.0

# Built-in commands plus the whoami command this example registers.
_EXPECTED_CMDS = [
    "/version",
    "/cmds/loglevel",
    "/cmds/config",
    "/cmds/rpcz",
    "/cmds/debug/language",
    "/cmds/whoami",
]

# mode -> (config file, admin port, workers that connect to admin)
_MODES = {
    "merge": ("merge.yaml", 19420, 1),
    "peer": ("peer.yaml", 19421, 2),
    "separate": ("separate.yaml", 19422, 3),
}


def _fail(message: str) -> None:
    """Print a failing check and stop the script.

    Args:
        message: What the check expected and what it saw.
    """
    raise SystemExit(f"FAIL: {message}")


def _wait_for_port(port: int, proc: subprocess.Popen) -> None:
    """Block until ``port`` accepts a connection or the server dies.

    Args:
        port: Admin port from the mode's config.
        proc: The server process, polled so a crash fails fast.
    """
    deadline = time.monotonic() + _READY_TIMEOUT_S
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            _fail(f"server exited early with code {proc.returncode}")
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.5):
                return
        except OSError:
            time.sleep(0.2)
    _fail(f"admin port {port} not reachable after {_READY_TIMEOUT_S}s")


def _get(port: int, path: str) -> dict:
    """GET one admin endpoint and decode its JSON body.

    Args:
        port: Admin port.
        path: Request path, starting with ``/``.

    Returns:
        The decoded response body.
    """
    with urllib.request.urlopen(f"http://127.0.0.1:{port}{path}", timeout=10) as resp:
        return json.loads(resp.read())


def _put(port: int, path: str, body: bytes) -> dict:
    """PUT form data to one admin endpoint and decode its JSON body.

    Args:
        port: Admin port.
        path: Request path, starting with ``/``.
        body: Raw form body.

    Returns:
        The decoded response body.
    """
    req = urllib.request.Request(f"http://127.0.0.1:{port}{path}", data=body, method="PUT")
    with urllib.request.urlopen(req, timeout=10) as resp:
        return json.loads(resp.read())


def _stop(proc: subprocess.Popen) -> None:
    """SIGTERM the server and its children, escalating to SIGKILL.

    Args:
        proc: The server process started for one mode.
    """
    if proc.poll() is not None:
        return
    proc.send_signal(signal.SIGTERM)
    try:
        proc.wait(timeout=_STOP_TIMEOUT_S)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait(timeout=_STOP_TIMEOUT_S)


def _check_mode(mode: str) -> None:
    """Start one mode and check every admin command against it.

    Args:
        mode: One of ``merge``, ``peer``, ``separate``.
    """
    conf_name, port, worker_count = _MODES[mode]
    proc = subprocess.Popen(
        [sys.executable, str(_SCRIPT), "--conf", str(_DIR / "conf" / conf_name)],
        cwd=str(_REPO),
    )
    try:
        _wait_for_port(port, proc)
        # Workers connect shortly after the port opens; give them a moment.
        time.sleep(1.0)
        _check_endpoints(mode, port, worker_count)
    finally:
        _stop(proc)
    print(f"ok: {mode}")


def _check_endpoints(mode: str, port: int, worker_count: int) -> None:
    """Assert the admin HTTP responses for one running mode.

    Args:
        mode: Mode name, only used in failure messages.
        port: Admin port.
        worker_count: How many pids ``/cmds/whoami`` must report.
    """
    version = _get(port, "/version")
    if version.get("errorcode") != 0 or not version.get("version"):
        _fail(f"{mode} /version -> {version}")

    cmds = _get(port, "/cmds").get("cmds", [])
    missing = [path for path in _EXPECTED_CMDS if path not in cmds]
    if missing:
        _fail(f"{mode} /cmds missing {missing}, got {cmds}")

    level = _get(port, "/cmds/loglevel")
    if level.get("logger") != "default":
        _fail(f"{mode} /cmds/loglevel -> {level}")
    changed = _put(port, "/cmds/loglevel", b"value=warning")
    if changed.get("level") != "warning":
        _fail(f"{mode} PUT /cmds/loglevel -> {changed}")

    config = _get(port, "/cmds/config")
    admin = config.get("content", {}).get("server", {}).get("admin", {})
    if admin.get("port") != port:
        _fail(f"{mode} /cmds/config admin -> {admin}")

    language = _get(port, "/cmds/debug/language")
    if language.get("language") != "python":
        _fail(f"{mode} /cmds/debug/language -> {language}")

    rpcz = _get(port, "/cmds/rpcz")
    if not isinstance(rpcz.get("spans"), list):
        _fail(f"{mode} /cmds/rpcz -> {rpcz}")

    whoami = _wait_for_whoami(mode, port, worker_count)
    pids = whoami["pids"]
    if len(pids) != worker_count or len(set(pids)) != worker_count:
        _fail(f"{mode} /cmds/whoami expected {worker_count} distinct pids, got {pids}")


def _wait_for_whoami(mode: str, port: int, worker_count: int) -> dict:
    """Poll ``/cmds/whoami`` until every worker has connected.

    Args:
        mode: Mode name, only used in the timeout message.
        port: Admin port.
        worker_count: How many pids must be present.

    Returns:
        The first response that lists enough pids.
    """
    deadline = time.monotonic() + _READY_TIMEOUT_S
    last = {}
    while time.monotonic() < deadline:
        try:
            last = _get(port, "/cmds/whoami")
        except (urllib.error.URLError, TimeoutError, json.JSONDecodeError):
            time.sleep(0.3)
            continue
        if len(last.get("pids", [])) >= worker_count:
            return last
        time.sleep(0.3)
    _fail(f"{mode} /cmds/whoami never reached {worker_count} workers, last={last}")
    return last


def main(argv: Optional[List[str]] = None) -> None:
    """Check the modes named on the command line, or all three.

    Args:
        argv: Mode names after the program name. ``None`` reads ``sys.argv``.
    """
    args = sys.argv[1:] if argv is None else argv
    unknown = [name for name in args if name not in _MODES]
    if unknown:
        raise SystemExit(f"unknown mode {unknown}, choose from {list(_MODES)}")
    for mode in args or list(_MODES):
        _check_mode(mode)


if __name__ == "__main__":
    main()
