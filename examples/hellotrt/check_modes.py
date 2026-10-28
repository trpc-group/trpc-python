# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Run ResNet50 against each hellotrt process layout.

Starts one server at a time, waits until the port is open and the expected
session lines are in the log, calls it, then signals the whole process
group. Peer has no task, so it only calls SayHello. A failure prints the
tail of that server's log and continues with the remaining layouts.
"""

from __future__ import annotations

import asyncio
import os
import re
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

_ROOT = Path(__file__).resolve().parents[2]
_HERE = Path(__file__).resolve().parent
# First run also downloads the checkpoint. Later layouts only read the cache.
_READY_TIMEOUT_S = 120
_STOP_TIMEOUT_S = 7
_LOG_TAIL_LINES = 80

_LOAD_RE = re.compile(r"SESSION_LOAD (\d+) (\d+) (\d+)")
_TASK_RE = re.compile(r"TASK_SESSION (\d+)")


def _child_env() -> dict:
    """Return the server environment.

    Unbuffered stdout so ``SESSION_LOAD`` reaches the log file before the
    checker reads it. Thread counts are set before the child imports Torch.

    Returns:
        A copy of the current environment plus those variables.
    """
    env = os.environ.copy()
    env["PYTHONUNBUFFERED"] = "1"
    env.setdefault("OMP_NUM_THREADS", "1")
    env.setdefault("MKL_NUM_THREADS", "1")
    return env


# name, conf, port, serving-loop count, whether a task process is registered.
_MODES: Sequence[Tuple[str, Path, int, int, bool]] = (
    ("merge", _HERE / "trpc_python.yaml", 8000, 1, True),
    ("single_process_merge", _HERE / "conf" / "single_process_merge.yaml", 8110, 3, True),
    ("single_process_separate", _HERE / "conf" / "single_process_separate.yaml", 8120, 3, True),
    ("multi_process_separate", _HERE / "conf" / "multi_process_separate.yaml", 8130, 2, True),
    ("multi_process_merge", _HERE / "conf" / "multi_process_merge.yaml", 8140, 2, False),
)


def _wait_port(port: int, proc: subprocess.Popen, deadline: float) -> bool:
    """Return whether ``port`` accepts a connection before ``deadline``.

    Args:
        port: TCP port from the layout YAML.
        proc: Server process. A non-zero poll means it already exited.
        deadline: ``time.monotonic`` value when the wait gives up.

    Returns:
        True when a connect succeeds while the process is still alive.
    """
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            return False
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(0.2)
        try:
            sock.connect(("127.0.0.1", port))
        except OSError:
            time.sleep(0.2)
        else:
            return True
        finally:
            sock.close()
    return False


def _log_text(path: Path) -> str:
    """Return the server log, or an empty string when it cannot be read.

    Args:
        path: Log file written by the server subprocess.
    """
    try:
        return path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return ""


def _log_tail(path: Path) -> str:
    """Return the last lines of a server log.

    Args:
        path: Log file written by the server subprocess.

    Returns:
        Up to ``_LOG_TAIL_LINES`` lines, or a note when the file is missing.
    """
    text = _log_text(path)
    if not text and not path.exists():
        return "(no log)"
    lines = text.splitlines()
    return "\n".join(lines[-_LOG_TAIL_LINES:])


def _sessions_ready(text: str, loops: int, expect_task: bool) -> bool:
    """Return whether the log already has every expected session line.

    Args:
        text: Server log so far.
        loops: How many ``SESSION_LOAD`` lines this layout prints.
        expect_task: Whether one ``TASK_SESSION`` line is required.
    """
    if len(_LOAD_RE.findall(text)) < loops:
        return False
    if expect_task and len(_TASK_RE.findall(text)) < 1:
        return False
    return True


def _check_sessions(text: str, loops: int, expect_task: bool) -> None:
    """Require the exact session lines for this layout.

    Args:
        text: Full server log after the calls.
        loops: Expected ``SESSION_LOAD`` count.
        expect_task: Whether exactly one ``TASK_SESSION`` line is required.

    Raises:
        AssertionError: The counts do not match.
    """
    loads = _LOAD_RE.findall(text)
    tasks = _TASK_RE.findall(text)
    if len(loads) != loops:
        raise AssertionError(f"SESSION_LOAD count {len(loads)}, want {loops}: {loads}")
    if expect_task and len(tasks) != 1:
        raise AssertionError(f"TASK_SESSION count {len(tasks)}, want 1")
    if not expect_task and tasks:
        raise AssertionError(f"peer printed TASK_SESSION: {tasks}")


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


def _run_mode(name: str, conf: Path, port: int, loops: int, expect_task: bool) -> Optional[str]:
    """Start one layout, run its checks, and always stop the server.

    Args:
        name: Short layout name used in the failure report.
        conf: YAML passed to ``trpc_main --conf``.
        port: Port the client dials.
        loops: Expected ``SESSION_LOAD`` lines.
        expect_task: Call ``SayHi`` and require one ``TASK_SESSION`` line.

    Returns:
        ``None`` on success, otherwise an error that includes the log tail.
    """
    # Imported here so ``python -m examples.hellotrt.check_modes`` inserts the
    # stub path from the client module before any generated import.
    from examples.hellotrt.test_service_rpc import run_calls

    log_path = Path(tempfile.gettempdir()) / f"hellotrt-{name}.log"
    log_file = log_path.open("w", encoding="utf-8")
    proc = subprocess.Popen(
        [sys.executable, "-m", "examples.hellotrt.trpc_main", "--conf", str(conf)],
        cwd=str(_ROOT),
        stdout=log_file,
        stderr=subprocess.STDOUT,
        start_new_session=True,
        env=_child_env(),
    )
    error: Optional[str] = None
    deadline = time.monotonic() + _READY_TIMEOUT_S
    try:
        ready = False
        while time.monotonic() < deadline:
            if proc.poll() is not None:
                break
            port_up = _wait_port(port, proc, min(deadline, time.monotonic() + 1))
            log_file.flush()
            if port_up and _sessions_ready(_log_text(log_path), loops, expect_task):
                ready = True
                break
            if port_up:
                time.sleep(0.5)
        if not ready:
            error = f"{name}: server did not become ready on 127.0.0.1:{port}\n{_log_tail(log_path)}"
        else:
            try:
                asyncio.run(run_calls(port, say_hi=expect_task))
                log_file.flush()
                _check_sessions(_log_text(log_path), loops, expect_task)
            except Exception as exc:  # noqa: BLE001 - report the layout and keep going
                error = f"{name}: {exc}\n{_log_tail(log_path)}"
    finally:
        _stop(proc)
        log_file.close()
    return error


def main(argv: Optional[List[str]] = None) -> int:
    """Run every layout and return 0 only when all of them pass.

    Args:
        argv: Unused. Accepted so the module can be called like a script.

    Returns:
        0 when every layout passes, 1 when any layout fails.
    """
    del argv
    failures: List[str] = []
    for name, conf, port, loops, expect_task in _MODES:
        print(f"== {name} {conf.name} :{port}", flush=True)
        error = _run_mode(name, conf, port, loops, expect_task)
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
