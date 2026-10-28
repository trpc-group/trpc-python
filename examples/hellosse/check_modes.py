# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Read one SSE event from each hellosse process layout.

Starts one server at a time, reads the first event, then signals the whole
process group so multi-process children do not keep the port. A failure
prints the tail of that server's log and continues with the remaining layouts.

The check only requires one event that contains ``sse 0``. macOS does not
balance ``SO_REUSEPORT``, and one SSE connection stays on the loop that
accepted it, so this does not try to hit every worker or handle thread.
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

from trpc_sdk.abc import LogLevelType
from trpc_sdk.abc import TrpcContext
from trpc_sdk.client import Options
from trpc_sdk.http import SSEClient
from trpc_sdk.log import get_logger

_ROOT = Path(__file__).resolve().parents[2]
_HERE = Path(__file__).resolve().parent
_READY_TIMEOUT_S = 15
# YAML close_wait_time is 2000 ms on the conf/ layouts. Leave extra time
# for the children to exit. The default single-loop file waits 0 ms.
_STOP_TIMEOUT_S = 7
_EVENT_TIMEOUT_S = 10
_LOG_TAIL_LINES = 80

_MODES: Sequence[Tuple[str, Path, int]] = (
    ("merge", _HERE / "trpc_python.yaml", 8080),
    ("single_process_merge", _HERE / "conf" / "single_process_merge.yaml", 8081),
    ("single_process_separate", _HERE / "conf" / "single_process_separate.yaml", 8082),
    ("multi_process_separate", _HERE / "conf" / "multi_process_separate.yaml", 8083),
    ("multi_process_merge", _HERE / "conf" / "multi_process_merge.yaml", 8084),
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


def _stop(proc: subprocess.Popen) -> None:
    """SIGTERM the server process group, then SIGKILL if it is still alive.

    Args:
        proc: Server started with ``start_new_session=True``. Its pid is the
            process-group id, which includes forked children. Multi-process
            children ignore SIGTERM and stop when the parent forwards
            shutdown; SIGKILL is the fallback if that drain does not finish.
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


async def _read_first_event(port: int) -> None:
    """Read one SSE event and disconnect.

    The server writes the first frame before it sleeps, so cancelling this
    generator after that frame is enough. The handler's ``CancelledError``
    path finishes the response.

    Args:
        port: TCP port of the already-running server.

    Raises:
        AssertionError: The stream ended early, or the first event does not
            contain ``sse 0``.
    """
    client = SSEClient()
    options = [Options().set_timeout(_EVENT_TIMEOUT_S * 1000)]
    async for event in client.request_events(TrpcContext(), "GET", f"http://127.0.0.1:{port}/sse", options=options):
        if "sse 0" not in event.data:
            raise AssertionError(f"first event {event.data!r} does not contain 'sse 0'")
        return
    raise AssertionError("stream ended before the first event")


def _run_mode(name: str, conf: Path, port: int, loop: asyncio.AbstractEventLoop) -> Optional[str]:
    """Start one layout, read one SSE event, and always stop the server.

    The HTTP client keeps one aiohttp session for the process. Every layout
    therefore reads on the same loop; a fresh ``asyncio.run`` would close
    that loop and the next request would fail.

    Args:
        name: Short layout name used in the failure report.
        conf: YAML passed to ``trpc_main --conf``.
        port: Port the client dials.
        loop: Loop shared by every layout's client read.

    Returns:
        ``None`` on success, otherwise an error that includes the log tail.
    """
    log_path = Path(tempfile.gettempdir()) / f"hellosse-{name}.log"
    log_file = log_path.open("w", encoding="utf-8")
    proc: Optional[subprocess.Popen] = None
    error: Optional[str] = None
    try:
        proc = subprocess.Popen(
            [sys.executable, "-m", "examples.hellosse.trpc_main", "--conf", str(conf)],
            cwd=str(_ROOT),
            stdout=log_file,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
        if not _wait_ready(port, proc, _READY_TIMEOUT_S):
            error = f"{name}: server did not become ready on 127.0.0.1:{port}\n{_log_tail(log_path)}"
        else:
            try:
                loop.run_until_complete(asyncio.wait_for(_read_first_event(port), _EVENT_TIMEOUT_S))
            except Exception as exc:  # noqa: BLE001 - report the event and keep going
                error = f"{name}: {exc}\n{_log_tail(log_path)}"
    finally:
        if proc is not None:
            _stop(proc)
        log_file.close()
    return error


def main(argv: List[str] | None = None) -> int:
    """Run every layout and return 0 only when all of them pass.

    Args:
        argv: Unused. Accepted so the module can be called like a script.

    Returns:
        0 when every layout passes, 1 when any layout fails.
    """
    del argv
    # No server YAML is loaded here, so the client logger stays at debug.
    get_logger("default").set_level(LogLevelType.WARNING)
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    failures: List[str] = []
    try:
        for name, conf, port in _MODES:
            print(f"== {name} {conf.name} :{port}", flush=True)
            error = _run_mode(name, conf, port, loop)
            if error is None:
                print(f"ok {name}", flush=True)
            else:
                print(error, flush=True)
                failures.append(name)
    finally:
        # Close SSE generators left open after the first event. Dropping the
        # loop without this leaves their ``aclose`` tasks pending.
        try:
            loop.run_until_complete(loop.shutdown_asyncgens())
        finally:
            loop.close()
    if failures:
        print("failed: " + ", ".join(failures), flush=True)
        return 1
    print("all layouts passed", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
