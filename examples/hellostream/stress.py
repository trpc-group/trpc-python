# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Stress the five hellostream process layouts.

Starts one server at a time with ``HELLOSTREAM_STRESS=1``, runs bidi and
client-stream traffic, and reports errors, latency, RSS change, and how many
processes and threads handled the streams. Stream frames are pinned by
``(session_id + stream_id)``. Each call opens one connection, so those two
ids would otherwise advance together and pin every stream to one worker
when the count is even; alternate calls skip a stream id so the split is
visible. ``multi_process_merge`` on macOS may show a single pid because
``SO_REUSEPORT`` does not balance; that is printed and is not a failure.

``stop_under_load`` opens bidi streams and then SIGTERMs the process group.
Client errors during that stop are expected.

Run from the repository root:

    python -m examples.hellostream.stress --mode merge --profile bidi --concurrency 32 --seconds 10
    python -m examples.hellostream.stress --all
"""

from __future__ import annotations

import argparse
import asyncio
import os
import re
import signal
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass
from dataclasses import field
from pathlib import Path
from typing import List
from typing import Optional
from typing import Sequence
from typing import Tuple

from examples.hellostream.check_modes import _MODES
from examples.hellostream.check_modes import _READY_TIMEOUT_S
from examples.hellostream.check_modes import _log_tail
from examples.hellostream.check_modes import _stop
from examples.hellostream.check_modes import _wait_ready

_ROOT = Path(__file__).resolve().parents[2]
_STUB = Path(__file__).resolve().parent / "stub"
if str(_STUB) not in sys.path:
    sys.path.insert(0, str(_STUB))

from trpc_py_stream_helloworld import pb  # noqa: E402
from trpc_py_stream_helloworld import rpc  # noqa: E402

from trpc_sdk.abc import LogLevelType  # noqa: E402
from trpc_sdk.abc import TrpcContext  # noqa: E402
from trpc_sdk.client import Options  # noqa: E402
from trpc_sdk.log import DEFAULT_LOG_NAME  # noqa: E402
from trpc_sdk.log import get_logger  # noqa: E402
from trpc_sdk.serialization import SerializationType  # noqa: E402

_SERVICE = "trpc.py_stream.helloworld.Greeter"
# How many streams this process has opened. Used to skip a stream id.
_streams_opened = 0
_LADDER: Tuple[int, ...] = (1, 16, 64)
_DEFAULT_SECONDS = 5.0
_DEFAULT_FRAMES = 16
_STOP_LOAD_CONCURRENCY = 16
# Service YAML timeout is 10s and request timeouts are disabled. A short echo
# at or above that is a broken call, not a slow machine.
_P99_LIMIT_S = 10.0
_PAD = "x" * 256
_STAMP = re.compile(r"^pid=(\d+) tid=(\d+) n=(-?\d+)$")
_PROFILES = ("bidi", "client_stream", "stop_under_load")


@dataclass
class _Row:
    """One printed result line."""

    mode: str
    profile: str
    concurrency: int
    ok: int = 0
    errors: int = 0
    streams_per_s: Optional[float] = None
    frames_per_s: Optional[float] = None
    p50: Optional[float] = None
    p99: Optional[float] = None
    rss_delta_mb: Optional[float] = None
    pids: Optional[int] = None
    tids: Optional[int] = None
    note: str = ""
    failed: bool = False


@dataclass
class _Tally:
    """Counters for one concurrency step. Touched only on the client loop."""

    ok: int = 0
    errors: int = 0
    frames: int = 0
    latencies: List[float] = field(default_factory=list)
    pids: set = field(default_factory=set)
    tids: set = field(default_factory=set)
    first_error: str = ""

    def add_error(self, exc: BaseException) -> None:
        """Count ``exc`` and keep the first one for the result line.

        Args:
            exc: Failure of one stream.
        """
        self.errors += 1
        if not self.first_error:
            self.first_error = _short(exc)

    def add_ok(self, elapsed: float, frames: int, pid: int, tid: int) -> None:
        """Record one stream whose replies matched what was sent.

        Args:
            elapsed: Seconds from stream open to the last reply.
            frames: Data frames verified on this stream.
            pid: Process that handled the stream.
            tid: Thread that handled the stream.
        """
        self.ok += 1
        self.frames += frames
        self.latencies.append(elapsed)
        self.pids.add(pid)
        self.tids.add(tid)


@dataclass
class _Running:
    """A server subprocess and the temp files tied to it."""

    proc: subprocess.Popen
    log_path: Path
    log_file: object
    conf_path: Path


def _quiet_client_log() -> None:
    """Hide the stress process's framework logs.

    This process never loads a log plugin, so it keeps the bootstrap logger.
    That logger defaults to debug and prints one line per dial. Stopping a
    server under load also logs a connection reset per stream. Failures are
    reported on the result line and, when a step fails, in the server log.
    """
    get_logger(DEFAULT_LOG_NAME).set_level(LogLevelType.FATAL)


def _short(exc: BaseException) -> str:
    """Return a single-line description of ``exc``, capped for the result line.

    Args:
        exc: Exception raised by one stream.

    Returns:
        ``TypeName: message``, trimmed to 180 characters.
    """
    text = " ".join(f"{type(exc).__name__}: {exc}".split())
    if len(text) > 180:
        return text[:177] + "..."
    return text


def _payload(index: int) -> str:
    """Build a request name with a sequence prefix and 256 bytes of padding.

    Args:
        index: Zero-based frame index on this stream.

    Returns:
        ``n=<index>`` followed by the padding.
    """
    return f"n={index} {_PAD}"


def _skew_stream_id(proxy: rpc.GreeterClientProxyImpl) -> None:
    """Skip one stream id on every other call.

    Each call opens one TCP connection, so the server session id and the
    client stream id otherwise both advance by one. Their sum then stays on
    one residue modulo 2, and every stream pins to the same worker when
    ``handle_num`` is 2. Skipping an id on alternate calls moves the sum by
    2, then by 3, so both a 2-worker and a 3-thread split are exercised.

    Args:
        proxy: Client whose stream-id counter is advanced before the open.
    """
    global _streams_opened
    _streams_opened += 1
    if _streams_opened % 2 == 0:
        proxy.stream_client.stream_id += 1


def _options(port: int) -> Options:
    """Build call options that do not load YAML.

    Args:
        port: TCP port of the already-running server.

    Returns:
        Options for one stream open.
    """
    return (Options().set_target(f"ip://127.0.0.1:{port}").set_protocol("trpc").set_serialization_type(
        SerializationType.PB).set_network("tcp").set_timeout(15000))


def _parse_stamp(message: str) -> Tuple[int, int, int]:
    """Parse ``pid=<pid> tid=<tid> n=<n>``.

    Args:
        message: ``HelloReply.message``.

    Returns:
        ``(pid, tid, n)``.

    Raises:
        AssertionError: ``message`` is not that stamp.
    """
    match = _STAMP.match(message)
    if match is None:
        raise AssertionError(f"bad reply {message!r}")
    return int(match.group(1)), int(match.group(2)), int(match.group(3))


def _percentile(samples: Sequence[float], pct: float) -> Optional[float]:
    """Return a linearly interpolated percentile, or ``None`` when empty.

    Args:
        samples: Latencies in seconds.
        pct: Fraction in ``[0, 1]``.

    Returns:
        The percentile, or ``None`` when ``samples`` is empty.
    """
    if not samples:
        return None
    ordered = sorted(samples)
    if len(ordered) == 1:
        return ordered[0]
    rank = (len(ordered) - 1) * pct
    low = int(rank)
    high = min(low + 1, len(ordered) - 1)
    weight = rank - low
    return ordered[low] * (1.0 - weight) + ordered[high] * weight


def _format_ms(seconds: Optional[float]) -> str:
    """Format a latency as milliseconds, or ``-`` when it was not measured.

    Args:
        seconds: Latency in seconds.

    Returns:
        ``12.3ms`` or ``-``.
    """
    if seconds is None:
        return "-"
    return f"{seconds * 1000:.1f}ms"


def _format_rate(value: Optional[float]) -> str:
    """Format a per-second rate, or ``-`` when it was not measured.

    Args:
        value: Streams or frames per second.

    Returns:
        One decimal, or ``-``.
    """
    if value is None:
        return "-"
    return f"{value:.1f}"


def _format_mb(value: Optional[float]) -> str:
    """Format an RSS delta in megabytes, or ``-`` when it was not measured.

    Args:
        value: Megabytes, after minus before.

    Returns:
        A signed number with two decimals, or ``-``.
    """
    if value is None:
        return "-"
    return f"{value:+.2f}"


def _format_count(value: Optional[int]) -> str:
    """Format a distinct-id count, or ``-`` when it was not measured.

    Args:
        value: Number of distinct pids or thread ids.

    Returns:
        The count, or ``-``.
    """
    if value is None:
        return "-"
    return str(value)


def _format(row: _Row) -> str:
    """Format one result line.

    Args:
        row: Counters for one layout, profile, and concurrency.

    Returns:
        A single line. ``FAIL`` is appended when the step did not pass.
    """
    note = f" {row.note}" if row.note else ""
    flag = " FAIL" if row.failed else ""
    return (f"{row.mode} {row.profile} c={row.concurrency} "
            f"ok={row.ok} err={row.errors} "
            f"streams/s={_format_rate(row.streams_per_s)} frames/s={_format_rate(row.frames_per_s)} "
            f"p50={_format_ms(row.p50)} p99={_format_ms(row.p99)} "
            f"rss_delta_mb={_format_mb(row.rss_delta_mb)} "
            f"pids={_format_count(row.pids)} tids={_format_count(row.tids)}{note}{flag}")


def _quiet_conf(src: Path) -> Path:
    """Copy ``src`` to a temp file with the console log level set to error.

    Args:
        src: Layout YAML. Its ``level: info`` writer line is rewritten.

    Returns:
        Path of the temp YAML. The caller deletes it after the server stops.
    """
    text = src.read_text(encoding="utf-8").replace("level: info", "level: error")
    handle = tempfile.NamedTemporaryFile("w", encoding="utf-8", suffix=".yaml", delete=False)
    try:
        handle.write(text)
    finally:
        handle.close()
    return Path(handle.name)


def _start(name: str, conf: Path) -> _Running:
    """Start one layout with the stress servicer and a quiet log level.

    Args:
        name: Layout name, used in the log file name.
        conf: YAML for this layout.

    Returns:
        The live process and the temp files to close later.
    """
    quiet = _quiet_conf(conf)
    log_path = Path(tempfile.gettempdir()) / f"hellostream-stress-{name}.log"
    log_file = log_path.open("w", encoding="utf-8")
    env = os.environ.copy()
    env["HELLOSTREAM_STRESS"] = "1"
    try:
        proc = subprocess.Popen(
            [sys.executable, "-m", "examples.hellostream.trpc_main", "--conf", str(quiet)],
            cwd=str(_ROOT),
            stdout=log_file,
            stderr=subprocess.STDOUT,
            start_new_session=True,
            env=env,
        )
    except Exception:
        log_file.close()
        quiet.unlink(missing_ok=True)
        raise
    return _Running(proc=proc, log_path=log_path, log_file=log_file, conf_path=quiet)


def _close_files(running: _Running) -> None:
    """Close the server log and delete the temp YAML.

    Args:
        running: Server started by :func:`_start`. The process is not stopped.
    """
    log_file = running.log_file
    close = getattr(log_file, "close", None)
    if close is not None:
        close()
    running.conf_path.unlink(missing_ok=True)


def _group_alive(pgid: int) -> bool:
    """Return whether any process remains in ``pgid``.

    Args:
        pgid: Process group id. With ``start_new_session=True`` this is the
            server pid.

    Returns:
        True when ``killpg`` still finds a member.
    """
    try:
        os.killpg(pgid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def _check_stopped(proc: subprocess.Popen) -> Optional[str]:
    """Return why ``proc`` did not stop cleanly, or ``None``.

    A SIGTERM that the server handles and exits from is clean. SIGKILL means
    :func:`_stop` gave up. A child still in the process group after a short
    pause is also a failure.

    Args:
        proc: Server process after :func:`_stop`.

    Returns:
        A short failure reason, or ``None`` when the group is gone.
    """
    if proc.returncode == -signal.SIGKILL:
        return "server required SIGKILL"
    if proc.poll() is None:
        return "server still running"
    if not _group_alive(proc.pid):
        return None
    time.sleep(0.3)
    if _group_alive(proc.pid):
        return "child processes still alive"
    return None


def _group_rss_kb(pgid: int) -> int:
    """Sum RSS of every process in ``pgid``.

    Args:
        pgid: Server process group id.

    Returns:
        Kilobytes, or ``-1`` when ``ps`` cannot be read.
    """
    try:
        output = subprocess.check_output(["ps", "-ax", "-o", "pgid=,rss="], text=True)
    except (OSError, subprocess.CalledProcessError):
        return -1
    total = 0
    for line in output.splitlines():
        parts = line.split()
        if len(parts) < 2:
            continue
        try:
            row_pgid = int(parts[0])
            rss = int(parts[1])
        except ValueError:
            continue
        if row_pgid == pgid:
            total += rss
    return total


def _rss_delta_mb(before_kb: int, after_kb: int) -> Optional[float]:
    """Return the RSS change in megabytes.

    Args:
        before_kb: Group RSS before the step, or ``-1`` when unknown.
        after_kb: Group RSS after the step, or ``-1`` when unknown.

    Returns:
        After minus before, or ``None`` when either sample is missing.
    """
    if before_kb < 0 or after_kb < 0:
        return None
    return (after_kb - before_kb) / 1024.0


async def _bidi_once(proxy: rpc.GreeterClientProxyImpl, port: int, frames: int) -> Tuple[float, int, int]:
    """Open one bidi stream, send ``frames``, and check the echoes.

    Args:
        proxy: Shared generated client.
        port: TCP port of the server.
        frames: Data frames to send. Replies must echo ``n`` in that order
            and stay on one pid and one thread.

    Returns:
        ``(elapsed_seconds, pid, tid)``.

    Raises:
        AssertionError: The replies do not match the sends, or the stream
            moved between workers.
        EOFError: The server closed before the expected replies.
    """
    started = time.perf_counter()
    _skew_stream_id(proxy)
    stream = await proxy.asyncSayHelloAll(TrpcContext(), _options(port))
    for index in range(frames):
        request = pb.HelloRequest()
        request.name = _payload(index)
        await stream.send(request)
    await stream.close_send()
    got: List[str] = []
    while True:
        try:
            reply = await stream.recv()
        except EOFError:
            break
        got.append(reply.message)
    elapsed = time.perf_counter() - started
    if len(got) != frames:
        raise AssertionError(f"reply count {len(got)} != {frames}")
    pids = set()
    tids = set()
    for index, message in enumerate(got):
        pid, tid, n = _parse_stamp(message)
        if n != index:
            raise AssertionError(f"reply n={n} at index {index}")
        pids.add(pid)
        tids.add(tid)
    if len(pids) != 1 or len(tids) != 1:
        raise AssertionError(f"stream hopped pid={pids} tid={tids}")
    return elapsed, next(iter(pids)), next(iter(tids))


async def _client_once(proxy: rpc.GreeterClientProxyImpl, port: int, frames: int) -> Tuple[float, int, int]:
    """Open one client stream, send ``frames``, and check the closing count.

    Args:
        proxy: Shared generated client.
        port: TCP port of the server.
        frames: Data frames to send. The close reply's ``n`` must equal this.

    Returns:
        ``(elapsed_seconds, pid, tid)``.

    Raises:
        AssertionError: The close reply is missing or its count is wrong.
    """
    started = time.perf_counter()
    _skew_stream_id(proxy)
    stream = await proxy.asyncSayHelloReq(TrpcContext(), _options(port))
    for index in range(frames):
        request = pb.HelloRequest()
        request.name = _payload(index)
        await stream.send(request)
    reply = await stream.close_and_recv()
    elapsed = time.perf_counter() - started
    pid, tid, n = _parse_stamp(reply.message)
    if n != frames:
        raise AssertionError(f"frame count {n} != {frames}")
    return elapsed, pid, tid


async def _run_level(proxy: rpc.GreeterClientProxyImpl, port: int, proc: subprocess.Popen, profile: str,
                      concurrency: int, seconds: float, frames: int) -> _Row:
    """Run one profile at one concurrency until ``seconds`` have passed.

    Workers stop starting new streams at the deadline. A stream already open
    is allowed to finish.

    Args:
        proxy: Shared generated client.
        port: TCP port of the server.
        proc: Server process, polled for an early exit.
        profile: ``bidi`` or ``client_stream``.
        concurrency: Number of workers, each one stream at a time.
        seconds: How long to keep opening streams.
        frames: Data frames per stream.

    Returns:
        The result line for this step. ``failed`` is set on any stream error,
        a dead server, no finished stream, or ``p99`` at or above 10 seconds.
    """
    rss_before = _group_rss_kb(proc.pid)
    tally = _Tally()
    deadline = time.monotonic() + seconds
    call = _bidi_once if profile == "bidi" else _client_once

    async def worker() -> None:
        while time.monotonic() < deadline:
            try:
                elapsed, pid, tid = await call(proxy, port, frames)
            except Exception as exc:  # pylint: disable=broad-except
                tally.add_error(exc)
                continue
            tally.add_ok(elapsed, frames, pid, tid)

    started = time.perf_counter()
    await asyncio.gather(*[asyncio.create_task(worker()) for _ in range(concurrency)])
    elapsed = max(time.perf_counter() - started, 1e-9)
    rss_after = _group_rss_kb(proc.pid)
    p50 = _percentile(tally.latencies, 0.50)
    p99 = _percentile(tally.latencies, 0.99)
    notes: List[str] = []
    if tally.first_error:
        notes.append(tally.first_error)
    if proc.poll() is not None:
        notes.append(f"server exited rc={proc.returncode}")
    if p99 is not None and p99 >= _P99_LIMIT_S:
        notes.append(f"p99 {p99:.3f}s >= {_P99_LIMIT_S:.0f}s")
    if tally.ok == 0 and tally.errors == 0:
        notes.append("no streams finished")
    return _Row(
        mode="",
        profile=profile,
        concurrency=concurrency,
        ok=tally.ok,
        errors=tally.errors,
        streams_per_s=tally.ok / elapsed,
        frames_per_s=tally.frames / elapsed,
        p50=p50,
        p99=p99,
        rss_delta_mb=_rss_delta_mb(rss_before, rss_after),
        pids=len(tally.pids),
        tids=len(tally.tids),
        note="; ".join(notes),
        failed=bool(notes),
    )


async def _cancel(tasks: Sequence[asyncio.Task]) -> None:
    """Cancel ``tasks`` and wait until they finish.

    Args:
        tasks: Tasks to cancel. Exceptions they already hold are discarded.
    """
    for task in tasks:
        task.cancel()
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)


def _first_task_error(tasks: Sequence[asyncio.Task]) -> Optional[str]:
    """Return the first finished task's exception text.

    Args:
        tasks: Pump tasks. A task that is still running is skipped.

    Returns:
        A short error, or ``None`` when no task has failed.
    """
    for task in tasks:
        if not task.done() or task.cancelled():
            continue
        exc = task.exception()
        if exc is not None:
            return _short(exc)
    return None


async def _pump(proxy: rpc.GreeterClientProxyImpl, port: int, started: asyncio.Event) -> None:
    """Keep one bidi stream in flight until cancelled.

    The first successful round trip sets ``started``. Later frames only keep
    the handler busy while the process group is signalled.

    Args:
        proxy: Shared generated client.
        port: TCP port of the server.
        started: Set after the first reply has been checked.

    Raises:
        AssertionError: The first reply is not the stamp for ``n=0``.
    """
    _skew_stream_id(proxy)
    stream = await proxy.asyncSayHelloAll(TrpcContext(), _options(port))
    index = 0
    while True:
        request = pb.HelloRequest()
        request.name = _payload(index)
        await stream.send(request)
        reply = await stream.recv()
        _pid, _tid, n = _parse_stamp(reply.message)
        if n != index:
            raise AssertionError(f"reply n={n} at index {index}")
        if index == 0:
            started.set()
        index += 1


async def _pump_and_stop(port: int, proc: subprocess.Popen, concurrency: int) -> Optional[str]:
    """Open ``concurrency`` bidi streams, then SIGTERM the server group.

    Args:
        port: TCP port of the server.
        proc: Server process. :func:`_stop` runs on a worker thread so the
            streams stay in flight on this loop.
        concurrency: Streams kept open.

    Returns:
        A failure reason when the load never started, or ``None`` after the
        signal. Whether the process actually exited is checked by the caller.
    """
    proxy = rpc.GreeterClientProxyImpl(_SERVICE)
    started = asyncio.Event()
    tasks = [asyncio.create_task(_pump(proxy, port, started)) for _ in range(concurrency)]
    starter = asyncio.create_task(started.wait())
    waiting = set(tasks)
    waiting.add(starter)
    try:
        while not started.is_set():
            done, waiting = await asyncio.wait(waiting, timeout=_READY_TIMEOUT_S, return_when=asyncio.FIRST_COMPLETED)
            if not done:
                return "load did not start"
            if all(task.done() for task in tasks):
                return _first_task_error(tasks) or "load did not start"
        await asyncio.to_thread(_stop, proc)
        return None
    finally:
        starter.cancel()
        await _cancel([starter, *tasks])


async def _stream_session(port: int, proc: subprocess.Popen, mode: str, levels: Sequence[Tuple[str, int, float,
                                                                                                int]]) -> List[_Row]:
    """Run every stream step against an already-ready server.

    Args:
        port: TCP port of the server.
        proc: Server process.
        mode: Layout name copied onto each row.
        levels: ``(profile, concurrency, seconds, frames)`` steps, in order.

    Returns:
        One row per step. Later steps are skipped once the server has exited.
    """
    proxy = rpc.GreeterClientProxyImpl(_SERVICE)
    rows: List[_Row] = []
    for profile, concurrency, seconds, frames in levels:
        if proc.poll() is not None:
            rows.append(
                _Row(mode=mode,
                     profile=profile,
                     concurrency=concurrency,
                     errors=1,
                     note=f"server exited rc={proc.returncode}",
                     failed=True))
            break
        row = await _run_level(proxy, port, proc, profile, concurrency, seconds, frames)
        row.mode = mode
        rows.append(row)
        if proc.poll() is not None:
            break
    return rows


def _ready_row(mode: str, profile: str, concurrency: int) -> _Row:
    """Return the row used when the listen port never accepts.

    Args:
        mode: Layout name.
        profile: Profile that was about to run.
        concurrency: Concurrency that was about to run.

    Returns:
        A failed row with a fixed note. The log tail is printed separately.
    """
    return _Row(mode=mode, profile=profile, concurrency=concurrency, errors=1, note="server did not become ready",
                failed=True)


def _emit_session(rows: List[_Row], log_path: Path) -> None:
    """Print result lines, then the server log tail when any line failed.

    Args:
        rows: Rows for one server process.
        log_path: Log written by that process. Already closed.
    """
    for row in rows:
        print(_format(row), flush=True)
    if any(row.failed for row in rows):
        print(_log_tail(log_path), flush=True)


def _run_stream_session(mode: str, conf: Path, port: int, levels: Sequence[Tuple[str, int, float, int]]) -> List[_Row]:
    """Start one server, run ``levels``, and always stop the process group.

    Args:
        mode: Layout name.
        conf: Layout YAML.
        port: TCP port from the layout YAML.
        levels: Stream steps. Empty when the caller only wants a stop test.

    Returns:
        Result rows, including a stop failure when the group does not exit
        on SIGTERM.
    """
    running = _start(mode, conf)
    rows: List[_Row] = []
    try:
        if not _wait_ready(port, running.proc, _READY_TIMEOUT_S):
            profile, concurrency, _seconds, _frames = levels[0]
            rows = [_ready_row(mode, profile, concurrency)]
        else:
            rows = asyncio.run(_stream_session(port, running.proc, mode, levels))
    except Exception as exc:  # pylint: disable=broad-except
        profile, concurrency, _seconds, _frames = levels[0]
        rows = [_Row(mode=mode, profile=profile, concurrency=concurrency, errors=1, note=_short(exc), failed=True)]
    finally:
        if running.proc.poll() is None:
            _stop(running.proc)
        _close_files(running)
    stop_error = _check_stopped(running.proc)
    if stop_error:
        rows.append(_Row(mode=mode, profile="stop", concurrency=0, errors=1, note=stop_error, failed=True))
    _emit_session(rows, running.log_path)
    return rows


def _run_stop_session(mode: str, conf: Path, port: int, concurrency: int) -> List[_Row]:
    """Start one server, load it, SIGTERM it, and check that the group exits.

    Args:
        mode: Layout name.
        conf: Layout YAML.
        port: TCP port from the layout YAML.
        concurrency: Bidi streams kept open across the signal.

    Returns:
        One row. Client errors after the signal are not counted.
    """
    running = _start(mode, conf)
    load_error: Optional[str] = None
    try:
        if not _wait_ready(port, running.proc, _READY_TIMEOUT_S):
            load_error = "server did not become ready"
        else:
            load_error = asyncio.run(_pump_and_stop(port, running.proc, concurrency))
    except Exception as exc:  # pylint: disable=broad-except
        load_error = _short(exc)
    finally:
        if running.proc.poll() is None:
            _stop(running.proc)
        _close_files(running)
    stop_error = _check_stopped(running.proc)
    note = "; ".join(part for part in (load_error, stop_error) if part)
    failed = bool(note)
    row = _Row(mode=mode,
               profile="stop_under_load",
               concurrency=concurrency,
               ok=0 if failed else 1,
               errors=1 if failed else 0,
               note=note,
               failed=failed)
    _emit_session([row], running.log_path)
    return [row]


def _parse_args(argv: Sequence[str]) -> argparse.Namespace:
    """Parse stress-run arguments.

    Args:
        argv: Arguments after the program name.

    Returns:
        The parsed namespace. ``--mode`` or ``--all`` is required.
    """
    parser = argparse.ArgumentParser(description="Stress hellostream process layouts")
    parser.add_argument("--all", action="store_true", help="run every layout")
    parser.add_argument("--mode",
                        choices=[name for name, _conf, _port, _unix in _MODES],
                        help="one layout name")
    parser.add_argument("--profile", choices=_PROFILES, help="default: bidi, or all three profiles with --all")
    parser.add_argument("--concurrency", type=int, help="one concurrency; default ladder is 1,16,64")
    parser.add_argument("--seconds", type=float, help=f"seconds per step (default {_DEFAULT_SECONDS:g})")
    parser.add_argument("--frames", type=int, default=_DEFAULT_FRAMES, help="data frames per stream")
    args = parser.parse_args(list(argv))
    if not args.all and not args.mode:
        parser.error("pass --mode or --all")
    if args.concurrency is not None and args.concurrency < 1:
        parser.error("--concurrency must be >= 1")
    if args.seconds is not None and args.seconds <= 0:
        parser.error("--seconds must be > 0")
    if args.frames < 1:
        parser.error("--frames must be >= 1")
    return args


def _profiles_of(args: argparse.Namespace) -> Tuple[str, ...]:
    """Return the profiles this invocation runs.

    Args:
        args: Parsed arguments.

    Returns:
        One profile when ``--profile`` is set. Otherwise bidi, or all three
        when ``--all`` is set.
    """
    if args.profile:
        return (args.profile, )
    if args.all:
        return _PROFILES
    return ("bidi", )


def _concurrencies_of(args: argparse.Namespace) -> Tuple[int, ...]:
    """Return the concurrency ladder for stream profiles.

    Args:
        args: Parsed arguments.

    Returns:
        The single ``--concurrency``, or ``1, 16, 64``.
    """
    if args.concurrency is not None:
        return (args.concurrency, )
    return _LADDER


def _run_mode(name: str, conf: Path, port: int, profiles: Sequence[str], concs: Sequence[int], seconds: float,
              frames: int, stop_concurrency: int) -> List[_Row]:
    """Run the selected profiles for one layout.

    Stream profiles share one server. ``stop_under_load`` starts its own,
    because it kills the process group.

    Args:
        name: Layout name.
        conf: Layout YAML.
        port: TCP port.
        profiles: Profiles to run, in order.
        concs: Concurrency ladder for stream profiles.
        seconds: Duration of each stream step.
        frames: Data frames per stream.
        stop_concurrency: Streams kept open for ``stop_under_load``.

    Returns:
        Every result row from this layout.
    """
    rows: List[_Row] = []
    stream_profiles = [profile for profile in profiles if profile != "stop_under_load"]
    if stream_profiles:
        levels = [(profile, concurrency, seconds, frames) for profile in stream_profiles for concurrency in concs]
        rows.extend(_run_stream_session(name, conf, port, levels))
    if "stop_under_load" in profiles:
        rows.extend(_run_stop_session(name, conf, port, stop_concurrency))
    return rows


def main(argv: Sequence[str] | None = None) -> int:
    """Run the selected layouts and return 0 only when every step passes.

    Args:
        argv: Arguments after the program name. ``None`` reads ``sys.argv``.

    Returns:
        0 when every step passes, 1 when any step fails.
    """
    _quiet_client_log()
    args = _parse_args(sys.argv[1:] if argv is None else argv)
    profiles = _profiles_of(args)
    concs = _concurrencies_of(args)
    seconds = _DEFAULT_SECONDS if args.seconds is None else args.seconds
    stop_concurrency = _STOP_LOAD_CONCURRENCY if args.concurrency is None else args.concurrency
    selected = [item for item in _MODES if args.all or item[0] == args.mode]
    failures: List[str] = []
    for name, conf, port, _unix in selected:
        print(f"== {name} {conf.name} :{port}", flush=True)
        rows = _run_mode(name, conf, port, profiles, concs, seconds, args.frames, stop_concurrency)
        for row in rows:
            if row.failed:
                failures.append(f"{row.mode}/{row.profile}/c={row.concurrency}")
    if failures:
        print("failed: " + ", ".join(failures), flush=True)
        return 1
    print("stress passed", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
