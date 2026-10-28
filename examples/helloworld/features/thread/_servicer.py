# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Thread identity carried back on every example reply.

``MAIN_IDENT`` is captured at import. Entry scripts import this module on
the process main thread, before ``Server.serve`` starts serving threads, so
the value is the thread that accepts (merge) or does I/O (separate). Handlers
compare against it to show they are not running there.

Reply text is ``info:<index>:<count>:<ident>:<on_main>:<thread_name>``.
Hooks print one ``HOOK_START`` line per serving thread so a checker can see
threads that received no traffic.
"""

from __future__ import annotations

import asyncio
import re
import threading
from dataclasses import dataclass

from trpc_sdk.abc import TrpcContext
from trpc_sdk.runtime import current_handler_thread
from trpc_sdk.runtime import register_handler_thread_hook

from trpc_trpc_python_group_helloworld import pb
from trpc_trpc_python_group_helloworld import rpc

# Ident of the thread that imported this module. That is the server main
# thread when the import happens before any serving thread starts.
MAIN_IDENT: int = threading.get_ident()

_INFO_RE = re.compile(r"info:(-?\d+):(\d+):(\d+):([01]):(\S+)")


@dataclass(frozen=True)
class ThreadReport:
    """One serving thread, as reported by a handler or a start hook.

    Attributes:
        index: Position among serving threads, ``0 .. count - 1``.
        count: How many serving threads this process started.
        ident: ``threading.get_ident()`` of that thread.
        on_main: True when ``ident`` is the process main thread.
        name: OS thread name (``trpc-loop-<n>`` or ``trpc-handle-<n>``).
    """

    index: int
    count: int
    ident: int
    on_main: bool
    name: str


def thread_reply() -> str:
    """Describe the serving thread executing the caller.

    Returns:
        ``info:<index>:<count>:<ident>:<on_main>:<thread_name>``. ``index``
        is ``-1`` and ``count`` is ``0`` when called off a serving thread.
    """
    info = current_handler_thread()
    ident = threading.get_ident()
    index = info.index if info is not None else -1
    count = info.count if info is not None else 0
    on_main = int(ident == MAIN_IDENT)
    name = threading.current_thread().name
    return f"info:{index}:{count}:{ident}:{on_main}:{name}"


def parse_thread_report(text: str) -> ThreadReport:
    """Pull a :class:`ThreadReport` out of a reply or log line.

    Args:
        text: Any string that contains one ``info:...`` report.

    Returns:
        The parsed report.

    Raises:
        ValueError: ``text`` has no report.
    """
    match = _INFO_RE.search(text)
    if match is None:
        raise ValueError(f"no thread report in {text!r}")
    index, count, ident, on_main, name = match.groups()
    return ThreadReport(int(index), int(count), int(ident), on_main == "1", name)


def _on_start(info) -> None:
    """Print one line when a serving thread starts.

    Args:
        info: :class:`trpc_sdk.runtime.HandlerThreadInfo` for this thread.
    """
    ident = threading.get_ident()
    on_main = int(ident == MAIN_IDENT)
    name = threading.current_thread().name
    print(f"HOOK_START {info.index} {info.count} {on_main} {name} {ident}", flush=True)


def _on_stop(info) -> None:
    """Print one line when a serving thread stops.

    Args:
        info: :class:`trpc_sdk.runtime.HandlerThreadInfo` for this thread.
    """
    print(f"HOOK_STOP {info.index}", flush=True)


def install_hooks() -> None:
    """Register the start and stop prints. Call this before ``Server.serve``."""
    register_handler_thread_hook(on_start=_on_start, on_stop=_on_stop)


class ThreadGreeter(rpc.GreeterServicer):
    """Greeter whose ``SayHello`` reply is the serving thread that ran it.

    ``info:<seconds>`` sleeps that long before replying, so a pipelined
    connection can have several requests in flight. Any other message
    replies at once. The reply is always :func:`thread_reply`.
    """

    async def SayHello(self, ctx: TrpcContext, request: pb.HelloRequest) -> pb.HelloReply:
        """Reply with this thread's identity, after an optional sleep.

        Args:
            ctx: Request context. Unused; the identity comes from the thread.
            request: ``msg`` may be ``info:<seconds>`` to sleep first.

        Returns:
            ``HelloReply`` whose ``msg`` is :func:`thread_reply`.
        """
        del ctx
        msg = request.msg or ""
        if msg.startswith("info:"):
            raw = msg[5:]
            if raw:
                await asyncio.sleep(float(raw))
        return pb.HelloReply(msg=thread_reply())

    async def SayHi(self, ctx: TrpcContext, request: pb.HelloRequest) -> pb.HelloReply:
        """Reply ``hi`` without touching the thread report.

        Args:
            ctx: Request context. Unused.
            request: Ignored.

        Returns:
            ``HelloReply`` with ``msg`` ``hi``.
        """
        del ctx, request
        return pb.HelloReply(msg="hi")
