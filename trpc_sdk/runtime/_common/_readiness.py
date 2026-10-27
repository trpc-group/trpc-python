# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Startup wait shared by the modes that fork serving children (separate, peer).

Before forking, the parent creates one shared-memory byte per child
(``multiprocessing.RawArray``). A child sets its byte once it serves; the
parent polls the array on its loop, before that loop runs forever, while
watching every child's sentinel. Startup is aborted with
:meth:`~trpc_sdk.runtime._common.WorkerExit._exit_now` when a child exits
(status 1), when SIGTERM arrives (status 0) or when ``ready_timeout`` passes
(status 1). Stopping, once serving, is the exit template's job, not this
module's.
"""

from __future__ import annotations

import asyncio
import multiprocessing
import signal
import threading
import time
from typing import Any
from typing import Callable
from typing import List
from typing import Optional

from trpc_sdk.log import logger
from trpc_sdk.utils import registered_timer_tasks
from .._constants import CHILD_EXIT_POLL_INTERVAL
from .._types import ChildProcessInfo


class _StartupFailure(Exception):
    """Startup cannot complete: a child died during startup."""


class _StartupInterrupted(Exception):
    """SIGTERM arrived while the children were still starting."""


def warn_unnamed_timers(owner: str, copies: int, unit: str) -> None:
    """Warn that timers without a name run once per serving process.

    Called before the fork, because it depends on what the application
    registered at import time.

    Args:
        owner: Process name, for the log line.
        copies: How many processes run each unnamed timer.
        unit: What those processes are called ("rpc tasks", "peer workers").
    """
    unnamed = [info.cls.__name__ for info in registered_timer_tasks() if not info.name]
    if unnamed and copies > 1:
        logger.warning("[%s] timers %s have no name, so each of the %s %s runs them; "
                       "name one 'worker:<index>' to run it once", owner, unnamed, copies, unit)


class ChildReadiness:
    """Parent side of the startup wait: ready flags, sentinels, ``ready_timeout``.

    A mixin listed before the worker class. It defines no ``__init__``, so the
    ``BaseWorker`` / ``WorkerExit`` constructor chain is untouched; its two
    fields are class-level defaults until assigned.

    The host provides ``loop``, ``name`` and ``_exit_now`` (from
    ``BaseWorker`` / ``WorkerExit``) and ``ready_timeout`` in seconds,
    where ``None`` or ``0`` means wait forever.

    The child side needs no mixin: it sets ``ready_flags[index] = 1`` once it
    serves, and its signals are handled through ``_controlled``.
    """

    # Declared by the host; listed for readers of this class. Annotations only:
    # a method defined here would shadow the host's, since this class comes
    # first in the MRO.
    loop: Optional[asyncio.AbstractEventLoop]
    name: str
    ready_timeout: Optional[float]
    _exit_now: Callable[[int], None]

    # One shared-memory byte per child, set to 1 by that child once it is
    # ready to serve. Created before any child is forked; each slot has one
    # writer, so no lock is needed.
    _ready_flags: Any = None
    # The future ``_wait_children_ready`` waits on; set only while it runs.
    _startup_waiter: Optional[asyncio.Future] = None

    def _create_ready_flags(self, count: int) -> None:
        """Allocate one ready flag per child; must run before the first fork.

        Args:
            count: Number of slots, one per child that will report readiness.
        """
        self._ready_flags = multiprocessing.RawArray("b", count)

    def _not_ready(self) -> List[int]:
        """Indexes of the slots that are still 0."""
        return [index for index in range(len(self._ready_flags)) if not self._ready_flags[index]]

    async def _poll_ready_flags(self) -> None:
        """Resolve the startup waiter once every ready flag is set."""
        while self._not_ready():
            await asyncio.sleep(CHILD_EXIT_POLL_INTERVAL)
        waiter = self._startup_waiter
        if waiter is not None and not waiter.done():
            waiter.set_result(None)

    def _wait_children_ready(self, children: List[ChildProcessInfo]) -> float:
        """Block until every ready flag is set, or abort startup.

        Runs on the parent's loop before it runs forever. Fails on any child
        in ``children`` exiting, a failed startup included, on SIGTERM, and when
        ``ready_timeout`` passes. SIGCHLD is not watched yet, so a child's
        death is seen through its sentinel. The SIGTERM handler installed here
        is temporary; the real one comes with ``_serve``.

        Args:
            children: Children whose death aborts startup. Usually every child,
                including those that set no flag (task processes).

        Returns:
            Seconds the wait took.

        Raises:
            SystemExit: Startup was aborted; status 0 for SIGTERM, 1 otherwise.
        """
        loop = self.loop
        waiter = loop.create_future()
        self._startup_waiter = waiter
        sentinels = []
        for child in children:
            sentinel = child.process.sentinel
            loop.add_reader(sentinel, self._on_child_died_during_startup, child)
            sentinels.append(sentinel)

        in_main_thread = threading.current_thread() is threading.main_thread()
        if in_main_thread:
            loop.add_signal_handler(signal.SIGTERM, self._on_sigterm_during_startup)

        began = time.monotonic()
        exit_code: Optional[int] = None
        poller = loop.create_task(self._poll_ready_flags())
        try:
            loop.run_until_complete(asyncio.wait_for(waiter, self.ready_timeout or None))
        except _StartupInterrupted:
            logger.warning("[%s] SIGTERM during startup; stopping the children", self.name)
            exit_code = 0
        except asyncio.TimeoutError:
            logger.error("children %s not ready within ready_timeout=%.0f ms (%s)", self._not_ready(),
                         (self.ready_timeout or 0) * 1000, self.name)
            exit_code = 1
        except _StartupFailure as err:
            logger.error("[%s] %s", self.name, err)
            exit_code = 1
        finally:
            poller.cancel()
            for sentinel in sentinels:
                loop.remove_reader(sentinel)
            if in_main_thread:
                loop.remove_signal_handler(signal.SIGTERM)
            self._startup_waiter = None
        if exit_code is not None:
            self._exit_now(exit_code)
        return time.monotonic() - began

    def _on_child_died_during_startup(self, child: ChildProcessInfo) -> None:
        """Event callback: ``child``'s sentinel became readable before startup completed.

        Args:
            child: The child that exited.
        """
        self.loop.remove_reader(child.process.sentinel)
        waiter = self._startup_waiter
        if waiter is not None and not waiter.done():
            waiter.set_exception(
                _StartupFailure(f"child {child.name} exited during startup (exit code {child.process.exitcode})"))

    def _on_sigterm_during_startup(self) -> None:
        """Event callback: SIGTERM arrived during the startup wait."""
        waiter = self._startup_waiter
        if waiter is not None and not waiter.done():
            waiter.set_exception(_StartupInterrupted())
