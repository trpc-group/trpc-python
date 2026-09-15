# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Dedicated timer processes and their registration records.

Decorators and :func:`register_timer_process` run in the parent at
import time. The engine reads :func:`get_register_timer_process` when
spawning children. Each child constructs its loop with
:data:`asynclib` after fork, then runs a :class:`TimerExecutor`.
"""

from __future__ import annotations

import asyncio
import signal
import sys
import threading
from multiprocessing import Process
from typing import Dict
from typing import Optional

from trpc_sdk.abc import TimerHook

from ._constants import ProcessType
from ._loop_lib import asynclib
from ._timer import TimerExecutor
from ._types import TimerProcessInfo


class TimerProcess(Process):
    """Child process whose only job is to run a :class:`TimerExecutor`.

    Built from a :class:`TimerProcessInfo` record registered in the
    parent. The child's loop is created with :data:`asynclib` (uvloop
    when enabled) after fork.

    Attributes:
        name: Process name; default ``timer:<id>`` when the record is empty.
        block: When True, ``run`` calls ``loop.run_forever()``.
    """

    __slots__ = ("block", "name", "__hook", "__executor", "__loop")
    __pid: int = -1

    def __init__(self, info: TimerProcessInfo):
        super().__init__()
        self.type = ProcessType.TIMER
        if len(info.name) == 0:
            info.name = f"timer:{self.gen_pid()}"
        self.name = info.name
        self.block = info.block
        self.__hook = info.hook
        self.__executor = info.executor or TimerExecutor(name=self.name)
        self.__loop = None

    @classmethod
    def gen_pid(cls) -> int:
        """Return the next process-local timer id (not an OS pid).

        Returns:
            Monotonic integer starting at 0 for this class.
        """
        cls.__pid += 1
        return cls.__pid

    def run(self):
        """Child entry: hook, loop, executor, then block on SIGTERM."""
        if self.__hook:
            self.__hook.before()
        self.__loop = asynclib.new_event_loop()
        asyncio.set_event_loop(self.__loop)
        self.__executor.loop = self.__loop
        self.__executor.run()

        def handle_kill_signal():
            if self.__hook:
                self.__hook.signal_handle()
            for task in asyncio.all_tasks(self.__loop):
                task.cancel()
            sys.exit()

        try:
            if threading.current_thread() is threading.main_thread():
                self.__loop.add_signal_handler(signal.SIGTERM, handle_kill_signal)
            if self.block:
                self.__loop.run_forever()
        except (SystemExit, KeyboardInterrupt):
            if self.__hook:
                self.__hook.after()
            self.__executor.stop()
            self.__loop.stop()


# Filled by :func:`register_timer_process` in the parent; the engine
# reads it when spawning dedicated timer children.
_TIMER_PROCESS_INFO_MAP: Dict[str, TimerProcessInfo] = {}


def get_register_timer_process() -> Dict[str, TimerProcessInfo]:
    """Return the map of registered timer-process records.

    Returns:
        Name to :class:`TimerProcessInfo`. The same dict is mutated by
        :func:`register_timer_process`; callers must not assume a copy.
    """
    global _TIMER_PROCESS_INFO_MAP
    return _TIMER_PROCESS_INFO_MAP


def register_timer_process(name: str = "",
                           executor: Optional[TimerExecutor] = None,
                           hook: Optional[TimerHook] = None,
                           block: bool = True) -> None:
    """Register a dedicated timer process to be spawned by the engine.

    Duplicate ``name`` values raise :class:`ValueError`. An empty name
    becomes ``timer:<id>`` using :meth:`TimerProcess.gen_pid`.

    Args:
        name: Process name. Empty is replaced with ``timer:<id>``.
        executor: Executor to run in the child; ``None`` uses a default.
        hook: Optional lifecycle hook.
        block: Passed through to :class:`TimerProcess`.

    Raises:
        ValueError: If ``name`` was already registered.
    """
    global _TIMER_PROCESS_INFO_MAP
    if len(name) == 0:
        name = f"timer:{TimerProcess.gen_pid()}"
    if name in _TIMER_PROCESS_INFO_MAP:
        raise ValueError
    _TIMER_PROCESS_INFO_MAP[name] = TimerProcessInfo(
        name=name,
        executor=executor,
        hook=hook,
        block=block,
        timer_handle=TimerProcess,
    )
