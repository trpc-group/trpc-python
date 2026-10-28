# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Timer tasks, their registration records, and per-process / per-thread executors.

Decorators run at import time in the parent. The executor instantiates
tasks in the child after fork, using the event loop it was built for.

A task has one of two scopes:

* :attr:`TimerScope.PROCESS` (default): one copy per serving process,
  run by the executor built with :meth:`TimerExecutor.for_process`.
* :attr:`TimerScope.THREAD`: one copy per handler loop of a serving
  process, run by the executors built with :meth:`TimerExecutor.for_thread`.
  I/O-only loops run none. A process serving on its main loop alone has
  one handler loop, index 0.

Both scopes share the same scheduling code; an executor only differs in
which registrations it accepts and which loop it schedules on.
"""

from __future__ import annotations

import asyncio
import os
import threading
from dataclasses import dataclass
from enum import Enum
from enum import unique
from typing import Callable
from typing import List
from typing import Optional
from typing import Tuple
from typing import Type
from typing import Union

import aiocron

from ._loop_lib import get_or_create_event_loop


@unique
class TimerType(Enum):
    """How :class:`TimerExecutor` schedules one task.

    Attributes:
        ONCE: Run ``handle`` once after ``interval`` seconds.
        REPEAT_WITH_CRON: Repeat on an aiocron spec (``TimerTask.spec``).
        REPEAT_WITH_SECOND: Repeat every ``interval`` seconds via
            :meth:`TimerTask.start`.
    """

    ONCE = 0
    REPEAT_WITH_CRON = 1
    REPEAT_WITH_SECOND = 2


@unique
class TimerScope(Enum):
    """How many copies of one task a serving process runs.

    Attributes:
        PROCESS: One per process, on the process executor.
        THREAD: One per handler loop, on that loop's thread executor.
    """

    PROCESS = 0
    THREAD = 1


@dataclass
class TimerTaskInfo:
    """Registration record for one :class:`TimerTask` subclass.

    Built by :func:`register_timer_task` at import time.
    :class:`TimerExecutor` instantiates ``cls`` in the child process and
    copies the remaining fields onto that instance.

    Attributes:
        cls: Task subclass to construct; ``None`` only as the dataclass default.
        interval: Delay or period in seconds for ``ONCE`` / ``REPEAT_WITH_SECOND``.
        type: Schedule kind; see :class:`TimerType`.
        name: Target process name such as ``worker:0``. Empty means every
            process runs it.
        spec: Cron expression used only for :attr:`TimerType.REPEAT_WITH_CRON`.
        scope: Per process or per handler loop; see :class:`TimerScope`.
        thread: Handler loop index the task is pinned to; ``None`` means
            every handler loop. Only meaningful for :attr:`TimerScope.THREAD`.
    """

    cls: Optional[Type[TimerTask]] = None
    interval: int = 1
    type: TimerType = TimerType.REPEAT_WITH_SECOND
    name: str = ""
    spec: str = ""
    scope: TimerScope = TimerScope.PROCESS
    thread: Optional[int] = None


class TimerTask:
    """Base class for a scheduled timer callback.

    Business code subclasses this and overrides :meth:`handle`. Fields
    are filled by :class:`TimerExecutor` from a :class:`TimerTaskInfo`
    record. ``name`` such as ``worker:0`` pins the task to one process;
    empty means every process runs it.

    Attributes:
        interval: Delay or period in seconds.
        type: Schedule kind; see :class:`TimerType`.
        name: Target process name, or empty for every process.
        spec: Cron expression for :attr:`TimerType.REPEAT_WITH_CRON`.
        scope: Per process or per handler loop; see :class:`TimerScope`.
        thread: Handler loop index for :attr:`TimerScope.THREAD`, or ``None``.
        loop: Event loop used by :meth:`start`; set by the executor.
    """

    def __init__(self) -> None:
        self.interval = 1
        self.type: TimerType = TimerType.REPEAT_WITH_SECOND
        self.name: str = ""
        self.spec: str = ""
        self.scope: TimerScope = TimerScope.PROCESS
        self.thread: Optional[int] = None
        self.loop = None
        self._timer_handle: Optional[asyncio.TimerHandle] = None

    async def handle(self):
        """User-supplied coroutine. Subclasses must override."""
        raise NotImplementedError('Method not implemented!')

    def start(self):
        """Schedule the first :meth:`call_next` ``interval`` seconds from now.

        Cancels any previous handle first. Creates a loop via
        :func:`get_or_create_event_loop` when none is set.
        """
        self.stop()
        self.loop = self.loop or get_or_create_event_loop()
        self._timer_handle = self.loop.call_at(self.loop.time() + self.interval, self.call_next)

    def stop(self):
        """Cancel the pending ``call_at`` handle, if any."""
        if self._timer_handle is not None:
            self._timer_handle.cancel()
        self._timer_handle = None

    def call_next(self):
        """Arm the next tick and spawn :meth:`handle` as a task.

        Used by :attr:`TimerType.REPEAT_WITH_SECOND`.
        """
        if self._timer_handle is not None:
            self._timer_handle.cancel()
        self._timer_handle = self.loop.call_at(self.loop.time() + self.interval, self.call_next)
        self.loop.create_task(self.handle())


# Filled by :func:`register_timer_task` at import time, before fork;
# consumed in the child by :meth:`TimerExecutor.run`.
_TIMER_TASKS_INFO: List[TimerTaskInfo] = []


class TimerExecutor:
    """Runs the registered timer tasks of one scope on one event loop.

    A serving process owns one process executor (``thread_index is None``)
    and one thread executor per handler loop. An executor runs a
    registration only if its scope matches, its ``name`` is empty or equal
    to :attr:`name`, and, for a thread executor, its ``thread`` is ``None``
    or equal to :attr:`thread_index`.

    :meth:`run` must be called on the thread that owns :attr:`loop`, since
    ``call_later`` / ``call_at`` / aiocron are not thread-safe. :meth:`stop`
    may be called from any thread, and in a forked child that inherited the
    executor, where it only forgets the parent's handles.

    Attributes:
        loop: Event loop that owns the cron / ``call_later`` handles.
        name: Logical process name used to filter tasks, e.g. ``worker:0``.
        thread_index: Handler loop index of a thread executor; ``None`` for
            the process executor.
    """

    __slots__ = ("loop", "name", "thread_index", "_timers", "_handles", "_tasks", "_built", "_started",
                 "_owner_pid", "_owner_tid")

    def __init__(self,
                 loop: Optional[asyncio.AbstractEventLoop] = None,
                 name: str = "",
                 thread_index: Optional[int] = None) -> None:
        self.loop = loop
        self.name = name
        self.thread_index = thread_index

        self._timers: List[Union[aiocron.Cron, TimerTask]] = []
        # ``call_later`` handles of ONCE tasks not fired yet.
        self._handles: List[asyncio.TimerHandle] = []
        self._tasks: List[TimerTask] = []
        self._built = False
        self._started = False
        # Set by :meth:`run`; ``stop`` compares them to detect a forked copy
        # or a call from another thread.
        self._owner_pid: Optional[int] = None
        self._owner_tid: Optional[int] = None

    @classmethod
    def for_process(cls, loop: asyncio.AbstractEventLoop, name: str) -> TimerExecutor:
        """Build the executor of :attr:`TimerScope.PROCESS` tasks.

        Args:
            loop: Loop the tasks are scheduled on; that of handler loop 0.
            name: Process name, e.g. ``worker:0``.
        """
        return cls(loop=loop, name=name)

    @classmethod
    def for_thread(cls, loop: asyncio.AbstractEventLoop, name: str, thread_index: int) -> TimerExecutor:
        """Build the executor of :attr:`TimerScope.THREAD` tasks for one handler loop.

        Args:
            loop: The handler loop.
            name: Process name, e.g. ``worker:0``.
            thread_index: Position of the loop among the process's handler loops.
        """
        return cls(loop=loop, name=name, thread_index=thread_index)

    def _accepts(self, info: TimerTaskInfo) -> bool:
        """Whether this executor runs the task registered as ``info``.

        Args:
            info: A registration record.
        """
        if (info.scope is TimerScope.THREAD) != (self.thread_index is not None):
            return False
        if info.name and info.name != self.name:
            return False
        return info.thread is None or info.thread == self.thread_index

    def add_register_task(self):
        """Instantiate every registration this executor accepts; idempotent.

        Called by :meth:`run`, after ``self.loop`` is the child's loop.
        """
        self._build_tasks()

    def _build_tasks(self) -> None:
        """Instantiate the accepted registrations once, bound to :attr:`loop`."""
        if self._built:
            return
        self._built = True
        loop = self.loop or get_or_create_event_loop()
        for task_info in _TIMER_TASKS_INFO:
            if not self._accepts(task_info):
                continue
            instance: TimerTask = task_info.cls()
            instance.interval = task_info.interval
            instance.type = task_info.type
            instance.name = task_info.name
            instance.spec = task_info.spec
            instance.scope = task_info.scope
            instance.thread = task_info.thread
            instance.loop = loop
            self._tasks.append(instance)

    @staticmethod
    def format_cron_spec(spec: str) -> str:
        """Map tRPC's 6-field cron spec onto the aiocron version in use.

        tRPC stores ``[second minute hour day month weekday]``. aiocron
        2.x uses the same order; aiocron 1.x wants the second field last.
        Five-field (minute-level) specs are returned unchanged.

        Args:
            spec: Cron expression from :attr:`TimerTask.spec`.

        Returns:
            Spec string safe to pass to ``aiocron.crontab``.
        """
        spec_list = [each.strip() for each in spec.split(" ") if each.strip()]
        if len(spec_list) != 6:
            return " ".join(spec_list) if spec_list else spec
        try:
            from importlib.metadata import version
            major, minor, *_ = (int(x) for x in version("aiocron").split(".")[:2])
            if (major, minor) >= (2, 0):
                return " ".join(spec_list)
        except Exception:
            pass
        spec_list = spec_list[1:] + [spec_list[0]]
        return " ".join(spec_list)

    def _add_timer_after(self, timer_task: TimerTask):
        """Schedule :meth:`TimerTask.handle` once after ``interval`` seconds.

        The handle is kept so :meth:`stop` can cancel a task not fired yet.

        Args:
            timer_task: Task with :attr:`TimerType.ONCE`.
        """
        handle: Optional[asyncio.TimerHandle] = None

        def wrap_sync():
            """Bridge ``call_later`` (sync) to the async ``handle``."""
            if handle in self._handles:
                self._handles.remove(handle)
            self.loop.create_task(timer_task.handle())

        handle = self.loop.call_later(timer_task.interval, wrap_sync)
        self._handles.append(handle)

    def _repeat_timer_with_cron(self, timer_task: TimerTask):
        """Start an aiocron job for ``timer_task.spec``.

        Args:
            timer_task: Task with a non-empty cron spec.
        """
        assert len(timer_task.spec) > 0
        spec = self.format_cron_spec(timer_task.spec)
        cron_obj = aiocron.crontab(spec, func=timer_task.handle, start=True, loop=self.loop)
        self._timers.append(cron_obj)

    def _repeat_timer_with_second(self, timer_task: TimerTask):
        """Start the ``call_at`` loop on ``timer_task``.

        Args:
            timer_task: Task using :attr:`TimerType.REPEAT_WITH_SECOND`.
        """
        timer_task.start()
        self._timers.append(timer_task)

    def run(self):
        """Instantiate the accepted registrations and start their schedules; idempotent.

        Must run on the thread that owns :attr:`loop`, which is recorded
        together with the pid for :meth:`stop`.
        """
        if self._started:
            return
        self._started = True
        self._owner_pid = os.getpid()
        self._owner_tid = threading.get_ident()
        self.loop = self.loop or get_or_create_event_loop()
        self._build_tasks()
        for task in self._tasks:
            if task.type == TimerType.REPEAT_WITH_CRON:
                self._repeat_timer_with_cron(task)
            elif task.type == TimerType.ONCE:
                self._add_timer_after(task)
            else:
                self._repeat_timer_with_second(task)

    def stop(self):
        """Cancel every schedule started by :meth:`run`; safe from any thread.

        In a forked child the handles belong to the parent's loop, so they
        are only forgotten. From a thread other than the owner, the
        cancellation is posted to :attr:`loop`; a loop already closed has
        nothing left to cancel.
        """
        if self._owner_pid is None:
            return
        if self._owner_pid != os.getpid():
            self._timers.clear()
            self._handles.clear()
            return
        if self._owner_tid != threading.get_ident():
            try:
                self.loop.call_soon_threadsafe(self._stop_now)
            except RuntimeError:
                self._timers.clear()
                self._handles.clear()
            return
        self._stop_now()

    def _stop_now(self) -> None:
        """Cancel every cron job, ``call_at`` loop and pending ONCE handle; runs on the owner thread."""
        timers, self._timers = self._timers, []
        handles, self._handles = self._handles, []
        for timer in timers:
            timer.stop()
        for handle in handles:
            handle.cancel()


def registered_timer_tasks() -> Tuple[TimerTaskInfo, ...]:
    """Return every timer registered so far, as a read-only snapshot.

    Lets the server inspect registrations before it forks (for instance to
    warn that an unnamed timer will run once per rpc task) without reaching
    into the module-level list.
    """
    return tuple(_TIMER_TASKS_INFO)


def register_timer_task(name: str = "",
                        type: TimerType = TimerType.REPEAT_WITH_SECOND,
                        interval: int = 1,
                        spec: str = "",
                        scope: TimerScope = TimerScope.PROCESS,
                        thread: Optional[int] = None) -> Callable[[TimerTask], None]:
    """Decorator that registers a :class:`TimerTask` subclass for later spawn.

    Records are stored in the parent at import time and instantiated by
    each matching :class:`TimerExecutor` in the child.

    Args:
        name: Process name that should run the task. Empty means every
            process. ``worker:1`` is the second worker process.
        type: Schedule kind; defaults to repeating by seconds.
        interval: Seconds for ``ONCE`` / ``REPEAT_WITH_SECOND``.
        spec: Cron expression for ``REPEAT_WITH_CRON``.
        scope: :attr:`TimerScope.PROCESS` runs one copy per process;
            :attr:`TimerScope.THREAD` one per handler loop.
        thread: With :attr:`TimerScope.THREAD`, the handler loop index that
            should run the task; ``None`` means every handler loop.

    Returns:
        Class decorator that appends a :class:`TimerTaskInfo` and returns
        the class unchanged.

    Raises:
        ValueError: ``thread`` is given with :attr:`TimerScope.PROCESS`.

    Example:
        @register_timer_task(name="worker:1")
        class UserTimerTask(TimerTask):
            async def handle(self):
                ...

        @register_timer_task(scope=TimerScope.THREAD, thread=0)
        class FirstLoopTimerTask(TimerTask):
            async def handle(self):
                ...
    """
    if scope is TimerScope.PROCESS and thread is not None:
        raise ValueError("register_timer_task: 'thread' requires scope=TimerScope.THREAD")

    def decorator(cls: TimerTask) -> object:
        _TIMER_TASKS_INFO.append(
            TimerTaskInfo(cls=cls, name=name, type=type, interval=interval, spec=spec, scope=scope, thread=thread))
        return cls

    return decorator
