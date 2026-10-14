# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Timer tasks, their registration records, and per-worker executors.

Decorators run at import time in the parent. The executor instantiates
tasks in the child after fork, using that child's event loop.
"""

from __future__ import annotations

import asyncio
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
            timer executor runs it.
        spec: Cron expression used only for :attr:`TimerType.REPEAT_WITH_CRON`.
    """

    cls: Optional[Type[TimerTask]] = None
    interval: int = 1
    type: TimerType = TimerType.REPEAT_WITH_SECOND
    name: str = ""
    spec: str = ""


class TimerTask:
    """Base class for a scheduled timer callback.

    Business code subclasses this and overrides :meth:`handle`. Fields
    are filled either by the subclass or by
    :class:`TimerExecutor.add_register_task` from a :class:`TimerTaskInfo`
    record. ``name`` such as ``worker:0`` pins the task to one process;
    empty means every executor runs it.

    Attributes:
        interval: Delay or period in seconds.
        type: Schedule kind; see :class:`TimerType`.
        name: Target process name, or empty for all timer executors.
        spec: Cron expression for :attr:`TimerType.REPEAT_WITH_CRON`.
        loop: Event loop used by :meth:`start`; set by the executor.
    """

    def __init__(self) -> None:
        self.interval = 1
        self.type: TimerType = TimerType.REPEAT_WITH_SECOND
        self.name: str = ""
        self.spec: str = ""
        self.loop = None
        self.__timer_handle = None

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
        self.__timer_handle = self.loop.call_at(self.loop.time() + self.interval, self.call_next)

    def stop(self):
        """Cancel the pending ``call_at`` handle, if any."""
        if self.__timer_handle is not None:
            self.__timer_handle.cancel()
        self.__timer_handle = None

    def call_next(self):
        """Arm the next tick and spawn :meth:`handle` as a task.

        Used by :attr:`TimerType.REPEAT_WITH_SECOND`.
        """
        if self.__timer_handle is not None:
            self.__timer_handle.cancel()
        self.__timer_handle = self.loop.call_at(self.loop.time() + self.interval, self.call_next)
        self.loop.create_task(self.handle())


# Filled by :func:`register_timer_task` at import time; consumed in the
# child by :meth:`TimerExecutor.add_register_task`.
_TIMER_TASKS_INFO: List[TimerTaskInfo] = []


class TimerExecutor:
    """Runs registered timer tasks on one process's event loop.

    Each worker (or dedicated timer process) owns one executor. Tasks
    whose ``name`` is non-empty and differs from :attr:`name` are skipped.

    Attributes:
        loop: Event loop that owns the cron / ``call_later`` handles.
        name: Logical process name used to filter tasks, e.g. ``worker:0``.
    """

    __slots__ = ("loop", "name", "__timers", "__tasks")

    def __init__(self,
                 loop: Optional[asyncio.base_events.BaseEventLoop] = None,
                 name: str = "",
                 tasks: Optional[List[TimerTask]] = None) -> None:
        self.loop = loop
        self.name = name

        self.__timers: List[Union[aiocron.Cron, TimerTask]] = []
        self.__tasks: List[TimerTask] = tasks or []

    def add_register_task(self):
        """Instantiate every :class:`TimerTaskInfo` onto this executor.

        Called from :meth:`run` in the child, after ``self.loop`` is the
        child's loop.
        """
        global _TIMER_TASKS_INFO
        for task_info in _TIMER_TASKS_INFO:
            instance: TimerTask = task_info.cls()
            instance.interval = task_info.interval
            instance.type = task_info.type
            instance.name = task_info.name
            instance.spec = task_info.spec
            instance.loop = self.loop or get_or_create_event_loop()
            self.__tasks.append(instance)

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

    def __add_timer_after(self, timer_task: TimerTask):
        """Schedule :meth:`TimerTask.handle` once after ``interval`` seconds.

        Args:
            timer_task: Task with :attr:`TimerType.ONCE`.
        """

        def wrap_sync():
            """Bridge ``call_later`` (sync) to the async ``handle``."""
            self.loop.create_task(timer_task.handle())

        self.loop.call_later(timer_task.interval, wrap_sync)

    def __repeat_timer_with_cron(self, timer_task: TimerTask):
        """Start an aiocron job for ``timer_task.spec``.

        Args:
            timer_task: Task with a non-empty cron spec.
        """
        assert len(timer_task.spec) > 0
        spec = self.format_cron_spec(timer_task.spec)
        corn_obj = aiocron.crontab(spec, func=timer_task.handle, start=True, loop=self.loop)
        self.__timers.append(corn_obj)

    def __repeat_timer_with_second(self, timer_task: TimerTask):
        """Start the ``call_at`` loop on ``timer_task``.

        Args:
            timer_task: Task using :attr:`TimerType.REPEAT_WITH_SECOND`.
        """
        timer_task.start()
        self.__timers.append(timer_task)

    def run(self):
        """Materialize registered tasks and start matching schedules."""
        self.add_register_task()
        for task in self.__tasks:
            if len(task.name) > 0 and task.name != self.name:
                continue
            if task.type == TimerType.REPEAT_WITH_CRON:
                self.__repeat_timer_with_cron(task)
            elif task.type == TimerType.ONCE:
                self.__add_timer_after(task)
            else:
                self.__repeat_timer_with_second(task)

    def stop(self):
        """Cancel every cron object and :class:`TimerTask` started by :meth:`run`."""
        for cor in self.__timers:
            cor.stop()


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
                        spec: str = "") -> Callable[[TimerTask], None]:
    """Decorator that registers a :class:`TimerTask` subclass for later spawn.

    Records are stored in the parent at import time and instantiated by
    each :class:`TimerExecutor` in the child.

    Args:
        name: Process name that should run the task. Empty means every
            timer executor. ``worker:1`` is the second worker process.
        type: Schedule kind; defaults to repeating by seconds.
        interval: Seconds for ``ONCE`` / ``REPEAT_WITH_SECOND``.
        spec: Cron expression for ``REPEAT_WITH_CRON``.

    Returns:
        Class decorator that appends a :class:`TimerTaskInfo` and returns
        the class unchanged.

    Example:
        @register_timer_task(name="worker:1")
        class UserTimerTask(TimerTask):
            async def handle(self):
                ...
    """

    def decorator(cls: TimerTask) -> object:
        global _TIMER_TASKS_INFO
        _TIMER_TASKS_INFO.append(TimerTaskInfo(cls=cls, name=name, type=type, interval=interval, spec=spec))
        return cls

    return decorator
