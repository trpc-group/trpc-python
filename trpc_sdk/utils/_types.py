# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Shared type aliases and records for :mod:`trpc.utils`.

Address, IP, and port aliases are used by :mod:`trpc.utils._address` and
:mod:`trpc.utils._network`. Timer records are filled before fork
(:func:`trpc.utils._timer_process.register_timer_process`) and read after
spawn by :class:`~trpc.utils._timer_process.TimerProcess`.

Timer class names are quoted under :data:`typing.TYPE_CHECKING` so this
module does not import :mod:`trpc.utils._timer` or
:mod:`trpc.utils._timer_process` at runtime.
"""

from __future__ import annotations

from dataclasses import dataclass
from multiprocessing import Process
from typing import TYPE_CHECKING
from typing import Tuple
from typing import Optional
from typing import Type
from typing import Union

from trpc_sdk.abc import TimerHook
from ._constants import TimerType

if TYPE_CHECKING:
    from ._timer import TimerExecutor
    from ._timer import TimerTask
    from ._timer_process import TimerProcess

# ``(host, port)`` as used by asyncio transports and listen endpoints.
# Unix sockets reuse the tuple with port unused.
AddrType = Tuple[str, int]

# Values accepted by :mod:`ipaddress` (dotted string, packed bytes, or int).
IpType = Union[int, str, bytes]
PortType = Union[int, str, bytes]


@dataclass
class TimerTaskInfo:
    """Registration record for one :class:`~trpc.utils._timer.TimerTask` subclass.

    Built by :func:`trpc.utils._timer.register_timer_task` at import time.
    :class:`~trpc.utils._timer.TimerExecutor` instantiates ``cls`` in the
    child process and copies the remaining fields onto that instance.

    Attributes:
        cls: Task subclass to construct; ``None`` only as the dataclass default.
        interval: Delay or period in seconds for ``ONCE`` / ``REPEAT_WITH_SECOND``.
        type: Schedule kind; see :class:`~trpc.utils._constants.TimerType`.
        name: Target process name such as ``worker:0``. Empty means every
            timer executor runs it.
        spec: Cron expression used only for :attr:`TimerType.REPEAT_WITH_CRON`.
    """

    cls: Optional[Type[TimerTask]] = None
    interval: int = 1
    type: TimerType = TimerType.REPEAT_WITH_SECOND
    name: str = ""
    spec: str = ""


@dataclass
class TimerProcessInfo:
    """Pre-fork record for one dedicated timer process.

    Constructed by :func:`trpc.utils._timer_process.register_timer_process`
    in the parent. :class:`~trpc.utils._timer_process.TimerProcess` reads
    the fields when the child is spawned; ``process_instance`` is filled
    only after ``Process.start``.

    ``timer_handle`` stores the :class:`~trpc.utils._timer_process.TimerProcess`
    *class*, not an instance, so tests and the engine can
    ``timer_handle(info)`` to construct the child.

    Attributes:
        name: Process name; empty is replaced with ``timer:<id>`` at spawn.
        executor: Executor to run in the child; ``None`` means construct
            a default :class:`~trpc.utils._timer.TimerExecutor`.
        hook: Optional :class:`~trpc.utils._timer_hook_abc.TimerHook` for
            before / after / signal callbacks.
        block: When True the child calls ``loop.run_forever()``.
        process_instance: Live :class:`multiprocessing.Process` after start.
        timer_handle: Class used to spawn the child (usually
            :class:`~trpc.utils._timer_process.TimerProcess`).
    """

    name: str = ""
    executor: Optional[TimerExecutor] = None
    hook: Optional[TimerHook] = None
    block: bool = True
    process_instance: Optional[Process] = None
    timer_handle: Optional[Type[TimerProcess]] = None

    @property
    def pid(self) -> int:
        """OS pid of :attr:`process_instance`.

        Raises:
            AttributeError: If ``process_instance`` has not been set yet.
                Callers historically read ``pid`` only after spawn.
        """
        return self.process_instance.pid
