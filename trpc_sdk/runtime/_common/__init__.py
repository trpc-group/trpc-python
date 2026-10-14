# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Services every serving process shares, after trpc-cpp ``runtime/common``.

Heartbeats, SIGTERM callbacks, process metadata and the exit template do not
depend on the mode or thread model: whichever worker class a process runs, it
uses them the same way. The startup wait is the exception. Only separate and
peer, the modes that fork serving children, use it. Merge does not.

The package is split by responsibility:

- :mod:`~trpc_sdk.runtime._common._healthy_checker` -- heartbeat pipes
  between the worker and the task processes it forks (trpc-cpp
  ``common/heartbeat``)
- :mod:`~trpc_sdk.runtime._common._signals` -- business callbacks run on
  SIGTERM
- :mod:`~trpc_sdk.runtime._common._process_info` -- listen addresses,
  handlers and IPC metadata shared by server processes
- :mod:`~trpc_sdk.runtime._common._readiness` -- ready flags and the
  startup wait shared by separate and peer mode
- :mod:`~trpc_sdk.runtime._common._shutdown` -- the exit template every
  serving process shares; :class:`~trpc_sdk.runtime._worker.BaseWorker`
  inherits it
"""

from ._healthy_checker import close_heartbeat_pipes
from ._healthy_checker import init_healthy_checker
from ._healthy_checker import register_timeout_callback
from ._healthy_checker import start_heartbeat_monitor
from ._process_info import ProcessInfo
from ._process_info import ProcessTaskInfo
from ._readiness import ChildReadiness
from ._readiness import warn_unnamed_timers
from ._shutdown import ExitTemplate
from ._signals import run_server_signal_handle
from ._signals import run_worker_signal_handle
from ._signals import set_signal_handle

__all__ = [
    "ChildReadiness",
    "ExitTemplate",
    "ProcessInfo",
    "ProcessTaskInfo",
    "close_heartbeat_pipes",
    "init_healthy_checker",
    "register_timeout_callback",
    "run_server_signal_handle",
    "run_worker_signal_handle",
    "set_signal_handle",
    "start_heartbeat_monitor",
    "warn_unnamed_timers",
]
