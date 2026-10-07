# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Process role stamps and parent-death binding.

:class:`trpc_sdk.abc.ProcessType` is the single source of truth for process
roles. Every serving process stamps it onto
``multiprocessing.current_process().type`` before plugins run, and both
:mod:`trpc_sdk.common` and :mod:`trpc_sdk.utils` read it back through
:func:`get_process_type`.
"""

from __future__ import annotations

import ctypes
import multiprocessing
import os
import signal
import sys
from typing import Any

from trpc_sdk.abc import ProcessType

# Linux ``<sys/prctl.h>``: deliver a signal to this process when the thread
# that forked it exits.
_PR_SET_PDEATHSIG = 1


def set_current_process_var(name: str, value: Any) -> None:
    """Set a custom attribute on the current process object.

    Args:
        name: Name of the attribute to set
        value: Value to assign to the attribute
    """
    current_process = multiprocessing.current_process()
    setattr(current_process, name, value)


def get_current_process_var(name: str, default: Any = None) -> Any:
    """Get a custom attribute from the current process object.

    Args:
        name: Name of the attribute to retrieve
        default: Default value if attribute doesn't exist

    Returns:
        The attribute value if exists, otherwise default value
    """
    current_process = multiprocessing.current_process()
    return getattr(current_process, name, default)


def get_process_type() -> ProcessType:
    """Get the type of the current process.

    Returns:
        ProcessType enum value indicating the process type
    """
    return get_current_process_var('type', default=ProcessType.UNKNOWN)


def get_process_name() -> str:
    """Get the name of the current process.

    Returns:
        String representing the process name
    """
    return get_current_process_var('name', default=ProcessType.UNKNOWN.name)


def is_worker_process() -> bool:
    """Check if current process is a worker process.

    Returns:
        True if process is WORKER type, False otherwise
    """
    return get_process_type() == ProcessType.WORKER


def is_task_process() -> bool:
    """Check if current process is a task process.

    Returns:
        True if process is TASK type, False otherwise
    """
    return get_process_type() == ProcessType.TASK


def is_timer_process() -> bool:
    """Check if current process is a timer process.

    Returns:
        True if process is TIMER type, False otherwise
    """
    return get_process_type() == ProcessType.TIMER


def bind_lifetime_to_parent() -> None:
    """Make this forked child receive SIGTERM when its parent dies.

    Linux only: sets ``PR_SET_PDEATHSIG`` to SIGTERM, then compares
    ``os.getppid()`` with the pid ``multiprocessing`` recorded for the parent.
    The comparison covers the window where the parent died before ``prctl``
    ran, in which case the signal would never come; the child exits at once
    instead.

    The kernel fires PDEATHSIG when the *thread* that forked this process
    exits, not the whole parent process. The framework forks only from the
    parent's main thread, so the two coincide; a child forked from another
    thread would be signalled when that thread ended.

    Other platforms rely on the parent sentinel watched by the worker's
    event loop instead. A no-op in a process ``multiprocessing`` did not fork.

    Raises:
        SystemExit: The parent had already exited before the binding took
            effect.
    """
    parent = multiprocessing.parent_process()
    if parent is None:
        return
    if sys.platform.startswith("linux"):
        libc = ctypes.CDLL(None, use_errno=True)
        libc.prctl(_PR_SET_PDEATHSIG, int(signal.SIGTERM), 0, 0, 0)
    if os.getppid() != parent.pid:
        sys.exit(1)
