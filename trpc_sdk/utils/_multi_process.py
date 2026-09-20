# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Read and write custom attributes on ``multiprocessing.current_process()``.

:class:`~trpc_sdk.utils._constants.ProcessType` is defined in
:mod:`trpc_sdk.utils._constants`. After fork, child processes stamp ``type``
and ``name`` on the current process object; these helpers are the only
readers of that convention.
"""

from __future__ import annotations

import multiprocessing
from typing import Any

from ._constants import ProcessType


def set_current_process_var(name: str, value: Any) -> None:
    """Set a custom attribute on the current process object.

    Args:
        name: Attribute name, typically ``type`` or ``name``.
        value: Value to assign.
    """
    current_process = multiprocessing.current_process()
    setattr(current_process, name, value)


def get_current_process_var(name: str, default: Any = None) -> Any:
    """Get a custom attribute from the current process object.

    Args:
        name: Attribute name to read.
        default: Returned when the attribute has not been set.

    Returns:
        The attribute value, or ``default``.
    """
    current_process = multiprocessing.current_process()
    return getattr(current_process, name, default)


def get_process_type() -> ProcessType:
    """Return the :class:`ProcessType` stamped on this process after fork.

    Returns:
        Stored type, or :attr:`ProcessType.UNKNOWN` in the parent / unset case.
    """
    return get_current_process_var('type', default=ProcessType.UNKNOWN)


def get_process_name() -> str:
    """Return the logical process name stamped after fork.

    Returns:
        Stored name, or ``UNKNOWN`` when unset.
    """
    return get_current_process_var('name', default=ProcessType.UNKNOWN.name)


def is_worker_process() -> bool:
    """Return whether this process is a request-handling worker.

    Returns:
        True when :func:`get_process_type` is :attr:`ProcessType.WORKER`.
    """
    return get_process_type() == ProcessType.WORKER


def is_dispatcher_process() -> bool:
    """Return whether this process is a dispatcher.

    Returns:
        True when :func:`get_process_type` is :attr:`ProcessType.DISPATCHER`.
    """
    return get_process_type() == ProcessType.DISPATCHER


def is_task_process() -> bool:
    """Return whether this process is a dedicated task process.

    Returns:
        True when :func:`get_process_type` is :attr:`ProcessType.TASK`.
    """
    return get_process_type() == ProcessType.TASK


def is_timer_process() -> bool:
    """Return whether this process is a timer process.

    Returns:
        True when :func:`get_process_type` is :attr:`ProcessType.TIMER`.
    """
    return get_process_type() == ProcessType.TIMER
