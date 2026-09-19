# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

from __future__ import annotations

import multiprocessing

from enum import IntEnum
from enum import unique
from typing import Any


@unique
class ProcessType(IntEnum):
    """Enumeration of different process types in the system.
    
    Attributes:
        WORKER: General worker process for task execution
        TASK: Process dedicated to specific task execution
        TIMER: Process handling timed operations
        UNKNOWN: Default type for unclassified processes
    """
    UNKNOWN = 0
    WORKER = 1
    TASK = 2
    TIMER = 3


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
