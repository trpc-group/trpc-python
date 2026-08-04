# Tencent is pleased to support the open source community by making tRPC-Agent-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Agent-Python is licensed under Apache-2.0.

"""User-defined task description models.

This module defines the ``TaskInfo`` dataclass used to describe worker-process
handlers, initialization hooks, and optional C-task driver settings.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any
from typing import Callable
from typing import Dict
from typing import List
from typing import NamedTuple
from typing import Optional


@dataclass
class TaskInfo:
    """Describe one user-defined task worker.

    Attributes:
        name: Logical task name.
        handler: Callable that processes one batch of inputs and returns
            corresponding outputs.
        initializer: Optional hook invoked when the worker process starts.
        batch_size: Maximum number of items processed in one batch.
        thread_num: Number of worker threads used by the task.
        model_path: Optional path to a model artifact loaded by the task.
        model_info: Optional opaque model metadata.
        use_c_task: Whether the task should run through the C-task driver.
        c_task_driver_params: Optional parameters forwarded to the C-task
            driver.
        on_fork: Optional hook invoked after the worker process forks.
        task_num: Number of worker processes or task instances to start.
        pause_heartbeat: Whether heartbeats should be paused for this task.
        task_stream_handler: Optional streaming handler descriptor. When set,
            ``use_c_task`` is forced to ``False``.
    """

    name: str = ""
    handler: Callable[[List[Any]], List[Any]]
    initializer: Optional[Callable[[None], None]] = None
    batch_size: int = 0
    thread_num: int = 0
    model_path: str = ""
    model_info: Optional[Any] = None
    use_c_task: bool = False
    c_task_driver_params: Optional[Dict[str, Any]] = None
    on_fork: Optional[Callable[[None], None]] = None
    task_num: int = 1
    pause_heartbeat: bool = False
    task_stream_handler: Optional[NamedTuple] = None

    def __post_init__(self):
        """Disable the C-task driver when a stream handler is configured."""
        if self.task_stream_handler is not None:
            self.use_c_task = False
