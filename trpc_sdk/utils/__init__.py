# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Utility functions"""

from ._factory import Factory
from ._fork_hook import register_at_fork
from ._fork_hook import execute_before
from ._fork_hook import execute_after_in_parent
from ._fork_hook import execute_after_in_child
from ._fork_hook import execute_before_loop_run_in_child
from ._multi_process_helper import ProcessType
from ._multi_process_helper import is_task_process
from ._multi_process_helper import is_worker_process
from ._multi_process_helper import get_process_name
from ._multi_process_helper import get_process_type
from ._multi_process_helper import set_current_process_var
from ._multi_process_helper import get_current_process_var
from ._singleton import singleton
from ._singleton import SingletonBase

__all__ = [
    "Factory",
    "register_at_fork",
    "execute_before",
    "execute_after_in_parent",
    "execute_after_in_child",
    "execute_before_loop_run_in_child",
    "ProcessType",
    "is_task_process",
    "is_worker_process",
    "get_process_type",
    "set_current_process_var",
    "get_current_process_var",
    "get_process_name",
    "singleton",
    "SingletonBase",
]
