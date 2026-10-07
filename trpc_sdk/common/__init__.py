# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.
"""Common functions"""

from ._loop import get_or_create_event_loop
from ._compatible import wait_for_by_loop
from ._compatible import sleep_by_loop
from ._compatible import create_object_with_loop
from ._compatible import TransportSocket
from ._maxprocs import get_max_procs
from ._process_hook import register_at_fork
from ._process_hook import register_child_callback
from ._process_hook import execute_before
from ._process_hook import execute_after_in_parent
from ._process_hook import execute_after_in_child
from ._process_hook import execute_before_loop_run_in_child
from ._process_helper import ProcessType
from ._process_helper import bind_lifetime_to_parent
from ._process_helper import is_task_process
from ._process_helper import is_timer_process
from ._process_helper import is_worker_process
from ._process_helper import get_process_name
from ._process_helper import get_process_type
from ._process_helper import set_current_process_var
from ._process_helper import get_current_process_var

__all__ = [
    "get_or_create_event_loop",
    "wait_for_by_loop",
    "sleep_by_loop",
    "create_object_with_loop",
    "TransportSocket",
    "get_max_procs",
    "register_at_fork",
    "register_child_callback",
    "execute_before",
    "execute_after_in_parent",
    "execute_after_in_child",
    "execute_before_loop_run_in_child",
    "ProcessType",
    "bind_lifetime_to_parent",
    "is_task_process",
    "is_timer_process",
    "is_worker_process",
    "get_process_name",
    "get_process_type",
    "set_current_process_var",
    "get_current_process_var",
]
