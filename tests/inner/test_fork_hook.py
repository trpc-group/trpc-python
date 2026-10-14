# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.
"""Test fork hook"""

import multiprocessing
import os

from trpc_sdk._inner.fork_hook import ProcessType
from trpc_sdk._inner.fork_hook import execute_after_in_child
from trpc_sdk._inner.fork_hook import execute_after_in_parent
from trpc_sdk._inner.fork_hook import execute_before
from trpc_sdk._inner.fork_hook import execute_before_loop_run_in_child
from trpc_sdk._inner.fork_hook import register_at_fork
from trpc_sdk._inner.fork_hook import register_child_callback
from trpc_sdk.utils import set_current_process_var


def _func(var: ProcessType = None):
    print(f"child pid: {os.getpid()}")
    if var:
        set_current_process_var('type', var)
    execute_after_in_child()
    execute_before_loop_run_in_child()


def _create_child_process(var: ProcessType = None):
    p = multiprocessing.Process(target=_func, args=(var, ))
    p.start()
    return p


def test_fork_hook():
    """Test fork hook"""
    test_val = multiprocessing.Value('i', 0)

    def _before():
        print(f"before pid: {os.getpid()}")
        nonlocal test_val
        with test_val.get_lock():
            test_val.value += 1

    def _after():
        print(f"after pid: {os.getpid()}")
        nonlocal test_val
        with test_val.get_lock():
            test_val.value += 1

    def _after_in_child():
        print(f"after_in_child pid: {os.getpid()}")
        nonlocal test_val
        with test_val.get_lock():
            test_val.value += 1

    def _before_loop_in_child():
        print(f"before_loop_in_child pid: {os.getpid()}")
        nonlocal test_val
        with test_val.get_lock():
            test_val.value += 1

    register_at_fork(before=_before,
                     after_in_parent=_after,
                     after_in_child=_after_in_child,
                     before_loop_in_child=_before_loop_in_child)

    print(f"master pid: {os.getpid()}")
    multiprocessing.set_start_method("fork", force=True)
    execute_before()
    p = _create_child_process()
    execute_after_in_parent()
    p.join()


def test_register_child_callback():
    """Test register_child_callback function."""
    test_val = multiprocessing.Value('i', 0)

    def _callback():
        nonlocal test_val
        with test_val.get_lock():
            test_val.value += 1

    register_child_callback(cb=_callback, type=ProcessType.DISPATCHER)
    register_child_callback(cb=_callback, type=ProcessType.WORKER)
    register_child_callback(cb=_callback, type=ProcessType.TASK)
    p = _create_child_process(ProcessType.DISPATCHER)
    p.join()
    p = _create_child_process(ProcessType.WORKER)
    p.join()
    p = _create_child_process(ProcessType.TASK)
    p.join()
    assert test_val.value == 3
