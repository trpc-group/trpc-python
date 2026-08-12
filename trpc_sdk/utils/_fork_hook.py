# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Fork hook list"""

from __future__ import annotations

from enum import Enum
from enum import unique

from typing import Callable
from typing import Dict
from typing import List
from typing import Union

from ._multi_process_helper import ProcessType
from ._multi_process_helper import is_task_process
from ._multi_process_helper import is_worker_process

HookType = Callable[[], None]

_child_cbs: Dict[ProcessType, List[HookType]] = {}


def register_child_callback(cb: Union[HookType, List[HookType]], type: ProcessType):
    """Register callback in child"""
    global _child_cbs
    if type not in _child_cbs:
        _child_cbs[type] = []
    if isinstance(cb, list):
        _child_cbs[type].extend(cb)
    else:
        _child_cbs[type].append(cb)


def get_child_callback(type: ProcessType) -> List[Callable[[], None]]:
    """Get callback in child"""
    global _child_cbs
    return _child_cbs.get(type, [])


@unique
class ForkHookType(Enum):
    """The type of hook"""
    BEFORE_FORK_IN_PARENT = 1
    AFTER_FORK_IN_PARENT = 2
    AFTER_FORK_IN_CHILD = 3
    BEFORE_RUN_LOOP_IN_CHILD = 4


_fork_hooks: Dict[ForkHookType, List[HookType]] = {}


def _save_hooks(hook_type: ForkHookType, func: HookType):
    if hook_type not in _fork_hooks:
        _fork_hooks[hook_type] = []
    if func:
        _fork_hooks[hook_type].append(func)


def register_at_fork(before: HookType = None,
                     after_in_parent: HookType = None,
                     after_in_child: HookType = None,
                     before_loop_in_child: HookType = None):
    """Register hook at fork
    Must call before serving
    """
    _save_hooks(ForkHookType.BEFORE_FORK_IN_PARENT, before)
    _save_hooks(ForkHookType.AFTER_FORK_IN_PARENT, after_in_parent)
    _save_hooks(ForkHookType.AFTER_FORK_IN_CHILD, after_in_child)
    _save_hooks(ForkHookType.BEFORE_RUN_LOOP_IN_CHILD, before_loop_in_child)


def _execute_hook(hook_type: ForkHookType):
    """Execute hook"""
    hooks = _fork_hooks.get(hook_type, [])
    for hook in hooks:
        hook()


def execute_before():
    """Execute fork before"""
    _execute_hook(ForkHookType.BEFORE_FORK_IN_PARENT)


def execute_after_in_parent():
    """Execute fork int parent"""
    _execute_hook(ForkHookType.AFTER_FORK_IN_PARENT)


def execute_after_in_child():
    """Execute fork child"""
    _execute_hook(ForkHookType.AFTER_FORK_IN_CHILD)
    global _child_cbs
    if is_task_process():
        cbs = _child_cbs.get(ProcessType.TASK, [])
        for cb in cbs:
            cb()
    elif is_worker_process():
        cbs = _child_cbs.get(ProcessType.WORKER, [])
        for cb in cbs:
            cb()


def execute_before_loop_run_in_child():
    """Execute fork child"""
    _execute_hook(ForkHookType.BEFORE_RUN_LOOP_IN_CHILD)
