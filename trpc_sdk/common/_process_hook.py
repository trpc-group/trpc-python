# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Fork hook list"""

from __future__ import annotations

from enum import Enum
from enum import unique

from collections import defaultdict

from typing import Callable
from typing import List
from typing import Optional
from typing import Union

from ._process_helper import ProcessType
from ._process_helper import is_task_process
from ._process_helper import is_worker_process

HookType = Callable[[], None]

_child_cbs: defaultdict[ProcessType, List[HookType]] = defaultdict(list)


def register_child_callback(cb: Union[HookType, List[HookType]], type: ProcessType):
    """Register callback in child
    
    Args:
        cb: The callback to register
        type: The type of process to register the callback for
    """
    global _child_cbs
    if type not in _child_cbs:
        _child_cbs[type] = []
    if isinstance(cb, list):
        _child_cbs[type].extend(cb)
    else:
        _child_cbs[type].append(cb)


def get_child_callback(type: ProcessType) -> List[Callable[[], None]]:
    """Get callback in child
    
    Args:
        type: The type of process to get the callback for
    Returns:
        The callback list
    """
    global _child_cbs
    return _child_cbs.get(type, [])


@unique
class ProcessHookType(Enum):
    """The type of hook
    
    Attributes:
        BEFORE_FORK_IN_PARENT: The hook before fork in parent
        AFTER_FORK_IN_PARENT: The hook after fork in parent
        AFTER_FORK_IN_CHILD: The hook after fork in child
    """
    BEFORE_FORK_IN_PARENT = 1
    AFTER_FORK_IN_PARENT = 2
    AFTER_FORK_IN_CHILD = 3


_process_hooks: defaultdict[ProcessHookType, List[HookType]] = defaultdict(list)


def _save_process_hooks(hook_type: ProcessHookType, hook: Optional[HookType] = None):
    """Save hook
    
    Args:
        hook_type: The type of hook
        hook: The hook to save, default is None
    """
    if hook:
        _process_hooks[hook_type].append(hook)


def register_at_fork(before: Optional[HookType] = None,
                     after_in_parent: Optional[HookType] = None,
                     after_in_child: Optional[HookType] = None):
    """Register hook at fork
    
    Args:
        before: The callback before fork in parent
        after_in_parent: The callback after fork in parent
        after_in_child: The callback after fork in child
    """
    _save_process_hooks(ProcessHookType.BEFORE_FORK_IN_PARENT, before)
    _save_process_hooks(ProcessHookType.AFTER_FORK_IN_PARENT, after_in_parent)
    _save_process_hooks(ProcessHookType.AFTER_FORK_IN_CHILD, after_in_child)


def _execute_process_hook(hook_type: ProcessHookType):
    """Execute hook
    
    Args:
        hook_type: The type of hook
    """
    hooks = _process_hooks.get(hook_type, [])
    for hook in hooks:
        hook()


def execute_before():
    """Execute process before in parent
    
    Must call before process in parent
    """
    _execute_process_hook(ProcessHookType.BEFORE_FORK_IN_PARENT)


def execute_after_in_parent():
    """Execute process after in parent
    
    Must call after fork in parent
    """
    _execute_process_hook(ProcessHookType.AFTER_FORK_IN_PARENT)


def execute_after_in_child():
    """Execute fork after in child
    
    Must call after fork in child
    """
    _execute_process_hook(ProcessHookType.AFTER_FORK_IN_CHILD)
    global _child_cbs
    if is_task_process():
        cbs = _child_cbs.get(ProcessType.TASK, [])
        for cb in cbs:
            cb()
    elif is_worker_process():
        cbs = _child_cbs.get(ProcessType.WORKER, [])
        for cb in cbs:
            cb()


_before_loop_run_in_child_hooks: List[HookType] = []


def register_before_loop_run_in_child(hook: HookType) -> None:
    """Register a hook that runs in the child after plugins, before the loop.

    Args:
        hook: Zero-argument callback invoked by
            :func:`execute_before_loop_run_in_child`.
    """
    _before_loop_run_in_child_hooks.append(hook)


def execute_before_loop_run_in_child() -> None:
    """Run child hooks after plugin init and before ``loop.run_forever()``.

    Must be called in the child after ``_init_plugins()``. With no
    registrations the hook list is empty and this is a no-op.
    """
    for hook in _before_loop_run_in_child_hooks:
        hook()
