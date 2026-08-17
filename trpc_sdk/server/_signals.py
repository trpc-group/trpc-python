# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Business callbacks run when a server process receives SIGTERM.

Master and worker processes keep separate callback lists because they shut down
for different reasons and at different times: the master deregisters services
and then kills its children, while each worker only has to release what it
opened after fork. A callback registered here runs inside the signal-handling
path, so it must not block.

Callbacks are stored per process role and run in registration order.
"""

from typing import Callable
from typing import List
from typing import Optional

SigHandlerType = Callable[[], None]

_master_handles: List[SigHandlerType] = []
_worker_handles: List[SigHandlerType] = []


def set_signal_handle(master_handle: Optional[SigHandlerType] = None,
                      worker_handle: Optional[SigHandlerType] = None) -> None:
    """Register SIGTERM callbacks for the master and/or worker processes.

    Can be called more than once; every registered callback is kept and runs in
    registration order.

    Args:
        master_handle: Zero-argument callable run when the master exits.
        worker_handle: Zero-argument callable run when a worker exits.

    Raises:
        Exception: If a handle is not callable or does not take zero arguments.
    """
    if master_handle:
        if callable(master_handle) and master_handle.__code__.co_argcount == 0:
            _master_handles.append(master_handle)
        else:
            raise Exception("register signal master handle failed")

    if worker_handle:
        if callable(worker_handle) and worker_handle.__code__.co_argcount == 0:
            _worker_handles.append(worker_handle)
        else:
            raise Exception("register signal worker handle failed")


def run_server_signal_handle() -> None:
    """Run the master-process exit callbacks, in registration order."""
    for callback in _master_handles:
        if callable(callback):
            callback()


def run_worker_signal_handle() -> None:
    """Run the worker-process exit callbacks, in registration order."""
    for callback in _worker_handles:
        if callable(callback):
            callback()
