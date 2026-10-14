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

Callbacks are stored per process role and run in registration order. One
callback raising is logged and does not prevent the rest from running.
"""

import inspect
from typing import Any
from typing import Callable
from typing import List
from typing import Optional

from trpc_sdk.log import logger

SigHandlerType = Callable[[], None]

_master_handles: List[SigHandlerType] = []
_worker_handles: List[SigHandlerType] = []


def _accepts_no_args(handle: Any) -> bool:
    """Tell whether ``handle`` can be called with no arguments.

    Accepts bound methods, ``functools.partial`` objects, and functions whose
    parameters all have defaults, which a ``__code__.co_argcount`` check
    would wrongly reject or could not inspect at all.
    """
    if not callable(handle):
        return False
    try:
        inspect.signature(handle).bind()
    except TypeError:
        return False
    except ValueError:
        # Some builtins expose no signature; let the call itself decide.
        return True
    return True


def set_signal_handle(master_handle: Optional[SigHandlerType] = None,
                      worker_handle: Optional[SigHandlerType] = None) -> None:
    """Register SIGTERM callbacks for the master and/or worker processes.

    Can be called more than once; every registered callback is kept and runs in
    registration order.

    Args:
        master_handle: Callable run with no arguments when the master exits.
        worker_handle: Callable run with no arguments when a worker exits.

    Raises:
        Exception: If a handle is not callable or cannot be called without
            arguments.
    """
    if master_handle:
        if _accepts_no_args(master_handle):
            _master_handles.append(master_handle)
        else:
            raise Exception("register signal master handle failed")

    if worker_handle:
        if _accepts_no_args(worker_handle):
            _worker_handles.append(worker_handle)
        else:
            raise Exception("register signal worker handle failed")


def _run_handles(handles: List[SigHandlerType], kind: str) -> None:
    for callback in handles:
        if not callable(callback):
            continue
        try:
            callback()
        except Exception:  # pylint: disable=broad-except
            logger.error("%s signal handle %r failed", kind, callback, exc_info=True)


def run_server_signal_handle() -> None:
    """Run the master-process exit callbacks, in registration order."""
    _run_handles(_master_handles, "master")


def run_worker_signal_handle() -> None:
    """Run the worker-process exit callbacks, in registration order."""
    _run_handles(_worker_handles, "worker")
