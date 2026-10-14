# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Processes the worker forks: task children, and ``separate`` / ``peer`` mode.

Task children are the batch and inference processes a worker forks before it
has an event loop (trpc-cpp ``runtime/task``). ``separate`` and ``peer`` have
no trpc-cpp counterpart: it scales with threads in one process, while Python
threads share one GIL, so these modes fork serving processes instead. Both
wait for their children through the same readiness flags before serving.

:mod:`~trpc_sdk.runtime._worker` and
:class:`~trpc_sdk.runtime._merge_worker.MergeWorker` import this package
lazily, from inside functions.
:class:`BaseTaskProcess` subclasses
:class:`~trpc_sdk.runtime._worker.BaseWorker`, and this package's init loads
the peer worker, which loads the merge worker, so a module-level import in
the other direction would be a cycle.

The package is split by responsibility:

- :mod:`~trpc_sdk.runtime._process._task_process` -- the task process classes,
  the per-task records built before fork, and the socketpair mesh between
  workers and tasks
- :mod:`~trpc_sdk.runtime._process._separate_worker` -- the I/O worker of
  ``separate`` mode, which forwards frames to rpc task processes
- :mod:`~trpc_sdk.runtime._process._rpc_task` -- the rpc task process, where
  ``separate`` mode runs handlers
- :mod:`~trpc_sdk.runtime._process._peer_worker` -- worker-0 of ``peer``
  mode and the equal workers it forks
"""

from ._peer_worker import PeerMainWorker
from ._rpc_task import RpcTaskProcess
from ._separate_worker import SeparateWorker
from ._task_process import BaseTaskProcess
from ._task_process import TaskIpcMesh
from ._task_process import TaskProcess
from ._task_process import build_task_infos
from ._task_process import set_task_ipc_mesh
from ._task_process import task_ipc_mesh
from ._task_process import worker_producer_name

__all__ = [
    "BaseTaskProcess",
    "PeerMainWorker",
    "RpcTaskProcess",
    "SeparateWorker",
    "TaskIpcMesh",
    "TaskProcess",
    "build_task_infos",
    "set_task_ipc_mesh",
    "task_ipc_mesh",
    "worker_producer_name",
]
