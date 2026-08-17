# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""tRPC-Python server module.

A tRPC server is a process tree. The master process parses the YAML config,
builds one :class:`Service` per ``server.service[]`` entry, creates every IPC
pipe, and then forks the children that do the actual work. Which children exist
depends on ``server.mode``: ``dispatcher`` forks one dispatcher that owns the
listen sockets plus N workers that run handlers, while ``multi-listener`` forks
N executors that each listen for themselves.

The package is split by responsibility:

- :mod:`~trpc_sdk.server.option` — ``ServerOption`` (process model) and
  ``ServiceOption`` (one listen endpoint), both filled from YAML
- :mod:`~trpc_sdk.server.service` — ``Service``: method registry, filter chain,
  and the per-request handler path
- :mod:`~trpc_sdk.server.server` — ``Server``: the master process, its signal
  handling and graceful shutdown
- :mod:`~trpc_sdk.server.server_engine` — ``TrpcServerEngine``: builds the process
  topology, forks children, and restarts them
- :mod:`~trpc_sdk.server._child_process`, :mod:`~trpc_sdk.server._worker`,
  :mod:`~trpc_sdk.server._executor`, :mod:`~trpc_sdk.server._dispatcher`,
  :mod:`~trpc_sdk.server._task` — the child process implementations and the
  startup sequence they share
- :mod:`~trpc_sdk.server._constants`, :mod:`~trpc_sdk.server._features`,
  :mod:`~trpc_sdk.server._pipe`, :mod:`~trpc_sdk.server._process_info`,
  :mod:`~trpc_sdk.server._signals`, :mod:`~trpc_sdk.server._types` — shared defaults,
  runtime switches, IPC primitives and metadata used across processes
- :mod:`~trpc_sdk.server._healthy_checker` — master/child heartbeat monitoring

Application code normally reaches this package through :func:`trpc_sdk.new`, which
returns a :class:`Server`. Generated stub code uses :class:`Server`,
:class:`Method`, :class:`ServiceDesc` and :class:`StreamDesc` to register
handlers.
"""

from trpc_sdk.abc import Method
from trpc_sdk.abc import ServiceDesc
from trpc_sdk.abc import StreamDesc
from trpc_sdk.abc import TrpcContext

from ._constants import MODE_DISPATCHER
from ._constants import MODE_MULTI_LISTENER
from ._constants import NETWORK_HTTP
from ._constants import NETWORK_TCP
from ._constants import NETWORK_UDP
from ._dispatcher import DispatcherInfo
from ._dispatcher import DispatcherProcess
from ._executor import ExecutorProcess
from ._features import use_c_dispatcher
from ._features import use_c_task
from ._features import use_trpcio
from ._process_info import ProcessInfo
from ._process_info import ProcessTaskInfo
from ._signals import set_signal_handle
from ._task import CTaskProcess
from ._task import TaskInfo
from ._task import TaskProcess
from ._types import ServerMode
from ._worker import CWorkerProcess
from ._worker import WorkerInfo
from ._worker import WorkerProcess
from ._worker import current_worker
from ._option import ServerOption
from ._option import ServiceOption

__all__ = [
    # Handler-facing types, used by generated stub code
    "Method",
    "ServiceDesc",
    "StreamDesc",
    "TrpcContext",
    # Options
    "ServerOption",
    "ServiceOption",
    # Process model
    "CTaskProcess",
    "CWorkerProcess",
    "DispatcherInfo",
    "DispatcherProcess",
    "ExecutorProcess",
    "ProcessInfo",
    "ProcessTaskInfo",
    "ServerMode",
    "current_worker",
    "TaskInfo",
    "TaskProcess",
    "WorkerInfo",
    "WorkerProcess",
    # Config values shared with the YAML schema
    "MODE_DISPATCHER",
    "MODE_MULTI_LISTENER",
    "NETWORK_HTTP",
    "NETWORK_TCP",
    "NETWORK_UDP",
    # Lifecycle hooks
    "set_signal_handle",
    # Runtime feature switches
    "use_c_dispatcher",
    "use_c_task",
    "use_trpcio",
]
