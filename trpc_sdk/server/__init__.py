# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""tRPC-Python server module.

There are two kinds of process. The **worker** is the process the application
started: it parses the YAML config, builds one :class:`Service` per
``server.service[]`` entry, and then binds the listen addresses and serves. A
**task** is a child the worker forks before it has an event loop, to run batch
or inference handlers off an IPC pipe. There is no master process; the worker
is not forked by anything.

``server.mode`` picks what the worker does with a request. The public values
are ``merge`` and ``separate``:

- ``merge`` (default) — one process accepts and runs handlers, spreading
  connections over ``worker_thread_num`` event-loop threads. A request never
  crosses a process boundary. Threads share one GIL, so the parallelism is
  real only for I/O waits and C extensions that release it.
- ``separate`` — the worker only does I/O and forwards frames to
  ``rpc_task_num`` rpc task processes, which is where handlers run. This is
  what a CPU-bound service needs; ``merge`` will not scale it across cores.

The package is split by responsibility:

- :mod:`~trpc_sdk.server._option` — ``ServerOption`` (process model) and
  ``ServiceOption`` (one listen endpoint), both filled from YAML
- :mod:`~trpc_sdk.server._cython` — ``Service``: method registry, filter chain,
  the per-request handler path, and :func:`create_service`
- :mod:`~trpc_sdk.server._registry` — ``ServiceRegistrar`` /
  ``start_service_registry``: per-service naming-service register /
  heartbeat / deregister on a ``RegistryThread`` with its own event loop
- :mod:`~trpc_sdk.server._server` — ``Server``: config, service table,
  :func:`create_server`, and the startup order that forks children before
  creating a loop
- :mod:`~trpc_sdk.server._modes`, :mod:`~trpc_sdk.server._compat` — internal
  mode lookup, and translation of retired YAML values during the migration
  window
- :mod:`~trpc_sdk.server._worker` — ``BaseWorker``: the lifecycle template
  every serving process follows, and the fork and supervision of children
- :mod:`~trpc_sdk.server._merge_worker`, :mod:`~trpc_sdk.server._acceptor`,
  :mod:`~trpc_sdk.server._loop_thread` — merge mode
- :mod:`~trpc_sdk.server._separate_worker` — separate mode and its rpc tasks
- :mod:`~trpc_sdk.server._task` — task children and the pre-fork socketpair
  mesh
- :mod:`~trpc_sdk.server._constants`,
  :mod:`~trpc_sdk.server._runtime_switches`,
  :mod:`~trpc_sdk.server._process_info`, :mod:`~trpc_sdk.server._signals`,
  :mod:`~trpc_sdk.server._types` — shared defaults, runtime switches, and
  metadata used across processes. The socketpair primitives those tables hold
  live in :mod:`trpc_sdk.ipc`
- :mod:`~trpc_sdk.server._healthy_checker` — worker/child heartbeat monitoring

Application code normally reaches this package through :func:`create_server`,
which returns a :class:`Server`. Generated stub code uses :class:`Server`,
:class:`Method`, :class:`ServiceDesc` and :class:`StreamDesc` to register
handlers.
"""

from trpc_sdk.abc import Method
from trpc_sdk.abc import ServiceDesc
from trpc_sdk.abc import StreamDesc
from trpc_sdk.abc import TrpcContext

from . import _engine as _engine_module
from ._constants import MODE_MERGE
from ._constants import MODE_SEPARATE
from ._constants import Network
from ._engine import TrpcServerEngine
from ._merge_worker import MergeWorker
from ._option import ServerOption
from ._option import ServiceOption
from ._process_info import ProcessInfo
from ._process_info import ProcessTaskInfo
from ._runtime_switches import use_c_task
from ._separate_worker import RpcTaskProcess
from ._separate_worker import SeparateWorker
from ._server import Server
from ._server import create_server
from ._cython import Service
from ._cython import create_service
from ._signals import set_signal_handle
from ._task import BaseTaskProcess
from ._task import TaskProcess
from ._types import TaskInfo
from ._types import WorkerInfo
from ._worker import BaseWorker
from ._worker import current_worker

# Imported last: registering the heartbeat plugin requires the rest of the
# package to be importable.
from ._healthy_checker import register_timeout_callback

__all__ = [
    # Entry points
    "Server",
    "ServerOption",
    "Service",
    "ServiceOption",
    "create_server",
    "create_service",
    # Handler-facing types, used by generated stub code
    "TrpcContext",
    "Method",
    "ServiceDesc",
    "StreamDesc",
    # Process model
    "MergeWorker",
    "ProcessInfo",
    "ProcessTaskInfo",
    "RpcTaskProcess",
    "SeparateWorker",
    "current_worker",
    "TaskInfo",
    "TaskProcess",
    "WorkerInfo",
    "BaseTaskProcess",
    "BaseWorker",
    # Config values shared with the YAML schema
    "MODE_MERGE",
    "MODE_SEPARATE",
    "NETWORK_HTTP",
    "NETWORK_TCP",
    "NETWORK_UDP",
    "Network",
    # Lifecycle hooks
    "register_timeout_callback",
    "set_signal_handle",
    # Runtime feature switches
    "use_c_task",
]
