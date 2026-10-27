# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""tRPC-Python runtime: the process and thread model behind a server.

There are two kinds of process. The **worker** is the process the application
started: :meth:`~trpc_sdk.server.Server.serve` builds it once the services
exist, and it then binds the listen addresses and serves. A **task** is a
child the worker forks before it has an event loop, to run batch or inference
handlers off an IPC pipe. There is no master process; the worker is not forked
by anything.

``ServerOption.mode`` picks what the worker does with a request. It is
translated from YAML ``global.processmodel`` and is one of ``merge``,
``separate`` and ``peer``:

- ``merge`` (default, ``single_process``) — one process accepts and runs
  handlers. Its thread model is ``io_thread_num`` / ``handle_thread_num``
  (YAML ``io_num`` / ``handle_num``): one event loop by default,
  ``io_thread_num`` serving loops that each read and handle, or, with
  ``io_handle_type: separate``, I/O loops that hand each request to one of
  ``handle_thread_num`` handle loops. A request never crosses a process
  boundary. Threads share one GIL, so the parallelism is real only for I/O
  waits and C extensions that release it.
- ``separate`` (``multi_process`` with ``io_handle_type: separate``) — the worker only does I/O and forwards frames to
  ``worker_num`` rpc task processes, which is where handlers run. This is
  what a CPU-bound service needs; ``merge`` will not scale it across cores.
  Only tRPC-over-TCP addresses pass through the worker; UDP, HTTP and
  user-protocol addresses are bound by every rpc task with ``SO_REUSEPORT``.
- ``peer`` (``multi_process`` with ``io_handle_type: merge``) — the worker forks ``worker_num - 1`` equal workers; every one,
  itself included, binds every address with ``SO_REUSEPORT`` and serves it
  like a single-loop ``merge`` worker. Nothing is forwarded between
  processes; the kernel spreads connections. Tasks are not supported.

The package is laid out after trpc-cpp's ``trpc/runtime``. The top level
holds what trpc-cpp keeps in ``runtime.h`` and the ``*_runtime`` files: mode
selection, the lifecycle every serving process follows, and one worker per
mode. Subpackages hold the pieces those workers are built from:

- :mod:`~trpc_sdk.runtime._modes` — internal mode lookup and
  :func:`create_worker` (trpc-cpp ``runtime.h`` / ``init_runtime.h``)
- :mod:`~trpc_sdk.runtime._worker` — ``BaseWorker``: the lifecycle template
  every serving process follows, and the fork and supervision of children
- :mod:`~trpc_sdk.runtime._merge_worker` — merge mode (trpc-cpp
  ``merge_runtime``); every mode listens through the services' transports
  (:mod:`trpc_sdk.transport._server`), dispatching on ``classify_address``
- :mod:`~trpc_sdk.runtime._threadmodel` — the :class:`ThreadModel` every
  server transport reads in ``listen``, loop threads, the handle-thread
  pool, per-serving-loop hooks, and
  :class:`~trpc_sdk.runtime._threadmodel._separate_thread.SeparateThread`
  for merge mode with the separate thread model (trpc-cpp
  ``runtime/threadmodel`` and ``separate_runtime``); responses cross back
  to the I/O loops through :class:`~trpc_sdk.protocol.ConnWriter`,
  re-exported here
- :mod:`~trpc_sdk.runtime._listeners` — address classification and the aiohttp
  application every mode serves HTTP with. Sockets and serving are the
  transports' (:mod:`trpc_sdk.transport._server`)
- :mod:`~trpc_sdk.runtime._common` — SIGTERM callbacks, process metadata and the exit template every mode shares (trpc-cpp
  ``runtime/common``)
- :mod:`~trpc_sdk.runtime._process` — task children and the pre-fork
  socketpair mesh (trpc-cpp ``runtime/task``), plus the multi-process
  ``separate`` and ``peer`` modes, which trpc-cpp has no counterpart for
- :mod:`~trpc_sdk.runtime._constants`,
  :mod:`~trpc_sdk.runtime._switches`,
  :mod:`~trpc_sdk.runtime._types` — shared constants, runtime switches, and
  records used across processes. The socketpair primitives those records
  hold live in :mod:`trpc_sdk.utils.ipc`

The service layer (``Server``, ``ServiceAdapter`` and the server adapter) lives in
:mod:`trpc_sdk.server`, which depends on this package and never the other way
round.

Application code reaches this package for lifecycle hooks such as
:func:`register_handler_thread_hook` and :func:`set_signal_handle`; serving
itself starts from :func:`trpc_sdk.server.create_server`, which also
re-exports :func:`register_protocol_handle`.
"""

from trpc_sdk.protocol import ConnWriter
from trpc_sdk.transport import register_protocol_handle

from ._common import ProcessInfo
from ._common import ProcessTaskInfo
from ._common import set_signal_handle
from ._constants import IO_HANDLE_TYPE_MERGE
from ._constants import IO_HANDLE_TYPE_SEPARATE
from ._constants import MODE_MERGE
from ._constants import MODE_PEER
from ._constants import MODE_SEPARATE
from ._constants import MODE_SPELLINGS
from ._constants import PROCESS_MODEL_MULTI
from ._constants import PROCESS_MODEL_SINGLE
from ._constants import Network
from ._merge_worker import MergeWorker
from ._process import BaseTaskProcess
from ._process import PeerMainWorker
from ._process import RpcTaskProcess
from ._process import SeparateWorker
from ._process import TaskProcess
from ._switches import use_c_task
from ._threadmodel import HandlerThreadInfo
from ._threadmodel import InlineReactor
from ._threadmodel import Reactor
from ._threadmodel import ReactorGroup
from ._threadmodel import ResourceKind
from ._threadmodel import ThreadModel
from ._threadmodel import current_handler_thread
from ._threadmodel import current_thread_model
from ._threadmodel import install_thread_model
from ._threadmodel import register_handler_thread_hook
from ._threadmodel import use_thread_model
from ._types import TaskInfo
from ._types import WorkerInfo
from ._worker import BaseWorker

# Called by Server.serve. Absent from __all__ on purpose: application code
# uses set_signal_handle, register_handler_thread_hook, and create_server.
from ._common import run_server_signal_handle
from ._modes import create_worker
from ._threadmodel import freeze_handler_thread_hooks

__all__ = [
    # Process model
    "ConnWriter",
    "MergeWorker",
    "PeerMainWorker",
    "ProcessInfo",
    "ProcessTaskInfo",
    "RpcTaskProcess",
    "SeparateWorker",
    "TaskInfo",
    "TaskProcess",
    "WorkerInfo",
    "BaseTaskProcess",
    "BaseWorker",
    # Config values shared with the YAML schema
    "IO_HANDLE_TYPE_MERGE",
    "IO_HANDLE_TYPE_SEPARATE",
    "MODE_MERGE",
    "MODE_PEER",
    "MODE_SEPARATE",
    "MODE_SPELLINGS",
    "PROCESS_MODEL_MULTI",
    "PROCESS_MODEL_SINGLE",
    "Network",
    # Thread model the worker installs after fork; server transports read it
    "InlineReactor",
    "Reactor",
    "ReactorGroup",
    "ResourceKind",
    "ThreadModel",
    "current_thread_model",
    "install_thread_model",
    "use_thread_model",
    # Lifecycle and extension hooks
    "HandlerThreadInfo",
    "current_handler_thread",
    "register_handler_thread_hook",
    "register_protocol_handle",
    "set_signal_handle",
    # Runtime feature switches
    "use_c_task",
]
