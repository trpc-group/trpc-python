# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Python dispatcher child process: accept client TCP and forward RPC frames.

In ``server.mode: dispatcher`` (the default), the master engine forks one
dispatcher process plus N worker processes. The dispatcher **owns the public
listen sockets**. It does not run service handlers. Incoming connections are
accepted here; complete frames are tagged with ``(fd, session_id)`` and written
to a worker over an IPC pipe. Worker responses come back on the same pipe and
are written to the original client transport.

This module provides:

* :class:`DispatcherInfo` — master-side record filled **before fork**: which
  ``Process`` subclass to spawn (this Python dispatcher or
  ``CDispatcherProcess``), plus the already-created worker/RPCZ pipes. After
  ``start()``, ``process_instance`` holds the live child.
* :class:`DispatcherProcess` — the Python asyncio implementation of that
  child. After fork, it wraps each worker pipe with ``WorkerConnection``, then
  calls ``create_server`` on every listen address with ``DispatcherConnection``
  as the protocol factory.
"""

from __future__ import annotations

import asyncio
import traceback
from dataclasses import dataclass
from dataclasses import field
from functools import partial
from multiprocessing import Process
from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from typing import Type

from trpc_sdk.common import ProcessType
from trpc_sdk.log import logger
from trpc_sdk.telemetry import report_attr
from trpc_sdk.transport import ClientConnectionInfo
from trpc_sdk.transport import DispatcherConnection
from trpc_sdk.transport import WorkerConnection
from trpc_sdk.transport import WorkerRpczConnection
from trpc_sdk.transport import get_dispatcher_transport
from trpc_sdk.transport import new_server_socket
from ._child_process import BaseChildProcess
from ._pipe import PipeConnection


@dataclass
class DispatcherInfo:
    """Master-side record for one dispatcher child process.

    The engine fills this **before fork**: which ``Process`` subclass to spawn
    (Python ``DispatcherProcess`` or ``CDispatcherProcess``), plus the
    already-created IPC pipes. After ``start()``, ``process_instance`` holds the
    live child. The dispatcher process itself never sees this object as a
    shared mutable bag; it copies the pipe lists in ``__init__``.

    Default mode is one dispatcher. ``transport`` is only used by the C
    dispatcher path; the Python path ignores it and uses asyncio instead.

    Example: ``worker_num=2`` yields two req/rsp pipes and two RPCZ pipes.
    ``handle`` is ``DispatcherProcess``, ``process_instance`` is set after
    ``Process.start()``, and ``pid`` is that child's OS pid.
    """

    # Req/rsp IPC pipes, one per worker; dispatcher end is used after fork.
    worker_pipe_conn_list: List[PipeConnection] = field(default_factory=list)
    # Side-channel pipes for worker RPCZ spans; one per worker.
    rpcz_pipe_conn_list: List[PipeConnection] = field(default_factory=list)
    # Child class to spawn: DispatcherProcess (Python) or CDispatcherProcess.
    handle: Optional[Type[Process]] = None
    # Live multiprocessing.Process after the engine has started the child.
    process_instance: Optional[Process] = None
    # Native C transport; unused by the Python DispatcherProcess path.
    transport: Any = field(default_factory=get_dispatcher_transport)

    @property
    def pid(self):
        """OS process id of the running dispatcher process."""
        return self.process_instance.pid


class DispatcherProcess(BaseChildProcess):
    """Python dispatcher child: accept client TCP, forward frames to workers.

    This process **owns the public listen sockets**. It does not run service
    handlers. After fork, it builds an event loop, wraps each worker pipe with
    ``WorkerConnection``, then calls ``create_server`` on every listen address
    with ``DispatcherConnection`` as the protocol factory.

    Data path::

        Client --TCP--> DispatcherConnection --pipe--> Worker
                         (frame + IPC header)
        Worker  --pipe--> WorkerConnection --TCP--> Client
                         (look up fd + session_id)

    ``client_connections`` is keyed by kernel **fd**, not session_id. The
    value is a ``ClientConnectionInfo`` (session_id + Transport). Session ids
    exist because fds are reused; a late worker response must match both.

    Contrast ``CDispatcherProcess`` in ``c_dispatcher.pyx``: same process
    role, native listen/forward instead of asyncio protocols.

    Example: listen ``10.0.0.5:8000``, two workers. Client connect gets
    ``fd=42``, ``session_id=7``. A complete RPC frame is encoded with those
    ids and written to worker 0's pipe. The response comes back tagged
    ``(42, 7)`` and is written on the Transport stored in
    ``client_connections[42]``.
    """

    def __init__(self, dispatcher: DispatcherInfo, server_engine):
        super().__init__("dispatcher", ProcessType.DISPATCHER)
        # IPC pipes from this dispatcher to each worker process.
        self.dispatcher_pipe_conn_list = dispatcher.worker_pipe_conn_list
        # IPC pipes used to forward RPCZ spans to worker processes.
        self.rpcz_pipe_conn_list = dispatcher.rpcz_pipe_conn_list
        # Public listen addresses inherited from the master engine.
        self.addresses = server_engine.process_info.addresses
        # Socket type (TCP/UDP/UNIX) for each listen address.
        self.socket_types = server_engine.process_info.socket_types
        # Protocol checkers used to inspect inbound client connections.
        self.checkers = server_engine.process_info.checkers
        # Live pipe transports to workers; index is the scheduler worker index.
        self.worker_connections: List[asyncio.Transport] = []
        # Live clients keyed by kernel fd; value is ClientConnectionInfo.
        self.client_connections: Dict[int, ClientConnectionInfo] = {}
        # RPCZ spans in flight, keyed by (fd, session_id).
        self.rpcz_span_map = {}
        # Monotonic id assigned to each new client session.
        self.session_id = 0

    def _prepare_to_serve(self) -> None:
        """Connect to workers and RPCZ pipes, then listen on public server addresses."""
        logger.info("[Dispatcher] DispatcherProcess start build connections to workers and RPCZ pipes and listen on public server addresses")
        # Build connections to worker processes.
        for dispatcher_pipe_conn in self.dispatcher_pipe_conn_list:
            dispatcher_pipe_conn.close_extra_conn()
            handle = partial(WorkerConnection, app=self)
            co_conn = self.loop.create_connection(handle, sock=dispatcher_pipe_conn.get_socket())
            self.loop.run_until_complete(co_conn)

        # Build connections to rpcz pipes.
        for rpcz_pipe_conn in self.rpcz_pipe_conn_list:
            rpcz_pipe_conn.close_extra_conn()
            handle = partial(WorkerRpczConnection, app=self)
            co_conn = self.loop.create_connection(handle, sock=rpcz_pipe_conn.get_socket())
            self.loop.run_until_complete(co_conn)

        # Listen on public server addresses.
        for i, address in enumerate(self.addresses):
            sock = new_server_socket(address, self.socket_types[i], reuse_port=True)
            handle = partial(DispatcherConnection, app=self, address=address, checker=self.checkers[i])
            loop_server = self.loop.create_server(handle, sock=sock, reuse_port=True, backlog=1024)
            self.loop.run_until_complete(loop_server)

    def _on_sigterm_received(self) -> None:
        logger.error("[Dispatcher] Received kill signal")

    def _on_graceful_exit(self) -> None:
        report_attr("trpc_dispatcher_exit", 1)
        logger.error("[Dispatcher] DispatcherProcess exited")

    def _on_unhandled_exception(self, err: BaseException) -> None:
        """Report the failure instead of re-raising; the master sees the exit."""
        report_attr("trpc_dispatcher_err_exit", 1)
        logger.error("[Dispatcher] DispatcherProcess exited with error: %s, trace: %s", repr(err),
                     traceback.format_stack())
