# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Worker of ``separate`` mode: this process does I/O, its children do the work.

The split is the one thing this mode exists for. :class:`SeparateWorker` owns
the public listen sockets and nothing else: it accepts connections, cuts the
byte stream into frames, and writes each frame to one of ``rpc_task_num``
:class:`RpcTaskProcess` children over an IPC pipe. Those children decode the
frame, run the service handler, and write the response back on the same pipe,
where the worker matches it to the client that asked.

That costs one extra copy through a socketpair per request, and buys real
parallelism: handlers run in separate OS processes, so Python bytecode is not
serialized by a single GIL. It is the mode a CPU-bound service needs. A
service that spends its time waiting on I/O is better served by ``merge``
mode, which keeps the request in one process.

The frame protocol and both protocol classes are the ones the old dispatcher
and worker pair used — ``inner_encode_req`` / ``inner_decode_rsp``,
:class:`~trpc_sdk.transport.DispatcherClientConnection` on this side and
:class:`~trpc_sdk.transport.WorkerDispatcherConnection` on the child's. Only the
process that runs them changed: the accepting process is now the server
process itself rather than a child a master forked.

Responses are addressed by ``(fd, session_id)``. The fd alone is not enough
because the kernel reuses fds: a response that arrives after the client is
gone must not be written to whoever inherited its descriptor.
"""

from __future__ import annotations

import asyncio
import socket
import traceback
from dataclasses import dataclass
from functools import partial
from multiprocessing import Process
from typing import Any
from typing import Dict
from typing import List
from typing import Tuple

from trpc_sdk.common import ProcessType
from trpc_sdk.ipc import PipeConnection
from trpc_sdk.ipc import PipeEnd
from trpc_sdk.ipc import PipeKind
from trpc_sdk.ipc import create_pipe
from trpc_sdk.log import logger
from trpc_sdk.plugin import init_task_plugins
from trpc_sdk.telemetry import report_attr
from trpc_sdk.transport import ClientConnectionInfo
from trpc_sdk.transport import DispatcherClientConnection
from trpc_sdk.transport import DispatcherRpczConnection
from trpc_sdk.transport import DispatcherWorkerConnection
from trpc_sdk.transport import WorkerDispatcherConnection
from trpc_sdk.transport import create_server_socket
from ._constants import LISTEN_BACKLOG
from ._constants import RPC_TASK_PRODUCER_PREFIX
from ._worker import BaseWorker


def rpc_task_producer_name(index: int) -> str:
    """Return the task-mesh producer name of one rpc task process.

    An rpc task calls infer tasks exactly as a merge worker would, so it needs
    its own pipe to each of them and therefore its own producer identity.

    Args:
        index: Position of the rpc task in the worker's child list.
    """
    return f"{RPC_TASK_PRODUCER_PREFIX}:{index}"


@dataclass
class RpcTaskPipes:
    """The two socketpairs joining the worker to one rpc task child.

    Holds the raw pairs rather than one endpoint, because both processes need
    an endpoint and they must be *different* ones. Created before the child is
    forked, so it inherits both ends of both pairs and each side then drops the
    one it does not own. The rpc task takes ``PipeEnd.LEFT`` and the worker
    ``PipeEnd.RIGHT``, the convention the dispatcher and its workers used.

    Attributes:
        request: Request/response frames, in both directions.
        rpcz: One-way side channel carrying finished RPCZ spans to the worker.
    """

    request: Tuple[socket.socket, socket.socket]
    rpcz: Tuple[socket.socket, socket.socket]

    def endpoints(self, end: PipeEnd) -> Tuple[PipeConnection, PipeConnection]:
        """Return this side's ``(request, rpcz)`` endpoints.

        Args:
            end: ``PipeEnd.LEFT`` for the rpc task, ``PipeEnd.RIGHT`` for the
                worker.
        """
        return PipeConnection(self.request, end), PipeConnection(self.rpcz, end)

    def close(self) -> None:
        """Close both ends of both pairs.

        Used by an rpc task on its siblings' pipes: it inherited them at fork
        and holding them would keep the kernel's reference count above zero, so
        the worker's end would never reach EOF when that sibling dies.
        """
        for pair in (self.request, self.rpcz):
            pair[0].close()
            pair[1].close()


class RpcTaskProcess(BaseWorker, Process):
    """Child of a separate-mode worker: decodes frames and runs handlers.

    Owns no listen socket. Everything it serves arrives on the request pipe,
    already framed and tagged with the fd, session id and listen port it came
    in on; the port is what selects the handler, timeout and transport options
    to serve it with.

    It is a full :class:`BaseWorker`, which is what makes
    :func:`~trpc_sdk.task.send_to_task` work from a handler running here without
    any extra plumbing: the base class connects to every infer task, and this
    process simply appears in the mesh as its own producer.
    """

    def __init__(self, index: int, pipes: List[RpcTaskPipes], engine) -> None:
        """Record this child's index and pipes; nothing is bound yet.

        Runs in the worker **before fork**. The event loop, plugins and IPC
        wiring all happen later, in :meth:`~trpc_sdk.server._worker.BaseWorker.run`.

        Args:
            index: Position in the worker's rpc task list. Doubles as the
                worker id used to label timers and inter-worker messages.
            pipes: Pipes of **every** rpc task, not just this one. Fork copies
                them all, so this child needs the whole list to know which
                descriptors to drop.
            engine: Engine holding ``process_info``; inherited through fork.
        """
        super().__init__(f"rpc_task-{index}", ProcessType.WORKER)
        self._bind_process_info(engine)

        self.index: int = index
        self.worker_id = index
        # Pipes of every sibling; all but ``index`` are closed after fork.
        self._all_pipes: List[RpcTaskPipes] = pipes
        self.worker_pipe_conn, self.rpcz_pipe_conn = pipes[index].endpoints(PipeEnd.LEFT)

    def _owned_producer_names(self) -> List[str]:
        """Claim this child's producer identity in the task mesh."""
        return [rpc_task_producer_name(self.index)]

    def start_worker_process(self) -> None:
        """Drop the descriptors of other processes, then attach the request pipe.

        Called by :meth:`~trpc_sdk.server._worker.BaseWorker._prepare_to_serve`
        once plugins and infer-task connections are up; this is where the
        process starts accepting work.

        The RPCZ pipe is closed to its far end but never wrapped in a
        protocol: this side only ever writes. ``WorkerDispatcherConnection``
        pushes finished spans through ``app.rpcz_pipe_conn.send()`` and the
        worker reads them.
        """
        logger.info("[rpc task] RpcTaskProcess-%s start_worker_process()", self.index)
        self._close_sibling_pipes()
        self.worker_pipe_conn.close_extra_conn()
        self.rpcz_pipe_conn.close_extra_conn()

        handle = partial(WorkerDispatcherConnection,
                         app=self,
                         addresses=self.addresses_map,
                         handlers=self.handlers,
                         timeouts=self.timeouts,
                         tran_opts=self.tran_opts)
        coro = self.loop.create_connection(handle, sock=self.worker_pipe_conn.get_socket())
        self.loop.run_until_complete(coro)

    def _close_sibling_pipes(self) -> None:
        """Close both ends of every other rpc task's pipes.

        Fork gave this child its siblings' descriptors too. Holding them keeps
        the kernel's reference count above zero, so when a sibling dies the
        worker's end never reaches EOF: the worker would keep writing requests
        into a pipe nobody reads, instead of noticing the loss.
        """
        for index, pipes in enumerate(self._all_pipes):
            if index != self.index:
                pipes.close()

    def _on_sigterm_received(self) -> None:
        """Log the shutdown; the base class exits the loop afterwards."""
        logger.error("[rpc task] RpcTaskProcess-%s received kill signal", self.index)


class SeparateWorker(BaseWorker):
    """The server process in ``separate`` mode: it listens, its children handle.

    Not a ``Process`` subclass: this object describes the process it runs in,
    so there is nobody to fork it. It forks the rpc tasks instead.

    Attributes:
        rpc_task_num: Number of rpc task children that run handlers.
        worker_connections: Live pipe transports to those children, indexed by
            the value the scheduler returns.
        client_connections: Live clients keyed by kernel fd; the value carries
            the session id a response has to match.
    """

    def __init__(self, engine, name: str = "separate_worker") -> None:
        """Copy the endpoint tables out of the engine; nothing is bound yet.

        Sockets, pipes and the event loop are all created later. Nothing here
        may outlive a fork boundary: this worker forks its children after
        being constructed and before its loop exists.

        Args:
            engine: Engine holding ``process_info`` (the per-endpoint tables
                filled during ``Service.run``) and ``opts``.
            name: Process name shown in logs and process listings.
        """
        super().__init__(name, ProcessType.WORKER)
        self._bind_process_info(engine)

        # At least one child, otherwise nothing would run handlers.
        self.rpc_task_num: int = max(1, engine.opts.rpc_task_num)
        # Pipes to the rpc tasks, index-aligned with them. Filled in
        # ``_spawn_children``, before any of them is forked.
        self.rpc_task_pipes: List[RpcTaskPipes] = []

        # Live pipe transports to the rpc tasks; the index the scheduler
        # returns selects one of these.
        self.worker_connections: List[asyncio.Transport] = []
        # Live clients keyed by kernel fd; value is a ClientConnectionInfo.
        self.client_connections: Dict[int, ClientConnectionInfo] = {}
        # RPCZ spans in flight, keyed by (fd, session_id).
        self.rpcz_span_map: Dict[Tuple[int, int], Any] = {}
        # Monotonic id assigned to each new client session, so a response can
        # be told apart from one meant for a previous owner of the same fd.
        self.session_id: int = 0

    def _init_plugins(self) -> None:
        """Initialize only the non-business plugins.

        Narrows :meth:`~trpc_sdk.server._worker.BaseWorker._init_plugins`: no
        service handler runs in this process, it only moves frames between
        client sockets and rpc task pipes. Filters, naming and codecs are set
        up in the children, which is where user code executes.
        """
        init_task_plugins()

    def _task_producer_names(self) -> List[str]:
        """Allocate one infer-task producer identity per rpc task child.

        The worker itself is deliberately absent: handlers run in the
        children, so the children are what talk to infer tasks.
        """
        return [rpc_task_producer_name(index) for index in range(self.rpc_task_num)]

    def _owned_producer_names(self) -> List[str]:
        """Claim nothing: every infer-task pipe belongs to a child.

        Returning an empty list is what makes
        :meth:`~trpc_sdk.server._worker.BaseWorker._close_foreign_pipes` drop all
        of them here after the forks.
        """
        return []

    def _spawn_children(self) -> None:
        """Fork the infer tasks first, then the rpc tasks.

        Order is deliberate. The infer tasks are forked by the base class
        before the rpc task pipes exist, so they cannot inherit descriptors
        that have nothing to do with them. The rpc tasks are forked after, and
        do inherit each other's, which is why each closes its siblings' in
        :meth:`RpcTaskProcess._close_sibling_pipes`.

        The task mesh itself is still built before every fork, as it must be:
        a peer-to-peer task pipe joins two children and neither can create it.
        """
        super()._spawn_children()

        self._create_rpc_task_pipes()
        for index in range(self.rpc_task_num):
            self._spawn(f"rpc_task-{index}",
                        partial(RpcTaskProcess, index, self.rpc_task_pipes, self.server_engine))

    def _create_rpc_task_pipes(self) -> None:
        """Build both socketpairs for every rpc task, before any of them forks.

        Stream pipes in both cases: the request pipe is re-framed by
        ``InnerParser`` on the child side, and the RPCZ channel carries a
        stream of spans.
        """
        for _ in range(self.rpc_task_num):
            self.rpc_task_pipes.append(
                RpcTaskPipes(request=create_pipe(PipeKind.TCP), rpcz=create_pipe(PipeKind.TCP)))

    def start_worker_process(self) -> None:
        """Attach the rpc task pipes, then bind and listen on every address.

        Pipes first: an address must not become reachable before there is
        somewhere to send the frames arriving on it, or the first requests
        would find ``worker_connections`` empty.
        """
        logger.info("[separate worker] SeparateWorker start_worker_process(), rpc tasks: %s", self.rpc_task_num)
        self._connect_rpc_tasks()
        self._listen()

    def _connect_rpc_tasks(self) -> None:
        """Wrap this worker's end of each child's two pipes.

        ``close_extra_conn`` drops the child's end, inherited at fork. Keeping
        it would stop this process from ever seeing the child die.
        """
        for pipes in self.rpc_task_pipes:
            request, rpcz = pipes.endpoints(PipeEnd.RIGHT)

            request.close_extra_conn()
            handle = partial(DispatcherWorkerConnection, app=self)
            self.loop.run_until_complete(self.loop.create_connection(handle, sock=request.get_socket()))

            rpcz.close_extra_conn()
            rpcz_handle = partial(DispatcherRpczConnection, app=self)
            self.loop.run_until_complete(self.loop.create_connection(rpcz_handle, sock=rpcz.get_socket()))

    def _listen(self) -> None:
        """Bind every configured address and start accepting on the main loop.

        ``create_server`` is enough here, unlike in merge mode: the connection
        is only read from and written to on this loop, never handed to another
        one, so there is nothing to gain from accepting by hand.
        """
        for i, address in enumerate(self.addresses):
            sock = create_server_socket(address, self.socket_types[i], reuse_port=True)
            handle = partial(DispatcherClientConnection, app=self, address=address, checker=self.checkers[i])
            server = self.loop.create_server(handle, sock=sock, reuse_port=True, backlog=LISTEN_BACKLOG)
            self.loop.run_until_complete(server)

    def _on_sigterm_received(self) -> None:
        """Log the shutdown; children are stopped after the loop unwinds."""
        logger.error("[separate worker] received kill signal")

    def _on_graceful_exit(self) -> None:
        """Report the exit so a supervisor can tell it from a crash."""
        report_attr("trpc_separate_worker_exit", 1)
        logger.error("[separate worker] SeparateWorker exited")

    def _on_unhandled_exception(self, err: BaseException) -> None:
        """Report the failure instead of re-raising.

        This process is the server, so re-raising would only replace a logged
        cause with a traceback nobody is left to read.

        Args:
            err: The exception that terminated the loop.
        """
        report_attr("trpc_separate_worker_err_exit", 1)
        logger.error("[separate worker] SeparateWorker exited with error: %s, trace: %s", repr(err),
                     traceback.format_stack())
