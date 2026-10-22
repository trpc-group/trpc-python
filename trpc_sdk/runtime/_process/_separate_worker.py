# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Worker of ``separate`` mode: this process does I/O, its children do the work.

The split is the one thing this mode exists for. :class:`SeparateWorker` owns
the public listen sockets and nothing else: it accepts connections, cuts the
byte stream into frames, and writes each frame to one of ``worker_num``
:class:`~trpc_sdk.runtime._process._rpc_task.RpcTaskProcess` children over
an IPC pipe. Those children decode the frame, run the service handler, and
write the response back on the same pipe, where the worker matches it to the
client that asked.

That costs one extra copy through a socketpair per request, and buys real
parallelism: handlers run in separate OS processes, so Python bytecode is not
serialized by a single GIL. It is the mode a CPU-bound service needs. A
service that spends its time waiting on I/O is better served by ``merge``
mode, which keeps the request in one process.

The frame protocol and both protocol classes are the ones the old dispatcher
and worker pair used — ``inner_encode_req`` / ``inner_decode_rsp``,
:class:`~trpc_sdk.transport.SeparateClientConnection` on this side and
:class:`~trpc_sdk.transport.RpcTaskConnection` on the child's. The
frame head's port field carries the listen *address index*, so two addresses
sharing a port still reach their own handler.

Responses are addressed by ``(fd, session_id)``. The fd alone is not enough
because the kernel reuses fds: a response that arrives after the client is
gone must not be written to whoever inherited its descriptor.

Scheduling. Unary frames go round-robin (or to the custom scheduler's pick),
stream frames stick to ``(session_id + stream_id) % rpc_task_num``, as in the
old dispatcher. An rpc task whose pipe closed or that SIGCHLD reported dead is
skipped; a stream pinned to it is dropped.

Startup. An rpc task connects to the task processes, runs its ``on_start``
hooks, binds its own listeners and starts its timers, then sets its ready
flag. Only once every flag is set does this process bind TCP, and only after
that does it register with the naming service. The flags and the wait are
:mod:`~trpc_sdk.runtime._common._readiness`, shared with peer mode.

Shutdown. The children ignore SIGTERM and SIGINT, so a signal sent to the
whole process group only reaches this process. Every process here runs the
shared exit template of :mod:`~trpc_sdk.runtime._common._shutdown`; this one fills its
hooks in so that the sequence is: deregister, close TCP, SIGUSR1 to the rpc
tasks (each closes its HTTP sites and drains ``close_wait_time`` on its own,
then runs ``worker_handle`` and ``on_stop``), wait the full
``close_wait_time`` (a second SIGTERM cuts it short and sends SIGUSR1 again,
which cuts the rpc tasks' drain short too), post-drain callbacks, wait for
the rpc tasks or SIGKILL them, ``master_handle``, close the client
connections, then SIGUSR1 to the task processes. Should this process die
instead, the children notice through ``PDEATHSIG`` (SIGUSR1, told apart from
a shutdown request by the changed parent pid) or the parent sentinel.

Only addresses whose bytes the I/O process can cut into self-delimiting tRPC
frames go through it: the ones
:func:`~trpc_sdk.runtime._listeners.classify_address` calls TCP. Every other
kind is served by each rpc task directly, on its own ``SO_REUSEPORT`` socket:

* USER — a protocol registered with
  :func:`~trpc_sdk.server.register_protocol_handle`; the I/O process has no
  parser that could frame it.
* UDP — datagrams carry no connection to forward over a pipe.
* HTTP — aiohttp parses and answers requests itself.

Those addresses therefore skip ``check_idle_timeout``, the custom scheduler
and the RPCZ post-I/O step, and the kernel picks the rpc task for each
connection or datagram. On macOS ``SO_REUSEPORT`` does not balance load: all
of them work, but in practice one rpc task serves each such address.
"""

from __future__ import annotations

import asyncio
import socket
import traceback
from dataclasses import dataclass
from functools import partial
from typing import Any
from typing import Callable
from typing import Dict
from typing import List
from typing import Optional
from typing import Tuple

from trpc_sdk.abc import ProcessType
from trpc_sdk.config import default_worker_num
from trpc_sdk.log import logger
from trpc_sdk.plugin import init_task_plugins
from trpc_sdk.telemetry import report_attr
from trpc_sdk.transport import ClientConnectionInfo
from trpc_sdk.transport import SeparateClientConnection
from trpc_sdk.transport import SeparateRpcTaskConnection
from trpc_sdk.transport import SeparateRpczConnection
from trpc_sdk.transport import TrpcServerTransport
from trpc_sdk.utils import PipeConnection
from trpc_sdk.utils import PipeEnd
from trpc_sdk.utils import PipeKind
from trpc_sdk.utils import create_pipe
from .._common import ChildReadiness
from .._common import warn_unnamed_timers
from .._constants import AddressKind
from .._constants import CHILD_EXIT_GRACE_SECONDS
from .._constants import SCHEDULE_TARGET_RPC_TASK
from .._listeners import classify_address
from .._types import ChildRecord
from .._worker import BaseWorker
from ._rpc_task import RpcTaskProcess
from ._rpc_task import rpc_task_producer_name


@dataclass
class RpcTaskPipes:
    """The two socketpairs joining the worker to one rpc task child.

    Holds the raw pairs rather than one endpoint, because both processes need
    an endpoint and they must be *different* ones. Created before the child is
    forked, so it inherits both ends of every pair and each side then drops the
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
        """Close both ends of every pair.

        Used by an rpc task on its siblings' pipes: it inherited them at fork
        and holding them would keep the kernel's reference count above zero, so
        the worker's end would never reach EOF when that sibling dies.
        """
        for pair in (self.request, self.rpcz):
            pair[0].close()
            pair[1].close()


class SeparateForwardTransport(TrpcServerTransport):
    """TCP listener of the separate-mode I/O process: frames are cut here and forwarded to rpc tasks.

    Built after fork from the transport the parent bound, so it serves the
    same endpoint with the same options; only the connection protocol differs.
    """

    def __init__(self, app: "SeparateWorker", index: int, parser: Any, schedule_target: str) -> None:
        """Record what every forwarding connection needs.

        Args:
            app: The I/O process, which owns the rpc task pipes.
            index: Position of the address in ``app.addresses``; carried in
                every forwarded frame head.
            parser: Cuts the client byte stream into frames.
            schedule_target: ``SchedulerParams.target`` used to pick an rpc task.
        """
        super().__init__()
        self._app = app
        self._index = index
        self._parser = parser
        self._schedule_target = schedule_target

    def protocol_factory(self, shared_pool: Optional[Any] = None) -> Callable[[], Any]:
        """Return the forwarding connection; ``shared_pool`` does not apply here."""
        del shared_pool
        return partial(SeparateClientConnection,
                       app=self._app,
                       address=self.opts.address,
                       parser=self._parser,
                       index=self._index,
                       schedule_target=self._schedule_target)


class SeparateWorker(ChildReadiness, BaseWorker):
    """The server process in ``separate`` mode: it listens, its children handle.

    Not a ``Process`` subclass: this object describes the process it runs in,
    so there is nobody to fork it. It forks the rpc tasks instead, and waits
    for their ready flags through
    :class:`~trpc_sdk.runtime._common._readiness.ChildReadiness`.

    Attributes:
        rpc_task_num: Number of rpc task children that run handlers
            (``server.worker_num``).
        worker_connections: Pipe transports to those children, indexed by rpc
            task; ``None`` until attached.
        rpc_task_alive: Per rpc task, whether frames may still be sent to it.
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
                filled during ``ServiceAdapter.run``) and ``opts``.
            name: Process name shown in logs and process listings.
        """
        super().__init__(name, ProcessType.WORKER)
        self._bind_process_info(engine)

        opts = engine.opts
        # Already resolved by ``server._option_resolver``; the fallback only
        # guards options built by hand.
        configured = getattr(opts, "worker_num", 0)
        self.rpc_task_num: int = configured if configured > 0 else default_worker_num()
        self.tcp_reuse_port: bool = bool(getattr(opts, "tcp_reuse_port", False))
        # Seconds every rpc task gets, together, to set its ready flag; None waits forever.
        self.ready_timeout: Optional[float] = (max(0, getattr(opts, "ready_timeout", 0)) / 1000.0) or None
        # Pipes to the rpc tasks, index-aligned with them. Filled in
        # ``_spawn_children``, before any of them is forked.
        self.rpc_task_pipes: List[RpcTaskPipes] = []

        self.worker_connections: List[Optional[asyncio.Transport]] = [None] * self.rpc_task_num
        self.rpc_task_alive: List[bool] = [True] * self.rpc_task_num
        # Live clients keyed by kernel fd; value is a ClientConnectionInfo.
        self.client_connections: Dict[int, ClientConnectionInfo] = {}
        # RPCZ spans in flight, keyed by (fd, session_id).
        self.rpcz_span_map: Dict[Tuple[int, int], Any] = {}
        # Monotonic id assigned to each new client session, so a response can
        # be told apart from one meant for a previous owner of the same fd.
        self.session_id: int = 0

        # Child records by role, in fork order.
        self._task_records: List[ChildRecord] = []
        self._rpc_records: List[ChildRecord] = []

    @property
    def exiting(self) -> bool:
        """Whether shutdown (or a startup abort) has begun."""
        return self._exiting

    def mark_rpc_task_dead(self, index: int) -> None:
        """Stop sending frames to rpc task ``index``.

        Called when its request pipe closes, or SIGCHLD reaps it,
        whichever comes first.

        Args:
            index: The rpc task.
        """
        if 0 <= index < len(self.rpc_task_alive) and self.rpc_task_alive[index]:
            self.rpc_task_alive[index] = False
            if not self._exiting:
                logger.error("[separate worker] rpc task %s is gone; no more frames go to it", index)

    def _init_plugins(self) -> None:
        """Initialize only the non-business plugins.

        Narrows :meth:`~trpc_sdk.runtime._worker.BaseWorker._init_plugins`: no
        service handler runs in this process, it only moves frames between
        client sockets and rpc task pipes. Filters, naming and codecs are set
        up in the children, which is where user code executes. A failure stops
        the children rather than leaving them to wait for a parent that is
        gone.
        """
        try:
            init_task_plugins()
        except Exception as err:  # pylint: disable=broad-except
            logger.error("[separate worker] plugin initialization failed: %s", repr(err), exc_info=True)
            self._exit_now(1)

    def _task_producer_names(self) -> List[str]:
        """Allocate one infer-task producer identity per rpc task child.

        The worker itself is deliberately absent: handlers run in the
        children, so the children are what talk to infer tasks.
        """
        return [rpc_task_producer_name(index) for index in range(self.rpc_task_num)]

    def _owned_producer_names(self) -> List[str]:
        """Claim nothing: every infer-task pipe belongs to a child.

        Returning an empty list is what makes
        :meth:`~trpc_sdk.runtime._worker.BaseWorker._close_foreign_pipes` drop all
        of them here after the forks.
        """
        return []

    def _spawn_children(self) -> None:
        """Fork the infer tasks first, then the rpc tasks.

        Order is deliberate. The infer tasks are forked by the base class
        before the rpc task pipes exist, so they cannot inherit descriptors
        that have nothing to do with them. The rpc tasks are forked after, and
        do inherit each other's, which is why each closes those in
        :meth:`~trpc_sdk.runtime._process._rpc_task.RpcTaskProcess._close_sibling_pipes`.

        The task mesh itself is still built before every fork, as it must be:
        a peer-to-peer task pipe joins two children and neither can create it.

        Addresses are validated before anything is forked, so a configuration
        error stops startup instead of crashing every rpc task.
        """
        self._validate_addresses()
        self._check_before_fork()
        super()._spawn_children()
        self._task_records = list(self.children)

        self._create_rpc_task_pipes()
        for index in range(self.rpc_task_num):
            self._spawn(f"rpc_task-{index}",
                        partial(RpcTaskProcess, index, self.rpc_task_pipes, self._ready_flags, self.server_engine))
        self._rpc_records = self.children[len(self._task_records):]

    def _before_fork_tasks(self) -> None:
        """Mark every task record as separate mode before it is forked."""
        for task_info in self.task_infos:
            task_info.separate_mode = True

    def _validate_addresses(self) -> None:
        """Fail before the fork on an address no process here could serve.

        Raises:
            ValueError: An address matches no listener, or a TCP address has
                no parser factory to cut its byte stream into frames.
        """
        for i, address in enumerate(self.addresses):
            kind = classify_address(self, i)
            if kind is AddressKind.UNKNOWN:
                raise ValueError(f"separate mode: unsupported network {self.networks[i]!r} for address {address}")
            if kind is AddressKind.TCP and not hasattr(self.parsers[i], "create"):
                raise ValueError(f"separate mode: no protocol parser for TCP address {address} "
                                 f"(protocol {self.services[i].opt.protocol!r}), so its frames cannot be forwarded")

    def _check_before_fork(self) -> None:
        """Warn about registrations that behave differently across rpc tasks.

        Done here rather than at config load because it depends on what the
        application registered at import time. A removed
        ``on_worker_message`` is warned about where it is assigned.
        """
        warn_unnamed_timers(self.name, self.rpc_task_num, "rpc tasks")

    def _create_rpc_task_pipes(self) -> None:
        """Build both socketpairs and the ready flags for every rpc task, before any of them forks.

        Stream pipes in both cases: the request pipe is re-framed by
        ``InnerParser`` on the child side, and the RPCZ channel carries a
        stream of spans.
        """
        self._create_ready_flags(self.rpc_task_num)
        for _ in range(self.rpc_task_num):
            self.rpc_task_pipes.append(RpcTaskPipes(request=create_pipe(PipeKind.TCP), rpcz=create_pipe(PipeKind.TCP)))

    def start_worker_process(self) -> None:
        """Attach the pipes, wait for every rpc task to be bound, then listen on TCP.

        Pipes first: an address must not become reachable before there is
        somewhere to send the frames arriving on it. Waiting for the ready
        flags next, so that no request reaches an rpc task whose ``on_start``
        hooks have not run, and the naming registration that follows this
        method never advertises a half-started server.
        """
        logger.info("[separate worker] SeparateWorker start_worker_process(), rpc tasks: %s", self.rpc_task_num)
        self._connect_rpc_tasks()
        elapsed = self._wait_children_ready(self._task_records + self._rpc_records)
        logger.info("[separate worker] separate: %s rpc tasks bound in %.0f ms", self.rpc_task_num, elapsed * 1000)
        self._listen()

    def _connect_rpc_tasks(self) -> None:
        """Wrap this worker's end of each child's request and RPCZ pipes.

        ``close_extra_conn`` drops the child's end, inherited at fork. Keeping
        it would stop this process from ever seeing the child die.
        """
        for index, pipes in enumerate(self.rpc_task_pipes):
            request, rpcz = pipes.endpoints(PipeEnd.RIGHT)

            request.close_extra_conn()
            handle = partial(SeparateRpcTaskConnection, app=self, index=index)
            self.loop.run_until_complete(self.loop.create_connection(handle, sock=request.get_socket()))

            rpcz.close_extra_conn()
            rpcz_handle = partial(SeparateRpczConnection, app=self)
            self.loop.run_until_complete(self.loop.create_connection(rpcz_handle, sock=rpcz.get_socket()))

    def _listen(self) -> None:
        """Serve every ``TCP`` address on the main loop, forwarding its frames.

        Other kinds are left to the rpc tasks, which listen on them
        themselves. Each transport is replaced by a
        :class:`SeparateForwardTransport` bound to the same options: frames
        are cut here and forwarded, not handled. The replacement happens after
        fork, so neither the parent nor the rpc tasks see it.
        ``SO_REUSEPORT`` only with ``tcp_reuse_port``: without it a second
        instance started by mistake fails here instead of silently sharing the
        port. A bind failure aborts startup, since serving half the addresses
        would be worse than not starting.
        """
        for i, address in enumerate(self.addresses):
            if classify_address(self, i) is not AddressKind.TCP:
                continue
            forward = SeparateForwardTransport(self, i, self.parsers[i], SCHEDULE_TARGET_RPC_TASK)
            forward.bind(self.transports[i].opts)
            self.transports[i] = forward
            try:
                self._listen_on(i)
            except OSError as err:
                logger.error("[separate worker] failed to bind TCP %s (errno %s): %s", address, err.errno, err)
                self._exit_now(1)

    def _replace_child(self, pid: int) -> None:
        """Stop scheduling to a dead rpc task, then let the base class decide.

        Outside shutdown the base class sets exit status 1 and starts the
        exit template, so the whole server shuts down in order. Requests
        already handed to a dead rpc task are lost; their clients time out.

        Args:
            pid: OS pid reported by ``waitpid``.
        """
        record = self._children_by_pid.get(pid)
        if record is not None and record in self._rpc_records:
            self.mark_rpc_task_dead(self._rpc_records.index(record))
        super()._replace_child(pid)

    def _close_client_connections(self) -> None:
        for info in list(self.client_connections.values()):
            if info.conn is not None and not info.conn.is_closing():
                info.conn.close()
        self.client_connections.clear()

    def _start_timers(self) -> None:
        """No timers here: this process runs no business code.

        The rpc tasks run ``worker:<index>`` executors. One here as well would
        share rpc task 0's name and run every unnamed timer once more, in a
        process whose business plugins were never initialized.
        """

    def _stop_accepting(self) -> None:
        """Stop the TCP transports accepting, then send the rpc tasks their first SIGUSR1.

        Open client connections keep being served. Each rpc task stops its
        own transports on that signal and drains ``close_wait_time`` alongside
        this process.
        """
        self._stop_listening()
        self._signal(self._rpc_records)

    def _on_drain_cut(self) -> None:
        """Send the rpc tasks another SIGUSR1, so they stop waiting out ``close_wait_time`` too."""
        self._signal(self._rpc_records)

    async def _wait_handlers(self) -> None:
        """Wait for the rpc tasks to exit, SIGKILL them after ``CHILD_EXIT_GRACE_SECONDS``.

        By now this process waited the full drain or had it cut short, and so
        did they, so the grace period only covers their ``worker_handle``,
        ``on_stop`` and listener cleanup. ``master_exit_callbacks`` run after
        this, so ``master_handle`` always runs after every ``worker_handle``.
        """
        await self._wait_children(self._rpc_records, CHILD_EXIT_GRACE_SECONDS)

    def _on_drained(self) -> None:
        """Close the client connections; no user worker exit callbacks, handlers run in the rpc tasks.

        The task processes are stopped afterwards, by ``_stop_children`` once
        the loop stopped; every rpc task is gone by then, so nobody waits for
        a task result any more.
        """
        self._close_client_connections()

    def _on_graceful_exit(self) -> None:
        """Report the exit so a supervisor can tell it from a crash."""
        report_attr("trpc_separate_worker_exit", 1)
        logger.error("[separate worker] SeparateWorker exited")

    def _on_unhandled_exception(self, err: BaseException) -> None:
        """Report the failure and exit with status 1 instead of re-raising.

        This process is the server, so re-raising would only replace a logged
        cause with a traceback nobody is left to read. The children were
        already stopped by :meth:`_stop_children`.

        Args:
            err: The exception that terminated the loop.
        """
        self._exit_code = 1
        report_attr("trpc_separate_worker_err_exit", 1)
        logger.error("[separate worker] SeparateWorker exited with error: %s, trace: %s", repr(err),
                     traceback.format_exc())
