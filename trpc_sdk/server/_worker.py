# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Worker child processes: the processes that run user handlers.

A worker never touches a client socket. In ``dispatcher`` mode the dispatcher
owns the listen sockets, reads whole frames off them and forwards each frame
over an IPC pipe; the worker decodes it, runs the service handler and writes
the response back on the same pipe. That keeps accept/read out of the process
that executes user code, and lets the master scale handlers by forking more
workers without touching the listen sockets.

This module provides:

* :class:`WorkerInfo` — the master-side record for one worker, filled before
  fork with its id and its end of every IPC pipe.
* :class:`BaseWorker` — the post-fork runtime shared by every worker flavour:
  task connections, the inter-worker message pipe, timers, and the mapping
  from listen port to service handler.
* :class:`WorkerProcess` — the asyncio worker paired with the Python
  dispatcher.
* :class:`CWorkerProcess` — the worker paired with the C dispatcher, which
  exchanges frames over datagram pipes unless the trpcio driver is enabled.

:class:`~trpc.server._executor.ExecutorProcess`, the ``multi-listener``
variant that listens for itself, extends :class:`BaseWorker` from its own
module.
"""

from __future__ import annotations

import asyncio
import signal
import socket
from dataclasses import dataclass
from dataclasses import field
from functools import partial
from multiprocessing import Process
from typing import Dict
from typing import List
from typing import Optional
from typing import Tuple
from typing import Type

from trpc_sdk.common import ProcessType
from trpc_sdk.config import TrpcConfig
from trpc_sdk.log import logger
from trpc_sdk.plugin import init_worker_plugins
from trpc_sdk.task import TaskStreamClientMap
from trpc_sdk.task import register_task_conn
from trpc_sdk.transport import CTaskConnection
from trpc_sdk.transport import CWorkerConnection
from trpc_sdk.transport import TaskConnection
from trpc_sdk.transport import WorkerConnection
from trpc_sdk.transport import WorkerMsgConnection
from trpc_sdk.transport import encode_worker_msg_req
from trpc_sdk.utils import TimerExecutor

from ._child_process import BaseChildProcess
from ._constants import LEFT_PORT
from ._features import use_trpcio
from ._pipe import PipeConnection
from ._signals import run_worker_signal_handle
from ._task import TaskInfo


@dataclass
class WorkerInfo:
    """Master-side record for one worker child process.

    The engine fills this **before fork**: worker id, IPC pipes, and which
    ``Process`` subclass to spawn. After ``start()``, ``process_instance``
    holds the live child.
    """

    # Worker index in ``worker_process_list``; used as the from/to id
    # on the inter-worker message pipe.
    id: int = 0
    # ``multiprocessing.Process`` subclass to spawn: ``WorkerProcess``,
    # ``CWorkerProcess``, or ``ExecutorProcess``. Set in
    # ``start_new_worker_process`` right before fork.
    handle: Optional[Type[Process]] = None
    # Live child after ``start()``. The master stores the ``Process``
    # here to join / inspect pid; after fork the child overwrites it
    # with ``self`` so ``current_worker().process_instance`` is this
    # process.
    process_instance: Optional[Process] = None
    # This worker's end (``LEFT_PORT``) of the dispatcher req/rsp IPC
    # pipe. The dispatcher owns ``RIGHT_PORT`` of the same pair.
    # Stream socket unless the C dispatcher is on without trpcio,
    # in which case it is a datagram pair.
    pipe_conn: Optional[PipeConnection] = None
    # Raw socketpair backing ``pipe_conn``. Kept so the dispatcher
    # can wrap the other end as ``PipeConnection(..., RIGHT_PORT)``.
    socket_pair: Optional[Tuple[socket.socket, socket.socket]] = None
    # This worker's end (``LEFT_PORT``) of the RPCZ span side-channel.
    # The worker writes finished spans here; the dispatcher reads them
    # and forwards to the RPCZ collector. Always a stream pair.
    rpcz_pipe_conn: Optional[PipeConnection] = None
    # Raw socketpair backing ``rpcz_pipe_conn``.
    rpcz_socket_pair: Optional[Tuple[socket.socket, socket.socket]] = None
    # This worker's read end (``LEFT_PORT``) of the inter-worker
    # message pipe. ``start_message_listener`` attaches
    # ``WorkerMsgConnection`` here and delivers payloads to
    # ``on_worker_message``.
    read_message_pipe: Optional[PipeConnection] = None
    # Write end (``RIGHT_PORT``) of that same message pipe. The master
    # (and ``send_msg``) push encoded frames here; the worker never
    # writes this end.
    write_message_pipe: Optional[PipeConnection] = None
    # This worker's end of each worker <-> task IPC pipe, index-aligned
    # with ``task_name`` and ``engine.task_process_list``. ``None`` for
    # stream tasks, which connect over a Unix-domain socket instead.
    task_pipe_conn: List[Optional[PipeConnection]] = field(default_factory=list)
    # Task names paired with ``task_pipe_conn``; ``task_name[j]`` is
    # the name of ``engine.task_process_list[j]``.
    task_name: List[str] = field(default_factory=list)

    @property
    def pid(self):
        """OS pid of the live child, or of this process after fork.

        Returns:
            ``process_instance.pid``. Raises ``AttributeError`` if called
            before the child has been started.
        """
        return self.process_instance.pid

    def send_msg(self, from_worker, data: str):
        """Push an inter-worker message onto this worker's write pipe.

        Called from the master via ``send_msg_to_worker``. Encodes
        ``from_worker`` plus ``data`` with ``encode_worker_msg_req`` and
        writes the frame to ``write_message_pipe``. The worker's
        ``WorkerMsgConnection`` reads it and invokes ``on_worker_message``.

        Args:
            from_worker: Sender worker id, forwarded to the callback.
            data: UTF-8 text payload delivered to ``on_worker_message``.
        """
        buf = encode_worker_msg_req(from_worker, data.encode("utf-8"))
        self.write_message_pipe.send(buf)


_current_worker: Optional[WorkerInfo] = None


def current_worker() -> Optional[WorkerInfo]:
    """Return the record of the worker running this code.

    Returns:
        The :class:`WorkerInfo` this process was forked for, or ``None`` when
        called outside a worker process (the master, a task or a dispatcher).
    """
    return _current_worker


def set_current_worker(worker: Optional[WorkerInfo]) -> None:
    """Mark this process as running for ``worker``; called right after fork.

    Args:
        worker: Record of the worker this process serves.
    """
    global _current_worker  # pylint: disable=global-statement
    _current_worker = worker


class BaseWorker(BaseChildProcess):
    """Post-fork runtime shared by every worker flavour.

    Subclasses only implement :meth:`start_worker_process`, which connects the
    worker to whatever feeds it requests: the dispatcher pipe, or, for
    ``multi-listener``, the listen sockets it owns. Everything else — task
    connections, the inter-worker message pipe, the timer executor and the
    port-to-service mapping — is set up here.
    """

    def __init__(self, worker: WorkerInfo, engine):
        """Copy the master-side record into this child; no sockets are bound yet.

        Runs in the master **before fork**. The event loop, plugin setup and
        IPC wiring happen later in :meth:`run` / :meth:`_prepare_to_serve`.

        Args:
            worker: Pre-fork record with this worker's id and IPC pipe ends.
            engine: Master engine that owns the task list, listen addresses
                and ``on_worker_message``. The child inherits this object
                through fork.
        """
        super().__init__(f"worker-{worker.id}", ProcessType.WORKER)
        # Master engine that forked this child; used for the task list and
        # ``on_worker_message``.
        self.server_engine = engine
        # Master-side record for this worker (id, pipes, ``process_instance``).
        # ``current_worker()`` returns this object after fork.
        self.worker = worker

        # This worker's end of each task IPC pipe; index-aligned with
        # ``task_name`` and ``engine.task_process_list``. ``None`` for stream
        # tasks, which connect over a Unix socket instead.
        self.task_pipe_conn: List[Optional[PipeConnection]] = worker.task_pipe_conn
        # Task names paired with ``task_pipe_conn``.
        self.task_name: List[str] = worker.task_name

        # Public listen addresses as ``(host, port)``; parallel with handlers.
        self.addresses = engine.process_info.addresses
        # Socket type (TCP/UDP/UNIX) for each listen address.
        self.socket_types = engine.process_info.socket_types
        # Listen port -> service index. Dispatcher-tagged frames carry the
        # listen port; the worker uses this map to pick handler and timeout.
        # The first index wins if two services share a port.
        self.addresses_map: Dict[int, int] = {}
        if engine.process_info.addresses is not None:
            for i, address in enumerate(engine.process_info.addresses):
                if address[1] not in self.addresses_map:
                    self.addresses_map[address[1]] = i

        # Timer executor started after fork; drives worker timers.
        self.timer: Optional[TimerExecutor] = None

        # asyncio protocol for the inter-worker message pipe; set in
        # ``start_message_listener``.
        self.message_pipe_protocol: Optional[asyncio.Protocol] = None
        # asyncio transport for that same message pipe.
        self.message_pipe_transport: Optional[asyncio.BaseTransport] = None
        # This worker's end of the dispatcher req/rsp IPC pipe.
        self.worker_pipe_conn: Optional[PipeConnection] = worker.pipe_conn
        # This worker's end of the RPCZ span side-channel pipe.
        self.rpcz_pipe_conn: Optional[PipeConnection] = worker.rpcz_pipe_conn

        # Request handlers for each listen address.
        self.handlers = engine.process_info.handlers
        # Idle/request timeout (ms) for each listen address.
        self.timeouts = engine.process_info.timeouts
        # Transport options for each listen address (TLS, unix path, ...).
        self.tran_opts = engine.process_info.transport_opt
        # Whether tracemalloc-based allocation snapshots are enabled.
        self.enable_memory_inspection = TrpcConfig.server_config().memory_inspection.enabled
        if self.enable_memory_inspection:
            # Import lazily: ``trpc_sdk._inner.MemoryInspecter`` pulls in
            # tracemalloc and its hash tables, which cost memory even when unused.
            from trpc_sdk._inner import MemoryInspecter
            # Writes allocation snapshots to a file on SIGUSR2.
            self.memory_inspecter = MemoryInspecter(
                TrpcConfig.server_config().memory_inspection.filename_prefix,
                TrpcConfig.server_config().memory_inspection.line_limit,
            )

    def _create_pipe_conn(self, pipe_conn, task_info: TaskInfo):
        """Attach this worker's end of a non-stream task IPC pipe to the loop.

        Drops the unused socketpair end inherited across fork, wraps the
        remaining fd in ``CTaskConnection`` or ``TaskConnection``, and
        registers the transport under ``task_info.name`` so handlers can
        produce to that task.

        Args:
            pipe_conn: This worker's ``RIGHT_PORT`` end of the worker <->
                task socketpair. Must not be ``None``.
            task_info: The task this pipe talks to; ``use_c_task`` selects
                the protocol class.
        """
        pipe_conn.close_extra_conn()
        if task_info.use_c_task:
            coros = self.loop.create_connection(CTaskConnection, sock=pipe_conn.get_socket())
        else:
            coros = self.loop.create_connection(TaskConnection, sock=pipe_conn.get_socket())
        (transport, _) = self.loop.run_until_complete(coros)
        register_task_conn(task_info.name, transport)

    def _create_unix_conn(self, task_info: TaskInfo):
        """Point the stream-task client at the Unix socket the task will bind.

        Stream tasks do not share a socketpair. After fork the task listens
        on ``.__{name}`` and this worker's client connects there. The client
        is stored in :class:`TaskStreamClientMap` so handlers can look it
        up by name.

        Args:
            task_info: Stream task whose ``task_stream_handler.client`` is
                configured here. The matching ``task_pipe_conn`` entry is
                ``None``.
        """
        task_stream_client = task_info.task_stream_handler.client
        task_stream_client.set_unix_path(f".__{task_info.name}")
        TaskStreamClientMap.register_task_stream_client(task_info.name, task_stream_client)

    def _before_event_loop(self) -> None:
        """Start tracemalloc before the event loop (or anything else) allocates.

        First hook inside :meth:`run`. A no-op when memory inspection is
        off. Starting later would miss allocations from loop and plugin
        setup.
        """
        if self.enable_memory_inspection:
            self.memory_inspecter.start()

    def _init_plugins(self) -> None:
        """Initialize every plugin, including business ones.

        Overrides the base ``init_task_plugins()`` used by dispatcher and
        task children. This process runs user handlers, so filters, naming,
        metrics and the rest must be fully set up.
        """
        # TODO: Need to check
        # init_worker_plugins()
        plugin.setup()

    def _prepare_to_serve(self) -> None:
        """Wire task IPC, the message pipe, the request source, then timers.

        Runs after plugins are initialized and before ``loop.run_forever()``.
        Order matters:

        1. Publish this process as ``current_worker()`` so
           ``send_msg_worker`` can tag messages with this worker's id.
        2. Connect to each task (pipe, or Unix socket for stream tasks).
        3. If ``on_worker_message`` is set, listen on ``read_message_pipe``.
        4. :meth:`start_worker_process` — subclass attaches to the
           dispatcher pipe or binds listen sockets.
        5. Start the timer executor.

        Uses ``loop.run_until_complete`` for connection setup, so the loop
        must already exist.
        """
        self.worker.process_instance = self
        set_current_worker(self.worker)
        self.timer = TimerExecutor(loop=self.loop, name=f"worker:{self.worker.id}")

        for i, pipe_conn in enumerate(self.task_pipe_conn):
            task_info: TaskInfo = self.server_engine.task_process_list[i]
            if task_info.task_stream_handler:
                assert pipe_conn is None
                self._create_unix_conn(task_info)
            else:
                self._create_pipe_conn(task_info=task_info, pipe_conn=pipe_conn)

        # Only listen when the user registered a callback; otherwise the
        # RIGHT end is never written and this pipe stays idle.
        if self.server_engine.on_worker_message:
            self.start_message_listener(self.server_engine)

        self.start_worker_process()
        self.timer.run()

    def _register_signal_handlers(self) -> None:
        """Install SIGTERM, and SIGUSR2 when memory inspection is on.

        SIGTERM still goes through :meth:`_on_sigterm_received`. SIGUSR2
        dumps a tracemalloc snapshot to the configured file prefix.
        """
        super()._register_signal_handlers()
        # User can get memory allocation information through SIGUSR2 signal
        if self.enable_memory_inspection:
            self.loop.add_signal_handler(signal.SIGUSR2, self.memory_inspecter.print_to_file)

    def _on_sigterm_received(self) -> None:
        """Run user worker-exit callbacks, then log the exit.

        Invoked on the event-loop signal path, so callbacks must not block.
        ``sys.exit()`` is issued by the base :meth:`_handle_sigterm` after
        this returns.
        """
        run_worker_signal_handle()
        logger.error("[worker] worker exit")

    def start_worker_process(self):
        """Connect this worker to whatever feeds it requests.

        No-op here. :class:`WorkerProcess` / :class:`CWorkerProcess` attach
        to the dispatcher IPC pipe; :class:`ExecutorProcess` binds the
        public listen sockets itself (multi-listener).
        """
        pass

    def start_message_listener(self, engine):
        """Listen on this worker's inter-worker message pipe.

        Wraps ``read_message_pipe`` (LEFT) in :class:`WorkerMsgConnection`.
        Frames written to the RIGHT end — typically by another worker via
        ``send_msg_worker``, using an fd inherited at fork — are decoded
        and handed to ``engine.on_worker_message(from_id, data)``. The
        master process is not on the data path; it only created the
        socketpair before fork.

        Args:
            engine: Server engine whose ``on_worker_message`` callback is
                invoked for each decoded frame.
        """
        handle = partial(WorkerMsgConnection, engine=engine)
        coro = self.loop.create_connection(handle, sock=self.worker.read_message_pipe.get_socket())
        (transport, protocol) = self.loop.run_until_complete(coro)
        self.message_pipe_transport = transport
        self.message_pipe_protocol = protocol


class WorkerProcess(BaseWorker):
    """Worker paired with the Python dispatcher (``dispatcher`` mode).

    This process owns no listen socket. The dispatcher accepts client
    connections, reads whole frames off them, tags each frame with the fd,
    session id and the listen port it arrived on, and forwards it over the
    req/rsp IPC pipe created before fork. This worker decodes the frame, runs
    the service handler and writes the response back on that same pipe, where
    the dispatcher matches it to the original client fd.

    The pipe is a stream socketpair, so frames are re-assembled by
    ``InnerParser`` inside :class:`WorkerConnection`; contrast with
    :class:`CWorkerProcess`, which by default reads datagrams instead.
    """

    def start_worker_process(self):
        """Attach the dispatcher req/rsp pipe to this worker's event loop.

        Called by :meth:`BaseWorker._prepare_to_serve` once plugins and task
        connections are up, and it is the point where this process starts
        accepting work.

        Steps:

        1. Drop the dispatcher-side ends of both socketpairs. Fork copied
           every fd, so without this the worker would hold the write end of
           its own pipes open and never see EOF when the dispatcher dies.
        2. Wrap the request pipe in :class:`WorkerConnection`.
           ``addresses_map`` maps the listen port carried by each frame back
           to a service index, which selects the entry in ``handlers``,
           ``timeouts`` and ``tran_opts`` used to serve it.

        The RPCZ pipe is only closed here, never wrapped in a protocol: this
        end is write-only. :class:`WorkerConnection` calls
        ``app.rpcz_pipe_conn.send()`` directly with each finished span, and
        the reading side lives in the dispatcher.
        """
        logger.info("[worker] WorkerProcess start_worker_process()")
        self.worker_pipe_conn.close_extra_conn()
        self.rpcz_pipe_conn.close_extra_conn()
        handle = partial(WorkerConnection,
                         app=self,
                         addresses=self.addresses_map,
                         handlers=self.handlers,
                         timeouts=self.timeouts,
                         tran_opts=self.tran_opts)
        coro = self.loop.create_connection(handle, sock=self.worker_pipe_conn.get_socket())
        self.loop.run_until_complete(coro)


class CWorkerProcess(BaseWorker):
    """Worker paired with the C dispatcher.

    Same division of labour as :class:`WorkerProcess` — the dispatcher owns
    the listen sockets and this process only runs handlers — but the frames
    come from the C dispatcher, so the pipe and the framing differ:

    * Default: the req/rsp pipe is a **datagram** socketpair. One datagram is
      one frame, and payloads larger than the socket buffer arrive as chunks
      that :class:`CWorkerConnection` re-assembles by session id.
    * With the trpcio driver enabled (:func:`use_trpcio`): a stream pipe
      driven by ``CWorkerConnectionTrpcio`` instead.

    There is no RPCZ side-channel in this mode; ``rpcz_pipe_conn`` is left
    untouched because the C path never emits spans.
    """

    def __init__(self, worker: WorkerInfo, engine):
        """Rename the process and prepare the peer-pipe list; no fd work yet.

        Runs in the master before fork, like the base constructor.

        Args:
            worker: Pre-fork record with this worker's id and IPC pipe ends.
            engine: Master engine, inherited through fork.
        """
        super().__init__(worker, engine)
        # Overrides the ``worker-{id}`` name set by BaseWorker so logs and ps
        # output tell the C flavour apart.
        self.name = f"cworker-{worker.id}"
        # LEFT ends of *every* worker's req/rsp pipe, not just this one,
        # rebuilt in ``start_worker_process`` from the socketpairs inherited
        # at fork. Kept alive for the C dispatcher's fd table.
        self.worker_pipe_list: List[PipeConnection] = []

    def start_worker_process(self):
        """Attach the C dispatcher req/rsp pipe to this worker's event loop.

        Called by :meth:`BaseWorker._prepare_to_serve`. Rebuilds
        ``worker_pipe_list`` from every entry of
        ``server_engine.worker_process_list``, drops the dispatcher-side end
        of this worker's own pipe, then binds the protocol that matches the
        transport:

        * trpcio on: ``create_connection`` with ``CWorkerConnectionTrpcio``
          (stream pipe). Imported lazily so builds without the trpcio
          extension can still run the default path.
        * trpcio off: ``create_datagram_endpoint`` with
          :class:`CWorkerConnection` (datagram pipe).
        """
        logger.info("[worker] CWorkerProcess start_worker_process()")
        for worker in self.server_engine.worker_process_list:
            worker_pipe = worker.pipe_conn
            pipe_conn = PipeConnection(worker_pipe.socket_pair, LEFT_PORT)
            self.worker_pipe_list.append(pipe_conn)

        self.worker_pipe_conn.close_extra_conn()

        if use_trpcio():
            from trpc_sdk.transport import CWorkerConnectionTrpcio
            handle = partial(CWorkerConnectionTrpcio,
                             app=self,
                             addresses=self.addresses_map,
                             handlers=self.handlers,
                             timeouts=self.timeouts)
            coro = self.loop.create_connection(handle, sock=self.worker_pipe_conn.get_socket())
        else:
            handle = partial(CWorkerConnection,
                             app=self,
                             addresses=self.addresses_map,
                             handlers=self.handlers,
                             timeouts=self.timeouts)
            coro = self.loop.create_datagram_endpoint(handle, sock=self.worker_pipe_conn.get_socket())

        self.loop.run_until_complete(coro)
