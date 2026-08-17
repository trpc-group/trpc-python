# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Master-side process engine: build the topology, fork children, restart them.

:class:`TrpcServerEngine` lives in the master process. It does not listen for
clients and does not run service handlers. It fills pre-fork records
(:class:`~trpc.server._worker.WorkerInfo`,
:class:`~trpc.server._dispatcher.DispatcherInfo`,
:class:`~trpc.server._task.TaskInfo`), creates every IPC pipe, then forks the
children that do the work.

Which children exist depends on ``engine_mode``:

* ``dispatcher`` (default) — one dispatcher owns the public listen sockets
  and forwards complete frames to N workers over IPC.
* ``multi-listener`` — no dispatcher; each worker is an
  :class:`~trpc.server._executor.ExecutorProcess` that listens for itself.

Startup is split so pipes exist before listen addresses do:

1. ``init_processes`` builds task/worker records and the worker-side pipes.
   ``Service.run`` then appends listen endpoints onto ``process_info``.
2. ``start_processes`` assigns ``GLOBAL_SERVER_ENGINE``, runs
   ``fork_hook.execute_before``, forks tasks then timers, wraps the
   dispatcher onto ``RIGHT_PORT`` of each worker pipe, forks workers, and
   forks the dispatcher last. ``fork_hook.execute_after_in_parent`` runs
   after every child exists.

Worker req/rsp and RPCZ pipes are created in ``init_worker_process``
**before fork**. The worker keeps ``LEFT_PORT``; the dispatcher is bound to
``RIGHT_PORT`` later. The req/rsp pair is a datagram pipe (``UDP_PIPE``)
when the C dispatcher is on without trpcio, otherwise a stream pipe
(``TCP_PIPE``). RPCZ and inter-worker message pipes are always stream.

``GLOBAL_SERVER_ENGINE`` is set to this instance **before** any fork, so
child processes inherit the reference. That is why
:func:`trpc.server.send_msg_worker` can reach ``worker_process_list`` from
inside a worker.
"""

from __future__ import annotations

import asyncio
import os
import signal
import threading
from functools import partial
from multiprocessing import Process
from typing import Callable
from typing import Dict
from typing import List
from typing import Optional
from typing import Tuple
from typing import Type

from trpc import fork_hook
from trpc.log import logger
from trpc.plugin import get_registered_tasks
from trpc.registry import pause_heartbeat
from trpc.task.task_util import register_ctask_name
from trpc.task.task_util import register_task_num
from trpc.trpc_abc.codec import Parser
from trpc.trpc_abc.server import BaseService
from trpc.trpc_abc.server import BaseTrpcServerEngine
from trpc.trpc_abc.transport import BaseServerTransportOption
from trpc.utils import get_or_create_event_loop
from trpc.utils import is_unix_socket
from trpc.utils import tcp_port_valid
from trpc.utils import udp_port_valid
from trpc.utils import unix_path_valid
from trpc.utils.timer import TimerProcessInfo
from trpc.utils.timer import get_register_timer_process
from ._constants import LEFT_PORT
from ._constants import NETWORK_HTTP
from ._constants import NETWORK_TCP
from ._constants import NETWORK_UDP
from ._constants import RIGHT_PORT
from ._constants import TCP_PIPE
from ._constants import UDP_PIPE
from ._dispatcher import DispatcherInfo
from ._dispatcher import DispatcherProcess
from ._executor import ExecutorProcess
from ._features import use_c_dispatcher
from ._features import use_trpcio
from ._pipe import PipeConnection
from ._pipe import create_pipe
from ._process_info import ProcessInfo
from ._task import CTaskProcess
from ._task import TaskInfo
from ._task import TaskProcess
from ._types import ServerMode
from ._worker import CWorkerProcess
from ._worker import WorkerInfo
from ._worker import WorkerProcess
from ._option import ServerOption


GLOBAL_SERVER_ENGINE: Optional[TrpcServerEngine] = None


class TrpcServerEngine(BaseTrpcServerEngine):
    """Master process controller for the tRPC server process tree.

    Holds the pre-fork records and the post-fork ``pid -> info`` maps used
    to restart children. Does not own listen sockets and does not run
    handlers; those live in the dispatcher / worker / executor children.
    """

    def __init__(self, opts: Optional[ServerOption] = None) -> None:
        """Record server options; no child is forked yet.

        Args:
            opts: Process-model options. ``service_num`` sizes
                ``registry_semaphore``. ``worker_num`` is consumed later by
                ``init_worker_process``.
        """
        self.opts: Optional[ServerOption] = opts
        # Master's event loop; filled in ``init_processes``. Children
        # create their own loops after fork.
        self.loop: Optional[asyncio.AbstractEventLoop] = None

        # ``DISPATCHER`` until ``set_executor_process`` switches to
        # ``MULTI_LISTENER``.
        self.engine_mode: ServerMode = ServerMode.DISPATCHER

        # Post-fork ``pid -> info`` maps used by ``SIGCHLD`` restart.
        self.sub_worker_processes: Dict[int, WorkerInfo] = {}
        self.sub_dispatcher_process: Dict[int, DispatcherInfo] = {}
        self.sub_worker_task_processes: Dict[int, TaskInfo] = {}
        self.sub_timer_processes: Dict[int, TimerProcessInfo] = {}

        # Pre-fork records: process class, pipes, and identity. Filled
        # before any ``Process.start()``.
        self.task_process_list: List[TaskInfo] = []
        self.worker_process_list: List[WorkerInfo] = []
        # Unused historical field; kept so existing attribute access
        # continues to work.
        self.dispatcher_list: List[DispatcherInfo] = []
        self.dispatcher: Optional[DispatcherInfo] = None

        # Process class for the dispatcher child. Chosen here by calling
        # ``use_c_dispatcher()`` (not cached at import). The Cython module
        # is imported lazily so the Python dispatcher path does not load it.
        self.dispatcher_handler: Type[Process]
        if use_c_dispatcher():
            from .c_dispatcher import CDispatcherProcess
            self.dispatcher_handler = CDispatcherProcess
        else:
            self.dispatcher_handler = DispatcherProcess

        # Listen/handler metadata appended by ``set_*_process`` during
        # ``Service.run``, after ``init_processes`` and before fork.
        self.process_info: ProcessInfo = ProcessInfo()

        # Custom worker process class; ``None`` falls back to the mode
        # default (``WorkerProcess`` / ``CWorkerProcess`` / ``ExecutorProcess``).
        self.work_handle: Optional[Type[Process]] = None

        # Each service ``close()`` releases once after deregister.
        # ``close()`` acquires ``service_num`` times before SIGTERM.
        self.registry_semaphore: threading.Semaphore = threading.Semaphore(self.opts.service_num)

    def init_processes(self) -> None:
        """Build pre-fork task and worker records, including worker-side pipes.

        Listen addresses are not known yet. ``Service.run`` fills
        ``process_info`` afterwards; ``start_processes`` then forks.
        """
        self.loop = get_or_create_event_loop()

        self.init_task_process()
        self.init_worker_process()

    @property
    def on_worker_message(self) -> Optional[Callable]:
        """Callback invoked when a worker receives an inter-worker message.

        Copied from ``ServerOption`` so child processes can read it after
        fork without holding the option object themselves.
        """
        if not self.opts:
            return None
        return self.opts.on_worker_message

    def check_server(self) -> None:
        """Reject an unknown ``engine_mode`` or a non-positive worker count.

        Raises:
            Exception: Mode is not dispatcher/multi-listener, or
                ``worker_num < 1``.
        """
        if self.engine_mode != ServerMode.DISPATCHER and self.engine_mode != ServerMode.MULTI_LISTENER:
            raise Exception("invalid engine mode")

        if self.opts.worker_num < 1:
            raise Exception("invalid worker number")

    def is_dispatcher(self) -> bool:
        """Return whether this engine is in dispatcher mode.

        Returns:
            True when ``engine_mode`` is ``ServerMode.DISPATCHER``.
        """
        return self.engine_mode == ServerMode.DISPATCHER

    def start_processes(self) -> None:
        """Fork task, timer, worker, and dispatcher children in that order.

        Sets ``GLOBAL_SERVER_ENGINE`` first so every subsequent fork
        inherits the reference. ``fork_hook.execute_before`` runs in the
        master before any child is created;
        ``fork_hook.execute_after_in_parent`` runs after the last fork.
        """
        global GLOBAL_SERVER_ENGINE
        GLOBAL_SERVER_ENGINE = self

        fork_hook.execute_before()

        self.check_server()

        self.start_task_process()

        self.start_timer_process()

        # Dispatcher pipe ends must exist before workers fork, so the
        # children inherit both sides of each pair.
        if self.is_dispatcher():
            self.init_dispatcher_process()

        self.start_new_worker_process()

        # Dispatcher forks last so its listen sockets come up after
        # workers are already waiting on the IPC pipes.
        if self.is_dispatcher():
            if use_c_dispatcher():
                logger.info("[Master Engine] tRPC-Python is using C dispatcher")
            self.start_new_dispatcher()

        fork_hook.execute_after_in_parent()

    def init_dispatcher_process(self) -> None:
        """Bind the dispatcher to the RIGHT end of each worker's existing pipes.

        ``init_worker_process`` already created two socketpairs per worker and
        kept ``LEFT_PORT`` on the worker (req/rsp pipe and RPCZ pipe). This
        wraps the same pairs with ``RIGHT_PORT`` so the dispatcher owns the
        other end. Lists stay index-aligned with ``worker_process_list``.

        Then fill ``self.dispatcher`` (``DispatcherInfo``): process class
        (``DispatcherProcess`` or ``CDispatcherProcess``) plus the two pipe
        lists. Does not fork; ``start_new_dispatcher`` does that later.
        """
        pipe_list = []
        rpcz_pipe_list = []
        # Stash the dispatcher at the RIGHT end of each worker's existing pipes.
        for worker in self.worker_process_list:
            worker_pipe = worker.pipe_conn
            pipe_conn = PipeConnection(worker_pipe.socket_pair, RIGHT_PORT)
            pipe_list.append(pipe_conn)

            worker_rpcz_pipe = worker.rpcz_pipe_conn
            rpcz_pipe_conn = PipeConnection(worker_rpcz_pipe.socket_pair, RIGHT_PORT)
            rpcz_pipe_list.append(rpcz_pipe_conn)

        # Fill dispatcher info for use by `start_new_dispatcher`.
        self.dispatcher = DispatcherInfo()
        self.dispatcher.handle = self.dispatcher_handler
        self.dispatcher.worker_pipe_conn_list = pipe_list
        self.dispatcher.rpcz_pipe_conn_list = rpcz_pipe_list

    def init_task_process(self) -> None:
        """Copy registered task plugins into pre-fork ``TaskInfo`` records.

        Does not create pipes or fork. ``pause_heartbeat()`` is called here
        when a task asks for it, still in the master, so the registry client
        stops before any child is created.
        """
        task_handlers = get_registered_tasks()
        if len(task_handlers) == 0:
            return

        for task_handler in task_handlers:
            register_task_num(task_handler.name, task_handler.task_num)
            if task_handler.use_c_task:
                handle = CTaskProcess
                enable = len(task_handler.c_task_driver_params) > 0
                register_ctask_name(task_handler.name, enable)
            else:
                handle = TaskProcess

            for i in range(task_handler.task_num):
                task = TaskInfo()
                task.task_stream_handler = task_handler.task_stream_handler
                task.name = f"{task_handler.name}:{i}"
                task.handle = handle
                task.task_handle = (task_handler.initializer, task_handler.handler)
                task.batch_size = task_handler.batch_size
                task.thread_num = task_handler.thread_num
                task.model_info = task_handler.model_info
                task.use_c_task = task_handler.use_c_task
                task.c_task_driver_params = task_handler.c_task_driver_params
                task.on_fork = task_handler.on_fork
                task.pause_heartbeat = task_handler.pause_heartbeat
                if task.pause_heartbeat:
                    pause_heartbeat()
                self.task_process_list.append(task)

    def init_worker_process(self) -> None:
        """Allocate one ``WorkerInfo`` per worker and create its pipes.

        All pipes are created **before fork**. For each worker:

        * Inter-worker message pipe: always stream. Worker reads
          ``LEFT_PORT``; the master (and ``send_msg``) writes ``RIGHT_PORT``.
        * Req/rsp pipe to the dispatcher: worker keeps ``LEFT_PORT``.
          Datagram (``UDP_PIPE``) when the C dispatcher is on without
          trpcio, otherwise stream (``TCP_PIPE``).
        * RPCZ span pipe: always stream; worker keeps ``LEFT_PORT``.

        Raises:
            Exception: ``worker_num`` is 0.
        """
        if self.opts.worker_num == 0:
            raise Exception("invalid worker_num")

        # The C dispatcher exchanges frames with workers over datagram pipes,
        # unless the trpcio driver is enabled, which keeps them as stream pipes.
        if use_c_dispatcher() and not use_trpcio():
            pipe_type = UDP_PIPE
        else:
            pipe_type = TCP_PIPE

        for worker_id in range(0, self.opts.worker_num):
            worker = WorkerInfo(id=worker_id)
            # Inter-worker broadcast pipe: worker reads LEFT, master writes RIGHT.
            message_pipe = create_pipe(TCP_PIPE)
            worker.read_message_pipe = PipeConnection(message_pipe, LEFT_PORT)
            worker.write_message_pipe = PipeConnection(message_pipe, RIGHT_PORT)

            # Dispatcher req/rsp + RPCZ: worker keeps LEFT; dispatcher
            # wraps RIGHT later in ``init_dispatcher_process``.
            worker.socket_pair = create_pipe(pipe_type)
            worker.pipe_conn = PipeConnection(worker.socket_pair, LEFT_PORT)
            worker.rpcz_socket_pair = create_pipe(TCP_PIPE)
            worker.rpcz_pipe_conn = PipeConnection(worker.rpcz_socket_pair, LEFT_PORT)

            self.worker_process_list.append(worker)

    def start_task_process(self) -> None:
        """Create worker <-> task IPC pipes, then fork each task process.

        For every ``(task, worker)`` pair, build one socketpair before fork so
        each worker can talk to that task independently:

        - No ``task_stream_handler``: ``LEFT_PORT`` stays on the task,
          ``RIGHT_PORT`` on the worker; both ends are non-blocking.
        - Stream task: skip the pipe; worker and task later connect over a
          Unix-domain socket instead.

        Lists stay index-aligned: ``task.task_pipe_conn[i]`` is the task end
        toward ``worker_process_list[i]``, and ``worker.task_pipe_conn[j]`` is
        the worker end toward ``task_process_list[j]``. After the mesh for a
        task is registered, ``spawn_task_process`` forks that task.
        """
        for task in self.task_process_list:
            # Register this task's IPC endpoint on every worker before fork.
            # Non-stream tasks get a socketpair (task=LEFT, worker=RIGHT);
            # stream tasks leave both ends None and connect later via UDS.
            # Appends stay index-aligned: task.task_pipe_conn[i] <-> worker i,
            # worker.task_pipe_conn[j] / worker.task_name[j] <-> task j.
            for worker in self.worker_process_list:
                if task.task_stream_handler is None:
                    socket_pair = create_pipe(TCP_PIPE)
                    socket_pair[0].setblocking(False)
                    socket_pair[1].setblocking(False)
                    t_pipe_conn = PipeConnection(socket_pair, LEFT_PORT)
                    w_pipe_conn = PipeConnection(socket_pair, RIGHT_PORT)
                else:
                    t_pipe_conn = None
                    w_pipe_conn = None
                task.task_pipe_conn.append(t_pipe_conn)
                worker.task_pipe_conn.append(w_pipe_conn)
                worker.task_name.append(task.name)
            self.spawn_task_process(task)

    def start_timer_process(self) -> None:
        """Fork each timer process.

        For every ``timer_info``, ``spawn_timer_process`` forks that timer.
        """
        for timer_info in get_register_timer_process().values():
            self.spawn_timer_process(timer_info)

    def start_new_worker_process(self) -> None:
        """Pick a process class for each worker, then fork.

        Pipes already exist from ``init_worker_process``. This only chooses
        ``worker.handle`` (a ``multiprocessing.Process`` subclass) and spawns:

        - ``self.work_handle`` set: use that custom process class.
        - Else dispatcher mode: ``WorkerProcess`` (Python) or ``CWorkerProcess``.
        - Else multi-listener: ``ExecutorProcess`` (each worker listens itself).
        """
        for worker in self.worker_process_list:
            if self.work_handle is None:
                if self.is_dispatcher():
                    worker.handle = CWorkerProcess if use_c_dispatcher() else WorkerProcess
                else:
                    # If multi-listener mode, use executor process instead of worker process.
                    worker.handle = ExecutorProcess
            else:
                worker.handle = self.work_handle
            self.spawn_new_worker_process(worker)

    def start_new_dispatcher(self) -> None:
        """Fork the dispatcher from the already-filled ``self.dispatcher`` record."""
        self.spawn_new_dispatcher_process(self.dispatcher)

    def spawn_new_worker_process(self, worker: WorkerInfo) -> WorkerInfo:
        """Fork one worker and index it in ``sub_worker_processes`` by pid.

        Args:
            worker: Pre-fork record whose ``handle`` is the process class.

        Returns:
            The same ``worker``, now with ``process_instance`` set.
        """
        spawn_function = partial(worker.handle, worker, self)
        worker_process = spawn_function()
        worker_process.start()

        worker.process_instance = worker_process
        self.sub_worker_processes[worker_process.pid] = worker

        logger.info("[Master Engine] started worker process: %s", worker_process.pid)

        return worker

    def spawn_task_process(self, task: TaskInfo) -> TaskInfo:
        """Fork one task and index it in ``sub_worker_task_processes`` by pid.

        Args:
            task: Pre-fork record whose ``handle`` is the process class.

        Returns:
            The same ``task``, now with ``process_instance`` set.
        """
        spawn_function = partial(task.handle, task, self)
        task_process = spawn_function()
        task_process.start()

        task.process_instance = task_process
        self.sub_worker_task_processes[task_process.pid] = task

        logger.info("[Master Engine] started task process: %s", task_process.pid)

        return task

    def spawn_timer_process(self, timer_info: TimerProcessInfo) -> None:
        """Fork one timer and index it in ``sub_timer_processes`` by pid.

        Args:
            timer_info: Registered timer record; ``timer_handle`` is the
                process class.
        """
        spawn_function = partial(timer_info.timer_handle, timer_info)
        timer_process = spawn_function()
        timer_process.start()

        timer_info.process_instance = timer_process
        self.sub_timer_processes[timer_process.pid] = timer_info

        logger.info("[Master Engine] started timer process: %s", timer_process.pid)

    def spawn_new_dispatcher_process(self, dispatcher: DispatcherInfo) -> DispatcherInfo:
        """Fork the dispatcher and index it in ``sub_dispatcher_process`` by pid.

        Args:
            dispatcher: Pre-fork record whose ``handle`` is the process class.

        Returns:
            The same ``dispatcher``, now with ``process_instance`` set.
        """
        spawn_function = partial(dispatcher.handle, dispatcher, self)

        dispatcher_process = spawn_function()
        dispatcher_process.start()

        dispatcher.process_instance = dispatcher_process
        self.sub_dispatcher_process[dispatcher_process.pid] = dispatcher

        logger.info("[Master Engine] started dispatcher process: %s", dispatcher_process.pid)

        return dispatcher

    def restart_task_process(self, task: TaskInfo) -> TaskInfo:
        """Fork a replacement for a dead task, reusing its pre-fork record.

        Args:
            task: Record of the task that exited; pipes are inherited
                from the original ``init`` / ``start_task_process`` pass.

        Returns:
            The same ``task`` after ``spawn_task_process``.
        """
        return self.spawn_task_process(task)

    def restart_timer_process(self, timer_info: TimerProcessInfo) -> None:
        """Fork a replacement for a dead timer, reusing its registration.

        Args:
            timer_info: Record of the timer that exited.
        """
        return self.spawn_timer_process(timer_info)

    def restart_worker_process(self, process_info: WorkerInfo) -> WorkerInfo:
        """Fork a replacement for a dead worker, reusing its pre-fork pipes.

        Args:
            process_info: Record of the worker that exited.

        Returns:
            The same record after ``spawn_new_worker_process``.
        """
        return self.spawn_new_worker_process(process_info)

    def restart_child_process(self, pid: int) -> None:
        """Replace an exited child, or refuse to restart the dispatcher.

        Drops ``pid`` from the corresponding map first so a second
        ``SIGCHLD`` cannot spawn two replacements. A dispatcher pid is
        removed and logged; it is never forked again.

        Args:
            pid: OS pid of the child that exited.
        """
        if pid in self.sub_dispatcher_process:
            del self.sub_dispatcher_process[pid]
            logger.warning("[Master Engine] dispatcher should never get restarted, pre-pid %s", str(pid))
        elif pid in self.sub_worker_processes:
            worker = self.sub_worker_processes[pid]
            del self.sub_worker_processes[pid]
            self.restart_worker_process(worker)
            logger.warning("[Master Engine] restart worker, pre-pid %s, cur-pid %s", str(pid), worker.pid)
        elif pid in self.sub_worker_task_processes:
            task = self.sub_worker_task_processes[pid]
            del self.sub_worker_task_processes[pid]
            self.restart_task_process(task)
            logger.warning("[Master Engine] restart dispatcher task, pre-pid %s, cur-pid %s", str(pid), task.pid)
        elif pid in self.sub_timer_processes:
            timer_info = self.sub_timer_processes[pid]
            del self.sub_timer_processes[pid]
            self.restart_timer_process(timer_info)
            logger.warning("[Master Engine] restart dispatcher task, pre-pid %s, cur-pid %s", str(pid), timer_info.pid)
        else:
            # error
            logger.error("[Master Engine] error! restart process - %s is not valid", str(pid))

    def set_dispatcher_process(self, address: Tuple[str, int], socket_type: str, checker: Parser, timeout: int,
                               network: str) -> None:
        """Append one dispatcher listen endpoint onto ``process_info``.

        Does not change ``engine_mode`` (already ``DISPATCHER`` by default).
        Validates unix path or TCP bind address; UDP is not accepted here.

        Args:
            address: ``(host, port)`` to listen on.
            socket_type: Socket family (unix vs net).
            checker: Frame-integrity parser for inbound connections.
            timeout: Idle/request timeout in milliseconds.
            network: Network protocol label stored with the endpoint.
        """
        # check socket info valid
        if is_unix_socket(socket_type):
            if not unix_path_valid(address[0]):
                logger.error("[Master Engine] cannot unlink unix address %s", address[0])
                exit()
        else:
            if not tcp_port_valid(address[0], address[1]):
                logger.error("[Master Engine] tcp address %s invalid", address)
                exit()

        self.process_info.add_address(address)
        self.process_info.add_socket_type(socket_type)
        self.process_info.add_timeout(timeout)
        self.process_info.add_checker(checker)
        self.process_info.add_network(network)

    def set_worker_process(self, handler: Callable, service: BaseService, opts: BaseServerTransportOption) -> None:
        """Append the handler/service/transport for one dispatcher endpoint.

        Called alongside ``set_dispatcher_process``; the lists on
        ``process_info`` stay index-aligned.

        Args:
            handler: Per-request handler for this listen endpoint.
            service: Service adapter bound to that endpoint.
            opts: Transport options for that endpoint.
        """
        self.process_info.add_handler(handler)
        self.process_info.add_service(service)
        self.process_info.add_transport_opt(opts)

    def set_executor_process(self,
                             address: Tuple[str, int],
                             socket_type: str,
                             handler: Callable,
                             checker: Parser,
                             timeout: int,
                             network: str,
                             service: BaseService,
                             tran_opt: BaseServerTransportOption,
                             target: str = '') -> None:
        """Switch to multi-listener mode and append one executor endpoint.

        Sets ``engine_mode`` to ``MULTI_LISTENER``. Unlike
        ``set_dispatcher_process``, TCP/HTTP and UDP binds are both
        accepted.

        Args:
            address: ``(host, port)`` to listen on.
            socket_type: Socket family (unix vs net).
            handler: Per-request handler for this listen endpoint.
            checker: Frame-integrity parser for inbound connections.
            timeout: Idle/request timeout in milliseconds.
            network: Network protocol (tcp/http/udp).
            service: Service adapter bound to that endpoint.
            tran_opt: Transport options for that endpoint.
            target: Optional custom address stored beside the bind address.
        """
        self.engine_mode = ServerMode.MULTI_LISTENER

        # check socket info valid
        if is_unix_socket(socket_type):
            if not unix_path_valid(address[0]):
                logger.error("[Master Engine] cannot unlink unix address %s", address[0])
                exit()
        else:
            if network.find(NETWORK_TCP) != -1 or network.find(NETWORK_HTTP) != -1:
                if not tcp_port_valid(address[0], address[1]):
                    logger.error("[Master Engine] tcp address %s invalid", address)
                    exit()
            elif network.find(NETWORK_UDP) != -1:
                if not udp_port_valid(address[0], address[1]):
                    logger.error("[Master Engine] udp address %s invalid", address)
                    exit()
            else:
                logger.error("[Master Engine] unsupported network %s", network)
                exit()

        self.process_info.add_address(address)
        self.process_info.add_socket_type(socket_type)
        self.process_info.add_handler(handler)
        self.process_info.add_checker(checker)
        self.process_info.add_timeout(timeout)
        self.process_info.add_network(network)
        self.process_info.add_service(service)
        self.process_info.add_transport_opt(tran_opt)
        self.process_info.add_target(target)

    def send_msg_to_worker(self, from_id: int, to: int, data: str) -> Optional[bool]:
        """Send an inter-worker message, or broadcast when ``to`` is negative.

        Out-of-range unicast (``to`` past the last worker) returns ``False``
        and sends nothing. A successful unicast or broadcast returns
        ``None``.

        Args:
            from_id: Sending worker id, written into the frame.
            to: Target worker id, or a negative value to broadcast to every
                worker except the sender.
            data: Payload; encoded as UTF-8 on the write end of the
                target's message pipe.

        Returns:
            ``False`` if ``to`` is out of range; otherwise ``None``.
        """
        if len(self.worker_process_list) <= to:
            return False

        if to < 0:
            self.broadcast_msg_to_worker(from_id, data)
        else:
            self.worker_process_list[to].send_msg(from_id, data)

    def broadcast_msg_to_worker(self, from_id: int, data: str) -> None:
        """Write ``data`` to every worker except the sender.

        Args:
            from_id: Sending worker id; that worker is skipped.
            data: Payload forwarded through each remaining worker's
                message pipe.
        """
        for worker in self.worker_process_list:
            if worker.id == from_id:
                continue
            worker.send_msg(from_id, data)

    def close(self) -> None:
        """Wait for service deregister, then SIGTERM every live child.

        Acquires ``registry_semaphore`` once per configured service (10s
        timeout each) so ``Service.close`` can finish registry logout
        before the children disappear. Kill order is dispatcher, workers,
        tasks, timers.
        """
        for _ in range(self.opts.service_num):
            self.registry_semaphore.acquire(True, 10)

        for pid in self.sub_dispatcher_process:
            if self.sub_dispatcher_process[pid].process_instance.is_alive():
                logger.warning("[Master Engine] killing child dispatcher process-%s", pid)
                self.close_child(pid)

        for pid in self.sub_worker_processes:
            if self.sub_worker_processes[pid].process_instance.is_alive():
                logger.warning("[Master Engine] killing child worker process-%s", pid)
                self.close_child(pid)

        for pid in self.sub_worker_task_processes:
            if self.sub_worker_task_processes[pid].process_instance.is_alive():
                logger.warning("[Master Engine] killing child task process-%s", pid)
                self.close_child(pid)

        for pid in self.sub_timer_processes:
            if self.sub_timer_processes[pid].process_instance.is_alive():
                logger.warning("[Master Engine] killing child timer process-%s", pid)
                self.close_child(pid)

    @staticmethod
    def close_child(pid: int) -> None:
        """Send SIGTERM to ``pid`` and wait for its exit status.

        Args:
            pid: Child OS pid to terminate.
        """
        try:
            os.kill(pid, signal.SIGTERM)
            # Block until the child reaps so the master does not leave zombies.
            _, status = os.waitpid(pid, 0)
            exit_status = status >> 8  # WEXITSTATUS
            logger.info("[Master Engine] child exit status %s", str(exit_status))
        except Exception as err:  # pylint: disable=broad-except
            logger.warning("[Master Engine] received exception when closing child, err: %s", str(err))

    def find_child_process(self, pid: int) -> bool:
        """Return whether ``pid`` is a dispatcher, worker, or task child.

        Args:
            pid: OS pid reported by ``SIGCHLD``.

        Returns:
            True if ``pid`` is in ``sub_dispatcher_process``,
            ``sub_worker_processes``, or ``sub_worker_task_processes``.
        """
        return (pid in self.sub_dispatcher_process) or (pid in self.sub_worker_processes) or (
            pid in self.sub_worker_task_processes) or (pid in self.sub_worker_task_processes)
