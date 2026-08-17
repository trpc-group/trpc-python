# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Master process: service table, startup order, signals, and graceful shutdown.

:class:`Server` is the object application code holds after :func:`trpc.new`.
It does not listen for clients and does not run handlers. Those live in the
children forked by :class:`~trpc.server._engine.TrpcServerEngine`.
This class owns the ``service_name -> Service`` map, the master's event loop,
and the SIGTERM / SIGCHLD path that tears the process tree down.

``serve()`` is ordered so pipes exist before listen addresses, and so signal
handlers are installed only after fork:

1. Force ``multiprocessing`` start method ``fork``.
2. ``engine.init_processes`` builds task/worker records and worker-side pipes.
3. ``Service.run`` appends listen endpoints onto ``process_info``.
4. ``init_healthy_checker`` runs **before fork** so children inherit it.
5. ``engine.start_processes`` forks the process tree.
6. ``_handle_sys_signals`` registers OS handlers. Doing this before fork
   breaks ``asyncio.subprocess``; uvloop also has no
   ``loop.add_signal_handler``.
7. The master loop runs until a signal or ``KeyboardInterrupt``.

``self.close`` is a boolean, not ``engine.close``. Once the master starts
shutting down it is set to True so ``SIGCHLD`` does not restart children
that were just killed.
"""

from __future__ import annotations

import asyncio
import multiprocessing
import os
import signal
import threading
import time
from abc import ABCMeta
from abc import abstractmethod
from functools import partial
from typing import Any
from typing import Dict
from typing import Iterable
from typing import Optional

from trpc import fork_hook
from trpc.log import logger
from trpc.share_mem import wait_notify_in_io
from trpc.trpc_abc.server import ServiceDesc
from trpc.utils import get_or_create_event_loop

from ._healthy_checker import init_healthy_checker
from ._signals import run_server_signal_handle
from ._option import ServerOption
from ._engine import TrpcServerEngine
from ._service import Service


class AbstractServer(metaclass=ABCMeta):
    """Minimal server interface: start serving and block."""

    @abstractmethod
    def serve(self) -> None:
        """Start the server and run until shutdown.

        Args:
            None.
        """


class Server(AbstractServer):
    """Master-process controller for a tRPC server.

    Holds the service table, :class:`ServerOption`, and the
    :class:`TrpcServerEngine` that builds and forks the process tree.
    Does not own listen sockets and does not run handlers.
    """

    def __init__(self, server_option: ServerOption) -> None:
        """Record options and create the engine; no child is forked yet.

        Args:
            server_option: Process-model options, including
                ``reboot_child`` and ``close_wait_time``.
        """
        # ``service_name -> Service``, filled by ``trpc.new`` / ``add_service``.
        self.services: Dict[str, Service] = {}
        self.opt: ServerOption = server_option
        # True once the master started shutting down; suppresses SIGCHLD restarts.
        self.close: bool = False
        # Process topology, IPC pipes, fork and child restart.
        self.server_engine: TrpcServerEngine = TrpcServerEngine(self.opt)

    def add_service(self, service_name: str, service: Service) -> None:
        """Attach one listen endpoint's ``Service`` to this server.

        Args:
            service_name: Key in ``self.services``, usually the YAML
                ``server.service[].name``.
            service: Service adapter that will ``run()`` during ``serve``.
        """
        self.services[service_name] = service

    def get_service(self, service_name: str) -> Optional[Service]:
        """Return the service registered under ``service_name``, if any.

        Args:
            service_name: Key previously passed to ``add_service``.

        Returns:
            The ``Service``, or ``None`` if the name is unknown.
        """
        return self.services.get(service_name, None)

    def register(self, service_desc: ServiceDesc, fix_rpc_name: bool = False) -> None:
        """Register the same ``ServiceDesc`` on every attached service.

        Generated stubs call this once. A process with several services
        should construct each with ``Service("servicename")`` first;
        otherwise every service on this server receives the implementation.

        Args:
            service_desc: Methods and streams to expose.
            fix_rpc_name: If True, rewrite each method's service-name
                segment to match that ``Service``'s configured name.
        """
        for _, service in self.services.items():
            service.register(service_desc, fix_rpc_name)

    def _handle_sys_signals(self) -> None:
        """Install OS signal handlers on the master's main thread.

        Must run **after fork**. ``SIGTERM`` / ``SIGINT`` / ``SIGUSR2`` /
        ``SIGSEGV`` tear the tree down; ``SIGCHLD`` restarts a child unless
        ``self.close`` is already True. uvloop cannot use
        ``loop.add_signal_handler``.
        """
        # uvloop does not support loop.add_signal_handler.
        # Register after forking, otherwise asyncio.subprocess will crash.
        if threading.current_thread() is not threading.main_thread():
            return

        signal.signal(signal.SIGTERM, self.handle_master_exit)
        signal.signal(signal.SIGINT, self.handle_master_exit)
        signal.signal(signal.SIGUSR2, self.handle_master_exit)
        signal.signal(signal.SIGSEGV, self.handle_master_exit)
        signal.signal(signal.SIGCHLD, self.handle_child_exit)

    def serve(self, io_wait_count: Optional[int] = None) -> None:
        """Build pipes, fork children, then run the master event loop.

        Forces the ``fork`` start method, creates the master's loop, then
        ``init_processes`` → ``Service.run`` → healthy checker →
        ``start_processes`` → signal handlers → ``run_forever``.

        Args:
            io_wait_count: If set, register a ``wait_notify_in_io`` child
                callback so IO workers wait for this many shared-memory
                notifications after fork.
        """
        try:
            multiprocessing.set_start_method("fork", force=True)
        except RuntimeError as err:
            logger.error("multiprocessing set_start_method occurs err: %s; current start_method: %s", str(err),
                         multiprocessing.get_start_method(True))

        self.opt.loop = get_or_create_event_loop()
        asyncio.set_event_loop(self.opt.loop)

        self.server_engine.init_processes()

        for s in self.services.values():
            s.run()

        if self.server_engine.is_dispatcher():
            process_type = fork_hook.ProcessType.DISPATCHER
            dispatcher_num = 1
        else:
            process_type = fork_hook.ProcessType.WORKER
            dispatcher_num = 0

        if io_wait_count:
            fork_hook.register_child_callback(partial(wait_notify_in_io, io_wait_count), process_type)

        # Heartbeat plugin is created in the master so children inherit it.
        init_healthy_checker(self.server_engine.worker_process_list, dispatcher_num,
                             self.server_engine.task_process_list)

        self.server_engine.start_processes()

        # After fork: SIGTERM tears down, SIGCHLD restarts children.
        self._handle_sys_signals()

        try:
            self.opt.loop.run_forever()
        except (SystemExit, KeyboardInterrupt):
            self.opt.loop.stop()

    async def wait_for_close(self, tasks: Iterable[Any]) -> None:
        """Await remaining service-close coroutines, then stop the master loop.

        Args:
            tasks: Awaitables returned by ``Service.close()``; unpacked
                into ``asyncio.gather``.
        """
        await asyncio.gather(*tasks)
        self.opt.loop.stop()

    def handle_master_exit(self, signum: int, frame: Optional[Any]) -> None:
        """Tear down services and children, then ``os._exit`` the master.

        Sets ``self.close`` first so a racing ``SIGCHLD`` cannot restart
        a child that is about to be killed. Each ``Service.close()``
        deregisters; ``close_wait_time`` sleeps **before**
        ``engine.close()`` SIGTERMs the children. User SIGTERM callbacks
        run last, then ``kill()``.

        Args:
            signum: OS signal that triggered shutdown.
            frame: Interrupted stack frame; unused, may be ``None``.
        """
        logger.warning("Prepare to exit master, signum: %s", signum)

        self.close = True
        close_queue = []

        for service in self.services.values():
            # Deregister and stop accepting; a non-None return is an awaitable.
            ret = service.close()
            if ret is not None:
                close_queue.append(ret)

        # Drain in-flight requests before SIGTERM'ing children.
        if self.opt.close_wait_time > 0:
            logger.warning("waiting %d ms for remaining requests to be processed", self.opt.close_wait_time)
            time.sleep(self.opt.close_wait_time / 1000.0)

        try:
            self.server_engine.close()
        except Exception as ex:  # pylint: disable=broad-except
            logger.error("receive exception when close server engine, err: %s", str(ex))

        if len(close_queue) > 0:
            asyncio.ensure_future(self.wait_for_close(close_queue))
        else:
            self.opt.loop.stop()

        run_server_signal_handle()

        logger.warning('kill master process-%s', os.getpid())
        self.kill()

    def handle_child_exit(self, signum: int, frame: Optional[Any]) -> None:
        """Restart a crashed child, or shut the whole tree down.

        Ignored when ``self.close`` is already True (master is exiting).
        ``reboot_child`` false, or ``ChildProcessError`` from ``waitpid``,
        both call ``handle_master_exit``.

        Args:
            signum: ``SIGCHLD`` (or the signal forwarded into master exit).
            frame: Interrupted stack frame; unused, may be ``None``.
        """
        try:
            if self.close:
                return

            cpid = self.get_exit_child_pid()
            if cpid == 0:
                logger.error('no child process was immediately available')
                return
            logger.error('child process-%s exceptionally exit', cpid)

            # Set reboot_child to false to stop the whole server when a
            # child coredumps instead of replacing that one process.
            if self.opt.reboot_child:
                if self.server_engine.find_child_process(cpid):
                    self.server_engine.restart_child_process(cpid)
            else:
                self.handle_master_exit(signum, frame)
        except ChildProcessError:
            self.handle_master_exit(signum, frame)

    @staticmethod
    def get_exit_child_pid() -> int:
        """Reap one exited child without blocking.

        Returns:
            The child's OS pid, or ``0`` if none was ready (``WNOHANG``).
        """
        cpid, _ = os.waitpid(-1, os.WNOHANG)
        return cpid

    @staticmethod
    def kill() -> None:
        """Exit the master immediately, skipping Python ``atexit`` handlers."""
        os._exit(0)
