# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Service table and the startup order that turns this process into a worker.

:class:`Server` is the object application code holds after :func:`trpc.new`.
It is not a supervisor of servers: it collects the ``service_name -> Service``
map, hands the listen endpoints to the engine, and then hands the process
itself over to a worker chosen by ``server.mode``. Everything after that —
listen sockets, handlers, signals, children — belongs to that worker.

``serve()`` has one ordering constraint worth stating outright: the children
are forked before this process creates an event loop. An event loop owns an
epoll/kqueue descriptor whose readiness state is meaningless once copied, so
a child that inherited one would share the parent's. That is why the sequence
is register endpoints, build the worker, ``_spawn_children``, then ``run``.

The only work :class:`Server` keeps for itself is
:meth:`Server._shutdown_services`, registered as a worker exit callback so
services leave the naming service before the process stops answering.
"""

from __future__ import annotations

import multiprocessing
import time
from functools import partial
from typing import Any
from typing import Dict
from typing import Optional

from trpc_sdk._inner import wait_notify_in_io
from trpc_sdk.abc import BaseServer
from trpc_sdk.abc import ServiceDesc
from trpc_sdk.common import ProcessType
from trpc_sdk.common import execute_after_in_parent
from trpc_sdk.common import execute_before
from trpc_sdk.common import register_child_callback
from trpc_sdk.log import logger

from . import _engine as _engine_module
from ._engine import TrpcServerEngine
from ._modes import create_worker
from ._option import ServerOption
from ._service import Service
from ._signals import run_server_signal_handle


class Server(BaseServer):
    """The service table of a tRPC server, and its startup sequence.

    Holds the services, :class:`ServerOption`, and the
    :class:`TrpcServerEngine` the listen endpoints are registered on. Does not
    own listen sockets and does not run handlers: :meth:`serve` builds the
    worker for the configured mode and that worker takes the process over.
    """

    def __init__(self, server_option: ServerOption) -> None:
        """Record options and create the engine; nothing is forked or bound yet.

        Args:
            server_option: Process-model options, including ``mode``,
                ``reboot_child`` and ``close_wait_time``.
        """
        # ``service_name -> Service``, filled by ``trpc.new`` / ``add_service``.
        self.services: Dict[str, Service] = {}
        self.opt: ServerOption = server_option
        # True once shutdown started; read by business code that wants to know.
        self.close: bool = False
        # Listen endpoints registered by ``Service.run``, and the options the
        # worker reads. It no longer forks anything.
        self.server_engine: TrpcServerEngine = TrpcServerEngine(self.opt)
        # What this process becomes in ``serve()``; ``None`` until then.
        self.worker: Optional[Any] = None

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

    def serve(self, io_wait_count: Optional[int] = None) -> None:
        """Register listen endpoints, fork the children, then become the worker.

        This process does not supervise a tree of servers; it *is* the server.
        The steps are:

        1. Force the ``fork`` start method, which every child relies on to
           inherit already-created pipes.
        2. ``Service.run`` registers each listen endpoint on the engine. No
           socket is bound yet, only recorded.
        3. Look up ``server.mode`` and build the worker for it.
        4. ``_spawn_children`` creates every IPC pipe and forks the children.
        5. ``run`` builds this process's event loop, binds the addresses and
           serves until SIGTERM.

        Step 4 must precede step 5. An event loop holds an epoll/kqueue
        descriptor whose readiness state does not survive fork, so children
        must be created while this process still has no loop.

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

        for s in self.services.values():
            s.run()

        if io_wait_count:
            register_child_callback(partial(wait_notify_in_io, io_wait_count),
                                              ProcessType.WORKER)

        self.worker = create_worker(self.opt.mode, self.server_engine)
        self.worker.exit_callbacks.append(self._shutdown_services)
        # Children reach ``worker_process_list`` through this reference, which
        # they can only inherit if it is set before the first fork.
        _engine_module.GLOBAL_SERVER_ENGINE = self.server_engine

        # Plugins quiesce anything that must not be duplicated into a child,
        # and repair it once every child exists.
        execute_before()
        self.worker._spawn_children()  # pylint: disable=protected-access
        execute_after_in_parent()

        self.worker.run()

    def _shutdown_services(self) -> None:
        """Leave the naming service and drain in-flight work, then let go.

        Runs on the worker's SIGTERM path, before it stops its loop and its
        children. Deregistering first is what makes ``close_wait_time``
        useful: callers stop being pointed here, and the wait then covers the
        requests already on their way.
        """
        self.close = True
        for service in self.services.values():
            service.close()

        if self.opt.close_wait_time > 0:
            logger.warning("waiting %d ms for remaining requests to be processed", self.opt.close_wait_time)
            time.sleep(self.opt.close_wait_time / 1000.0)

        run_server_signal_handle()
