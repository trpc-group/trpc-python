# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Service table and the startup order that turns this process into a worker.

:class:`Server` is the object application code holds after :func:`create_server`.
It is not a supervisor of servers: it collects the ``service_name -> ServiceAdapter``
map, hands the listen endpoints to the engine, and then hands the process
itself over to a worker chosen by ``server.mode``. Everything after that —
listen sockets, handlers, signals, children — belongs to that worker.

``serve()`` has one ordering constraint worth stating outright: the children
are forked before this process creates an event loop. An event loop owns an
epoll/kqueue descriptor whose readiness state is meaningless once copied, so
a child that inherited one would share the parent's. That is why the sequence
is register endpoints, build the worker, ``_spawn_children``, then ``run``.

The only lifecycle work :class:`Server` keeps for itself is handed to the
worker as callbacks: naming registration runs once the worker listens
(``ready_callbacks``), :meth:`Server._shutdown_services` deregisters first
on SIGTERM (``exit_callbacks``), and after the drain period
(``post_drain_callbacks``) the server waits for that deregistration
(:meth:`TrpcServerEngine.close`, at most 10 seconds per service). The user
server exit callback (``master_handle``) is a ``master_exit_callbacks`` entry:
merge runs it right before the user worker exit callbacks, separate once every
rpc task has run its own and exited.
"""

from __future__ import annotations

import inspect
import multiprocessing
from functools import partial
from typing import Any
from typing import Collection
from typing import Dict
from typing import Optional

from trpc_sdk.abc import BaseServer
from trpc_sdk.abc import ServiceDesc
from trpc_sdk.common import execute_after_in_parent
from trpc_sdk.common import execute_before
from trpc_sdk.config import SocketType
from trpc_sdk.config import TrpcConfig
from trpc_sdk.filters import get_server_filter
from trpc_sdk.log import logger
from trpc_sdk.plugin import PluginInitError
from trpc_sdk.runtime import create_worker
from trpc_sdk.runtime import freeze_handler_thread_hooks
from trpc_sdk.runtime import run_server_signal_handle
from trpc_sdk.transport import DefaultServerTransport
from trpc_sdk.transport import DefaultServerTransportOption
from trpc_sdk.transport import get_server_transport
from trpc_sdk.utils import find_free_port
from trpc_sdk.utils import get_async_lib
from . import _engine as _engine_module
from ._engine import TrpcServerEngine
from ._option import ServerOption
from ._option import ServiceOption
from ._option_resolver import resolve_server_option
from ._cython import ServiceAdapter
from ._cython import create_service
from ._registry import start_service_registry
from ._utils import save_bind_info


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
        # ``service_name -> ServiceAdapter``, filled by ``create_server`` / ``add_service``.
        self.services: Dict[str, ServiceAdapter] = {}
        self.opt: ServerOption = server_option
        # True once shutdown started; read by business code that wants to know.
        self.close: bool = False
        # Listen endpoints registered by ``ServiceAdapter.run``, and the options the
        # worker reads. It no longer forks anything.
        self.server_engine: TrpcServerEngine = TrpcServerEngine(self.opt)
        # What this process becomes in ``serve()``; ``None`` until then.
        self.worker: Optional[Any] = None

    def add_service(self, service_name: str, service: ServiceAdapter) -> None:
        """Attach one listen endpoint's ``ServiceAdapter`` to this server.

        Args:
            service_name: Key in ``self.services``, usually the YAML
                ``server.service[].name``.
            service: Service adapter that will ``run()`` during ``serve``.
        """
        self.services[service_name] = service

    def get_service(self, service_name: str) -> Optional[ServiceAdapter]:
        """Return the service registered under ``service_name``, if any.

        Args:
            service_name: Key previously passed to ``add_service``.

        Returns:
            The ``ServiceAdapter``, or ``None`` if the name is unknown.
        """
        return self.services.get(service_name, None)

    def register(self, service_desc: ServiceDesc, fix_rpc_name: bool = False) -> None:
        """Register the same ``ServiceDesc`` on every attached service.

        Generated stubs call this once. A process with several services
        should construct each with ``ServiceAdapter("servicename")`` first;
        otherwise every service on this server receives the implementation.

        Args:
            service_desc: Methods and streams to expose.
            fix_rpc_name: If True, rewrite each method's service-name
                segment to match that ``ServiceAdapter``'s configured name.

        A handler that is not ``async def`` is only warned about and still
        registered: handlers run as tasks on a serving loop and the
        framework keeps no thread pool for blocking ones, so it fails at
        call time if it does not return an awaitable.
        """
        for desc in list(service_desc.methods or ()) + list(service_desc.streams or ()):
            if not _is_coroutine_handler(desc.impl_func):
                logger.warning("handler %s is not 'async def'; it runs as a task on a serving loop "
                               "and the framework keeps no thread pool for blocking handlers", desc.name)
        for _, service in self.services.items():
            service.register(service_desc, fix_rpc_name)

    def serve(self, io_wait_count: Optional[int] = None) -> None:
        """Register listen endpoints, fork the children, then become the worker.

        This process does not supervise a tree of servers; it *is* the server.
        The steps are:

        1. Force the ``fork`` start method for the whole process.
        2. ``ServiceAdapter.run`` registers each listen endpoint on the engine. No
           socket is bound yet, only recorded.
        3. Look up ``server.mode`` and build the worker for it, and hand it
           the lifecycle work that belongs to the server: naming
           registration once it listens, deregistration and the drain on
           SIGTERM.
        4. ``_spawn_children`` creates every IPC pipe and forks the children.
        5. ``run`` builds this process's event loop, binds the addresses,
           registers with the naming service and serves until SIGTERM.

        Step 4 must precede step 5. An event loop holds an epoll/kqueue
        descriptor whose readiness state does not survive fork, so children
        must be created while this process still has no loop. For the same
        reason no registry thread is started before the fork any more.

        Children inherit the pipes created before step 4 and the state
        already in this process, which is why step 1 forces ``fork``: they
        are started with the global start method.

        Args:
            io_wait_count: If set, the worker waits for this many
                ``inc_io_wait_count`` notifications from its tasks before it
                binds any address.
        """
        try:
            multiprocessing.set_start_method("fork", force=True)
        except RuntimeError as err:
            logger.error("multiprocessing set_start_method occurs err: %s; current start_method: %s", str(err),
                         multiprocessing.get_start_method(True))

        for s in self.services.values():
            s.run()

        freeze_handler_thread_hooks()
        self.worker = create_worker(self.opt.mode, self.server_engine)
        if io_wait_count:
            self.worker.io_wait_count = io_wait_count
        self.worker.ready_callbacks.append(partial(start_service_registry, self))
        self.worker.exit_callbacks.append(self._shutdown_services)
        self.worker.post_drain_callbacks.append(self.server_engine.close)
        self.worker.master_exit_callbacks.append(run_server_signal_handle)
        self.worker.drain_seconds = max(0, self.opt.close_wait_time) / 1000.0
        # Children reach ``worker_process_list`` through this reference, which
        # they can only inherit if it is set before the first fork.
        _engine_module.GLOBAL_SERVER_ENGINE = self.server_engine

        if TrpcConfig.server_config().admin:
            # Registers AdminPlugin and its after-fork-in-parent hook.
            import trpc_sdk.admin  # noqa: F401  pylint: disable=import-outside-toplevel,unused-import

        # Plugins quiesce anything that must not be duplicated into a child,
        # and repair it once every child exists.
        execute_before()
        self.worker._spawn_children()  # pylint: disable=protected-access
        execute_after_in_parent()

        self.worker.run()

    def _shutdown_services(self) -> None:
        """Leave the naming service; the worker drains afterwards.

        Runs first on the worker's SIGTERM path. Deregistering first is what
        makes ``close_wait_time`` useful: callers stop being pointed here,
        and the worker's drain period (``drain_seconds``, without blocking
        its loop) then covers the requests already on their way. After that
        drain, post-drain callbacks wait for the deregistration to finish and
        then run the user server exit callbacks, followed by the worker's user
        worker exit callbacks.
        """
        self.close = True
        for service in self.services.values():
            service.close()


def _is_coroutine_handler(func: Any) -> bool:
    """Tell whether calling ``func`` returns a coroutine.

    Sees through ``functools.partial`` and ``functools.wraps`` decorators.
    Bound methods are handled by :func:`inspect.iscoroutinefunction` itself,
    and an object whose ``__call__`` is ``async def`` counts too.

    Args:
        func: A ``Method.impl_func`` or ``StreamDesc.impl_func``.

    Returns:
        True if the handler is a coroutine function.
    """
    while isinstance(func, partial):
        func = func.func
    func = inspect.unwrap(func)
    if inspect.iscoroutinefunction(func):
        return True
    if not inspect.isroutine(func) and callable(func):
        return inspect.iscoroutinefunction(type(func).__call__)
    return False


def _add_server_filters(service_option: ServiceOption, filter_names: Optional[Collection[str]]) -> None:
    """Install named server filters onto ``service_option``.

    Args:
        service_option: Endpoint that receives the filter instances.
        filter_names: Plugin names from YAML ``filter`` lists; ``None`` or
            empty is a no-op.

    Raises:
        PluginInitError: If a name has no registered server filter plugin.
    """
    if not filter_names:
        return
    for filter_name in filter_names:
        filter_ins = get_server_filter(filter_name)
        if filter_ins is None:
            raise PluginInitError(f"cannot find server filter plugin: {filter_name}")
        service_option.add_filter(filter_ins)


def create_server(conf_path: str,
                  server_option: Optional[ServerOption] = None,
                  ignore_services: Collection[str] = ()) -> Server:
    """Load YAML, build one :class:`ServiceAdapter` per ``server.service[]``, return a :class:`Server`.

    This is the application entry that used to live as ``trpc.new``. It does
    not bind sockets, does not fork and does not register with the naming
    service: it fills :class:`ServerOption` from the config and constructs
    each listen endpoint. Registration happens in :meth:`Server.serve`, once
    the worker listens.

    An empty YAML ``server.mode`` is left untouched so :class:`ServerOption`'s
    own default (``merge``) stands; a missing field must not become a
    lookup failure in :func:`create_worker`. The thread- and process-model
    fields are copied raw and then resolved by
    :func:`~trpc_sdk.server._option_resolver.resolve_server_option`.

    Args:
        conf_path: Path to the framework YAML file.
        server_option: Process-model options; constructed empty and then
            overwritten from YAML when omitted.
        ignore_services: Service names skipped at startup; empty means
            start every ``server.service[]`` entry.

    Returns:
        A :class:`Server` whose ``services`` map is filled; call
        :meth:`Server.serve` to bind and run.
    """
    TrpcConfig.load(conf_path, "utf-8")

    server_cfg = TrpcConfig.server_config()
    global_cfg = TrpcConfig.global_config()
    get_async_lib(global_cfg.enable_uvloop)

    if server_option is None:
        server_option = ServerOption()
    # An absent ``server.mode`` parses as the empty string, which is not a mode
    # name. Leaving the option untouched lets its own default stand, so a config
    # that never mentions the field gets merge mode rather than a lookup failure.
    if server_cfg.mode:
        server_option.mode = server_cfg.mode
    server_option.worker_num = server_cfg.worker_num
    server_option.io_handle_type = server_cfg.io_handle_type
    server_option.io_thread_num = server_cfg.io_thread_num
    server_option.handle_thread_num = server_cfg.handle_thread_num
    server_option.handler_max_concurrency = server_cfg.handler_max_concurrency
    server_option.handler_queue_size = server_cfg.handler_queue_size
    server_option.tcp_reuse_port = server_cfg.tcp_reuse_port
    server_option.ready_timeout = server_cfg.ready_timeout
    server_option.service_num = len(server_cfg.service)
    server_option.task_wait_interval = server_cfg.task_wait_interval
    server_option.task_batch_size = server_cfg.task_batch_size
    server_option.task_queue_size = server_cfg.task_queue_size
    server_option.task_consumer_num = server_cfg.task_consumer_num
    server_option.reboot_child = server_cfg.reboot_child
    server_option.close_wait_time = server_cfg.close_wait_time
    resolve_server_option(server_option, server_cfg.service)
    server = Server(server_option)

    for service_conf in server_cfg.service:
        if service_conf.name in ignore_services:
            continue
        service_option = ServiceOption()
        service_option.app = server
        service_option.set_queue_wait_timeout(server_cfg.queue_wait_timeout)
        service_option.set_queue_wait_policy(server_cfg.queue_wait_policy)

        if len(service_conf.registry) > 0:
            service_option.set_registry(service_conf.registry)

        trans = get_server_transport(service_conf.protocol)
        if trans is None:
            trans = DefaultServerTransport()
        service_option.transport = trans

        trans_option = DefaultServerTransportOption()
        trans_option.set_worker_num(server_option.worker_num)
        trans_option.set_mode(server_option.mode)
        trans_option.set_tls_key(service_conf.tls_key)
        trans_option.set_tls_cert(service_conf.tls_cert)
        trans_option.set_target(service_conf.target)
        service_option.transport_option = trans_option

        service_option.set_namespace(global_cfg.namespace)
        service_option.set_env_name(global_cfg.env_name)
        service_option.set_service_name(service_conf.name)
        service_option.set_protocol(service_conf.protocol)
        service_option.set_network(service_conf.network)
        service_option.set_timeout(int(service_conf.timeout))
        service_option.set_disable_request_timeout(service_conf.disable_request_timeout)
        service_option.set_enable_stream_filter(service_conf.enable_stream_filter)
        service_option.set_server_tls(service_conf.tls_cert, service_conf.tls_key)
        service_option.set_registry_delay(server_cfg.registry_delay)
        service_option.set_registry_option(server_cfg.registry_option)
        service_option.set_max_window_size(int(service_conf.max_window_size))
        if global_cfg.enable_set == "Y":
            service_option.set_setname(global_cfg.full_set_name)
        service_option.set_socket_type(service_conf.socket_type)
        if service_conf.socket_type == SocketType.UNIX:
            service_option.set_address((service_conf.unix_path, 0))
        else:
            # Port 0 means pick an ephemeral port and record it so the rest of
            # startup (bind-info dump, naming register) sees the real value.
            if service_conf.port == 0:
                service_conf.port = find_free_port()
            service_option.set_address((service_conf.ip, service_conf.port))
            save_bind_info(global_cfg, service_conf)

        _add_server_filters(service_option, server_cfg.filters)
        _add_server_filters(service_option, service_conf.filters)

        service = create_service(service_conf.name, service_option)
        server.add_service(service_conf.name, service)

    return server
