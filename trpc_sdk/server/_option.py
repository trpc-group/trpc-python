# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Process-model and per-service options consumed by Server / TrpcServerEngine.

``create_server(yaml)`` copies YAML ``server:`` fields onto :class:`ServerOption`
and each ``server.service[]`` entry onto a :class:`ServiceOption`. The engine
reads ``ServerOption.mode`` (``merge`` or ``separate``) and how many
threads or rpc tasks to start. Each ``Service`` keeps its ``ServiceOption``
for listen address, codec, registry, and filters.
"""

from __future__ import annotations

from typing import TYPE_CHECKING
from typing import Any
from typing import Callable
from typing import Optional

from trpc_sdk.codec import get_server_checker
from trpc_sdk.codec import get_server_codec
from trpc_sdk.filters import FilterChain
from trpc_sdk.naming import defaultRegistry
from trpc_sdk.naming import get_registry
from trpc_sdk.transport import DefaultServerTransport
from trpc_sdk.transport import DefaultServerTransportOption
from ._constants import DEFAULT_ADDRESS
from ._constants import DEFAULT_CLOSE_WAIT_TIME
from ._constants import DEFAULT_COMPRESS_TYPE
from ._constants import DEFAULT_DISABLE_REQUEST_TIMEOUT
from ._constants import DEFAULT_ENABLE_STREAM_FILTER
from ._constants import DEFAULT_ENV_NAME
from ._constants import DEFAULT_HANDLER_SET
from ._constants import DEFAULT_MAX_WINDOW_SIZE
from ._constants import DEFAULT_MODE
from ._constants import DEFAULT_NAMESPACE
from ._constants import DEFAULT_PROTOCOL
from ._constants import DEFAULT_QUEUE_WAIT_POLICY
from ._constants import DEFAULT_QUEUE_WAIT_TIMEOUT
from ._constants import DEFAULT_REBOOT_CHILD
from ._constants import DEFAULT_REGISTRY_DELAY
from ._constants import DEFAULT_REGISTRY_OPTION
from ._constants import DEFAULT_RPC_TASK_NUM
from ._constants import DEFAULT_SERIALIZATION_TYPE
from ._constants import DEFAULT_SERVICE_NAME
from ._constants import DEFAULT_SERVICE_NUM
from ._constants import DEFAULT_SET_NAME
from ._constants import DEFAULT_SOCKET_TYPE
from ._constants import DEFAULT_TASK_BATCH_SIZE
from ._constants import DEFAULT_TASK_CONSUMER_NUM
from ._constants import DEFAULT_TASK_QUEUE_SIZE
from ._constants import DEFAULT_TASK_WAIT_INTERVAL
from ._constants import DEFAULT_TIMEOUT
from ._constants import DEFAULT_TIMER_SPEC
from ._constants import DEFAULT_WORKER_NUM
from ._constants import DEFAULT_WORKER_THREAD_NUM

if TYPE_CHECKING:
    from asyncio import AbstractEventLoop

    from trpc_sdk.abc import AddrType
    from trpc_sdk.abc import BaseServerTransport
    from trpc_sdk.abc import BaseServerTransportOption
    from trpc_sdk.abc import BaseServiceRegistry
    from trpc_sdk.codec import ServerCodec
    from trpc_sdk.stream import StreamDispatcher
    from ._server import Server


class ServerOption:
    """Process-wide server options (YAML ``server:``, one per Server).

    Not a listen endpoint. Worker count, process mode, task-queue knobs, and
    child-restart policy live here. Per-port settings are :class:`ServiceOption`.

    Example: ``server.mode: merge`` and ``server.worker_thread_num: 4`` become
    ``mode="merge"`` and ``worker_thread_num=4``; the worker then spreads
    connections over four event-loop threads in this process.
    """

    def __init__(self) -> None:
        # Process model: "merge" / "separate". Retired YAML names are
        # translated in ``trpc_sdk.server._compat`` before the worker is built.
        self.mode: str = DEFAULT_MODE
        # Worker (or executor) process count. YAML default is CPU core count.
        self.worker_num: int = DEFAULT_WORKER_NUM
        # Event-loop threads inside a merge-mode worker; 1 stays single-threaded.
        self.worker_thread_num: int = DEFAULT_WORKER_THREAD_NUM
        # Rpc task processes forked by a separate-mode worker.
        self.rpc_task_num: int = DEFAULT_RPC_TASK_NUM
        # Number of ServiceOption entries; filled as len(server.service).
        self.service_num: int = DEFAULT_SERVICE_NUM
        # Task-process batch wait, in milliseconds.
        self.task_wait_interval: int = DEFAULT_TASK_WAIT_INTERVAL
        # Max tasks pulled in one task-process batch.
        self.task_batch_size: int = DEFAULT_TASK_BATCH_SIZE
        # Task-process inbound queue capacity.
        self.task_queue_size: int = DEFAULT_TASK_QUEUE_SIZE
        # Consumer threads inside each task process.
        self.task_consumer_num: int = DEFAULT_TASK_CONSUMER_NUM
        # Optional callback for master -> worker message-pipe payloads.
        self.on_worker_message: Optional[Callable[[int, bytes], Any]] = None
        # If True, master restarts a crashed worker/task child.
        self.reboot_child: bool = DEFAULT_REBOOT_CHILD
        # Event loop assigned by the engine. Cython left this as None via
        # ``cdef public object``; Python must set it explicitly.
        self.loop: Optional[AbstractEventLoop] = None
        # Graceful-shutdown wait on SIGTERM, in milliseconds.
        self.close_wait_time: int = DEFAULT_CLOSE_WAIT_TIME


class ServiceOption:
    """One listen endpoint / named service (YAML ``server.service[]``).

    Holds naming (namespace, env, set, service_name), bind address, protocol
    codec, transport, registry, filters, and stream settings. Setters that
    affect I/O also update ``transport_option`` so the listen path stays in
    sync.

    Example: YAML ``name: trpc.test.helloworld.Greeter``, ``ip/port``,
    ``protocol: trpc`` becomes this object; ``set_protocol`` installs the
    server codec and the frame checker used by the dispatcher.
    """

    def __init__(self) -> None:
        # Owning Server instance; set by create_server().
        self.app: Optional[Server] = None

        # Naming-service namespace: Production vs Development.
        self.namespace: str = DEFAULT_NAMESPACE
        # Naming-service environment name.
        self.env_name: str = DEFAULT_ENV_NAME
        # Polaris set name; empty unless global.enable_set is Y.
        self.set_name: str = DEFAULT_SET_NAME
        # RPC service name, e.g. trpc.app.server.Service.
        self.service_name: str = DEFAULT_SERVICE_NAME

        # Cron spec for a timer service; empty means not a timer.
        # Format: "second minute hour day month weekday", e.g. "0 */5 * * * ?".
        self.timerSpec: str = DEFAULT_TIMER_SPEC

        # Bind address as (host, port), or (unix_path, 0) for UNIX sockets.
        self.address: AddrType = DEFAULT_ADDRESS
        # Socket family/type from YAML (tcp / unix / ...).
        self.socket_type: str = DEFAULT_SOCKET_TYPE
        # Request timeout in milliseconds; also copied to transport idle timeout.
        self.timeout: int = DEFAULT_TIMEOUT
        # If True, do not inherit the upstream caller's remaining timeout.
        self.disable_request_timeout: bool = DEFAULT_DISABLE_REQUEST_TIMEOUT
        # Queue-wait timeout in milliseconds before a request is dropped.
        self.queue_wait_timeout: int = DEFAULT_QUEUE_WAIT_TIMEOUT
        # Queue-wait circuit-breaker policy (0 = off; see YAML queue_wait_policy).
        self.queue_wait_policy: int = DEFAULT_QUEUE_WAIT_POLICY

        # Override serialization; -1 means use the protocol default.
        self.current_serialization_type: int = DEFAULT_SERIALIZATION_TYPE
        # Override compression; -1 means use the protocol default.
        self.current_compress_type: int = DEFAULT_COMPRESS_TYPE

        # Wire protocol name: "trpc", "http", ...
        self.protocol: str = DEFAULT_PROTOCOL
        # Whether a custom handler is set.
        self.handler_set: bool = DEFAULT_HANDLER_SET

        # Listen/transport knobs (address, network, checker, TLS, mode).
        # DefaultServerTransportOption is the same class as
        # BaseServerTransportOption; DefaultServerTransport() is a placeholder
        # that create_server() replaces with whatever sits in the plugin slot.
        self.transport_option: BaseServerTransportOption = DefaultServerTransportOption()
        # Server transport implementation (default or protocol plugin).
        self.transport: BaseServerTransport = DefaultServerTransport()

        # Service registry plugin; defaultRegistry is a no-op.
        self.registry: Optional[BaseServiceRegistry] = defaultRegistry
        # Delay before registering, in milliseconds.
        self.registry_delay: int = DEFAULT_REGISTRY_DELAY
        # Registry control: 0 = off, 1 = on.
        self.registry_option: int = DEFAULT_REGISTRY_OPTION
        # Server codec for this protocol; set by set_protocol().
        self.codec: Optional[ServerCodec] = None

        # Server filter chain (True = server-side chain).
        self.filters: FilterChain = FilterChain(True)

        # Streaming RPC handler; None if this service is unary-only.
        self.stream_handle: Optional[StreamDispatcher] = None
        # Streaming transport; None uses the default stream transport.
        self.stream_transport: Any = None
        # Stream flow-control window size; 0 means protocol default.
        self.max_window_size: int = DEFAULT_MAX_WINDOW_SIZE
        # If True, run stream filters on this service.
        self.enable_stream_filter: bool = DEFAULT_ENABLE_STREAM_FILTER

    def set_namespace(self, namespace: str) -> None:
        """Set the namespace.

        Naming-service namespace: Production vs Development.

        Args:
            namespace: Namespace string from YAML ``global.namespace``.
        """
        self.namespace = namespace

    def set_env_name(self, env_name: str) -> None:
        """Set the current environment.

        Naming-service environment name from YAML ``global.env_name``.

        Args:
            env_name: Environment string from YAML ``global.env_name``.
        """
        self.env_name = env_name

    def set_setname(self, set_name: str) -> None:
        """Set the set grouping.

        Polaris set name; typically ``global.full_set_name`` when enable_set is Y.

        Args:
            set_name: Set name; typically ``global.full_set_name`` when enable_set is Y.
        """
        self.set_name = set_name

    def set_service_name(self, service_name: str) -> None:
        """Set the current service's service name.

        Also copied onto the transport option.

        Args:
            service_name: Full service name, e.g. ``trpc.test.helloworld.Greeter``.
        """
        self.service_name = service_name
        self.transport_option.set_service_name(service_name)

    def add_filter(self, f: Any) -> None:
        """Add a server interceptor that runs before and after the business handler.

        Args:
            f: Filter instance from ``get_server_filter``.
        """
        self.filters.add_filter(f)

    def set_address(self, address: AddrType) -> None:
        """Set the server listen address: (ip, port) or ('', port).

        Also copied onto the transport option.

        Args:
            address: ``(host, port)`` or ``(unix_path, 0)``. Port must not be ``''``.

        Raises:
            Exception: If ``address`` is not a 2-tuple or port is ``''``.
        """
        if len(address) != 2:
            raise Exception("address is invalid")
        if address[1] == '':
            raise Exception("address port is invalid")

        self.transport_option.set_address(address)
        self.address = address

    def set_socket_type(self, socket_type: str) -> None:
        """Set the socket type.

        Also copied onto the transport option.

        Args:
            socket_type: YAML ``socket_type`` (e.g. TCP vs UNIX).
        """
        self.transport_option.set_socket_type(socket_type)
        self.socket_type = socket_type

    def set_server_tls(self, cert_file: str, key_file: str) -> None:
        """Set the server TLS file paths.

        Reserved; currently a no-op. Cert/key are applied on
        ``transport_option`` in ``create_server()``.

        Args:
            cert_file: Path to the server certificate.
            key_file: Path to the private key.
        """
        # TODO: self.transport_option.set_server_tls(cert_file, key_file)
        pass

    def set_network(self, network: str) -> None:
        """Set the server listen network (tcp or udp); default is tcp.

        Applied on the transport option. HTTP is also a valid YAML value.

        Args:
            network: YAML ``network``; matched against Network.TCP/UDP/HTTP.
        """
        self.transport_option.set_network(network)

    def set_timeout(self, timeout: int) -> None:
        """Set the overall timeout for a single request; default is 1s.

        Also copied to the transport idle timeout. The field starts at 0 until
        YAML load fills it.

        Args:
            timeout: Timeout in milliseconds from YAML ``service.timeout``.
        """
        self.transport_option.set_idle_timeout(timeout)
        self.timeout = timeout

    def set_disable_request_timeout(self, disable_request_timeout: bool) -> None:
        """Choose whether to ignore the upstream remaining timeout.

        Args:
            disable_request_timeout: True to use only this service's timeout.
        """
        self.disable_request_timeout = disable_request_timeout

    def set_enable_stream_filter(self, enable_stream_filter: bool) -> None:
        """Enable or disable stream filters for this service.

        Args:
            enable_stream_filter: YAML ``enable_stream_filter``.
        """
        self.enable_stream_filter = enable_stream_filter

    def set_queue_wait_timeout(self, queue_wait_timeout: int) -> None:
        """Set how long a request may wait in the server queue.

        Args:
            queue_wait_timeout: Timeout in milliseconds; 0 disables the check.
        """
        self.queue_wait_timeout = queue_wait_timeout

    def set_queue_wait_policy(self, queue_wait_policy: int) -> None:
        """Set the queue-wait circuit-breaker policy.

        Args:
            queue_wait_policy: YAML ``queue_wait_policy``; 0 means off.
        """
        self.queue_wait_policy = queue_wait_policy

    def set_transport(self, transport: BaseServerTransport) -> None:
        """Replace the underlying server transport.

        Args:
            transport: ``BaseServerTransport`` instance (plugin or default).
        """
        self.transport = transport

    def set_registry(self, registry_name: str) -> None:
        """Set the server service registry; one service supports only one registry.

        Args:
            registry_name: Plugin name passed to ``get_registry``.
        """
        self.registry = get_registry(registry_name)

    def set_registry_delay(self, registry_delay: int) -> None:
        """Set how long to wait before registering this service.

        Args:
            registry_delay: Delay in milliseconds.
        """
        self.registry_delay = registry_delay

    def set_registry_option(self, registry_option: int) -> None:
        """Enable or disable registry for this service.

        Args:
            registry_option: 0 = off, 1 = on.
        """
        self.registry_option = registry_option

    def set_protocol(self, protocol: str) -> None:
        """Set the service protocol name, e.g. trpc.

        Also installs the codec and the frame checker on the transport option.

        Args:
            protocol: Protocol name, e.g. ``trpc`` or ``http``.
        """
        self.protocol = protocol
        self.codec = get_server_codec(protocol)
        self.transport_option.set_checker(get_server_checker(protocol))

    def set_handler(self, handler: Any) -> None:
        """Set the server transport handle.

        Default is the server itself, which calls codec internally; this
        replaces that handle.

        Args:
            handler: Callable used as ``transport_option.handle``.
        """
        self.transport_option.handle = handler
        self.handler_set = True

    def set_timer(self, spec: str) -> None:
        """Set timer service start spec: [second minute hour day month weekday].

        Example: ``"0 */5 * * * ?"`` runs every 5 minutes.

        Args:
            spec: ``second minute hour day month weekday``,
                e.g. ``"0 */5 * * * ?"`` (every 5 minutes).
        """
        self.timerSpec = spec

    def set_stream_handle(self, stream_handle: StreamDispatcher) -> None:
        """Set the streaming transport handler.

        Args:
            stream_handle: Stream handler used by the stream transport.
        """
        self.stream_handle = stream_handle

    def set_stream_transport(self, stream_transport: Any) -> None:
        """Set the streaming transport.

        Args:
            stream_transport: Stream transport instance.
        """
        self.stream_transport = stream_transport

    def set_max_window_size(self, window_size: int) -> None:
        """Set the stream flow-control window size.

        Args:
            window_size: Window size; 0 means protocol default.
        """
        self.max_window_size = window_size

    def set_current_serialization_type(self, current_serialization_type: int) -> None:
        """Set serialization_type.

        Override the serialization type for this service; -1 means default.

        Args:
            current_serialization_type: Codec serialization id; -1 = default.
        """
        self.current_serialization_type = current_serialization_type

    def set_current_compress_type(self, current_compress_type: int) -> None:
        """Set compress_type.

        Override the compression type for this service; -1 means default.

        Args:
            current_compress_type: Codec compression id; -1 = default.
        """
        self.current_compress_type = current_compress_type
