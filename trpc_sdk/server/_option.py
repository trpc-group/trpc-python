# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Process-model and per-service options consumed by Server / TrpcServerAdapter.

``create_server(yaml)`` copies YAML ``server:`` fields onto :class:`ServerOption`
and each ``server.service[]`` entry onto a :class:`ServiceOption`. The server adapter
reads ``ServerOption.mode`` (``merge``, ``separate`` or ``peer``), the merge-mode
thread model, and how many rpc tasks or peer workers to start. Each ``ServiceAdapter`` keeps its ``ServiceOption``
for listen address, codec, registry, and filters.
"""

from __future__ import annotations

from asyncio import AbstractEventLoop
from dataclasses import dataclass
from dataclasses import field
from typing import Any
from typing import Optional

from trpc_sdk.abc import AddrType
from trpc_sdk.abc import BaseServer
from trpc_sdk.abc import BaseServerTransport
from trpc_sdk.abc import BaseServerTransportOption
from trpc_sdk.abc import BaseServiceRegistry
from trpc_sdk.codec import TrpcServerCodec
from trpc_sdk.codec import get_server_codec
from trpc_sdk.codec import get_server_parser
from trpc_sdk.filters import FilterChain
from trpc_sdk.naming import default_registry
from trpc_sdk.naming import get_registry
from trpc_sdk.stream import StreamDispatcher
from trpc_sdk.transport import DefaultServerTransport
from trpc_sdk.transport import DefaultServerTransportOption
from ._constants import DEFAULT_ADDRESS
from ._constants import DEFAULT_CLOSE_WAIT_TIME
from ._constants import DEFAULT_COMPRESS_TYPE
from ._constants import DEFAULT_DISABLE_REQUEST_TIMEOUT
from ._constants import DEFAULT_ENABLE_STREAM_FILTER
from ._constants import DEFAULT_ENV_NAME
from ._constants import DEFAULT_HANDLE_THREAD_NUM
from ._constants import DEFAULT_HANDLER_MAX_CONCURRENCY
from ._constants import DEFAULT_HANDLER_QUEUE_SIZE
from ._constants import DEFAULT_HANDLER_SET
from ._constants import DEFAULT_IO_HANDLE_TYPE
from ._constants import DEFAULT_IO_WAIT_COUNT
from ._constants import DEFAULT_IO_THREAD_NUM
from ._constants import DEFAULT_MAX_WINDOW_SIZE
from ._constants import DEFAULT_MODE
from ._constants import DEFAULT_NAMESPACE
from ._constants import DEFAULT_PROTOCOL
from ._constants import DEFAULT_QUEUE_WAIT_POLICY
from ._constants import DEFAULT_QUEUE_WAIT_TIMEOUT
from ._constants import DEFAULT_READY_TIMEOUT_MS
from ._constants import DEFAULT_REBOOT_CHILD
from ._constants import DEFAULT_REGISTRY_DELAY
from ._constants import DEFAULT_REGISTRY_OPTION
from ._constants import DEFAULT_SERIALIZATION_TYPE
from ._constants import DEFAULT_SERVICE_NAME
from ._constants import DEFAULT_SERVICE_NUM
from ._constants import DEFAULT_SET_NAME
from ._constants import DEFAULT_SOCKET_TYPE
from ._constants import DEFAULT_TASK_BATCH_SIZE
from ._constants import DEFAULT_TASK_CONSUMER_NUM
from ._constants import DEFAULT_TASK_NUM
from ._constants import DEFAULT_TASK_QUEUE_SIZE
from ._constants import DEFAULT_TASK_WAIT_INTERVAL
from ._constants import DEFAULT_TCP_REUSE_PORT
from ._constants import DEFAULT_TIMEOUT
from ._constants import DEFAULT_TIMER_SPEC
from ._constants import DEFAULT_WORKER_NUM


@dataclass
class ServerOption:
    """Process-wide server options (YAML ``server:``, one per Server).

    Not a listen endpoint. Worker count, process mode, task-queue knobs, and
    child-restart policy live here. Per-port settings are :class:`ServiceOption`.

    The thread- and process-model fields come from YAML ``global.processmodel``
    and hold *resolved* values: ``create_server`` fills them through
    :func:`~trpc_sdk.server._option_resolver.apply_process_model` and then runs
    :func:`~trpc_sdk.server._option_resolver.resolve_server_option` on them.
    Example: ``mode: single_process`` with ``io_num: 2`` becomes
    ``mode="merge"`` and ``io_thread_num=2``.
    """

    # Process model: "merge" / "separate" / "peer".
    mode: str = DEFAULT_MODE
    # Processes of separate mode (rpc tasks) and peer mode (workers,
    # worker-0 included); merge does not read it. ``0`` means the CPU
    # count, resolved by ``create_server`` (or by the worker when the
    # option is built by hand).
    worker_num: int = DEFAULT_WORKER_NUM
    # Thread model of a merge-mode worker: "merge" or "separate". Left
    # empty in YAML it is inferred: separate iff ``handle_thread_num > 0``.
    io_handle_type: str = DEFAULT_IO_HANDLE_TYPE
    # Serving threads. In merge they do both I/O and handlers;
    # in separate they are the I/O loops, the main loop
    # included. 1 in merge stays single-threaded.
    io_thread_num: int = DEFAULT_IO_THREAD_NUM
    # Handle threads that run user code; only read by the separate thread model.
    handle_thread_num: int = DEFAULT_HANDLE_THREAD_NUM
    # Unary requests in flight per serving loop; 0 means unlimited.
    handler_max_concurrency: int = DEFAULT_HANDLER_MAX_CONCURRENCY
    # Per-loop FIFO capacity once the limit is reached; unused when the
    # limit is 0.
    handler_queue_size: int = DEFAULT_HANDLER_QUEUE_SIZE
    # Set SO_REUSEPORT on the TCP listen sockets; shared by merge and
    # separate (merge also applies it to HTTP).
    tcp_reuse_port: bool = DEFAULT_TCP_REUSE_PORT
    # Milliseconds the serving / I/O threads, or the separate-mode rpc
    # tasks, get together to finish startup (task pipes, ``on_start``
    # hooks, self-bound listeners).
    ready_timeout: int = DEFAULT_READY_TIMEOUT_MS
    # Number of ServiceOption entries; filled as len(server.service).
    service_num: int = DEFAULT_SERVICE_NUM
    # Task-process batch wait, in milliseconds.
    task_wait_interval: int = DEFAULT_TASK_WAIT_INTERVAL
    # Max tasks pulled in one task-process batch.
    task_batch_size: int = DEFAULT_TASK_BATCH_SIZE
    # Task-process inbound queue capacity.
    task_queue_size: int = DEFAULT_TASK_QUEUE_SIZE
    # Consumer threads inside each task process.
    task_consumer_num: int = DEFAULT_TASK_CONSUMER_NUM
    # Processes of a task registered without ``task_num``; filled from
    # YAML ``global.processmodel.task_num``.
    task_num: int = DEFAULT_TASK_NUM
    # Restart a crashed child instead of exiting. Not supported in this
    # version: the worker treats it as False and warns when it is set.
    reboot_child: bool = DEFAULT_REBOOT_CHILD
    # Event loop assigned by the server adapter.
    loop: Optional[AbstractEventLoop] = None
    # Drain period on SIGTERM, in milliseconds: the listen sockets are
    # closed and in-flight requests get this long to finish before exit.
    close_wait_time: int = DEFAULT_CLOSE_WAIT_TIME
    # ``inc_io_wait_count`` notifications the worker waits for from its tasks
    # before binding any address; 0 means no wait. Peer mode has no task and
    # ignores it with a warning.
    io_wait_count: int = DEFAULT_IO_WAIT_COUNT


@dataclass
class ServiceOption:
    """One listen endpoint / named service (YAML ``server.service[]``).

    Holds naming (namespace, env, set, service_name), bind address, protocol
    codec, transport, registry, filters, and stream settings. Setters that
    affect I/O also update ``transport_option`` so the listen path stays in
    sync; plain fields are assigned directly.

    Example: YAML ``name: trpc.test.helloworld.Greeter``, ``ip/port``,
    ``protocol: trpc`` becomes this object; ``set_protocol`` installs the
    server codec and the frame parser used by the dispatcher.
    """

    # Owning Server instance; set by create_server(). ServiceAdapter and the
    # registry read ``app.server_adapter`` through this field.
    app: Optional[BaseServer] = None

    # Naming-service namespace: Production vs Development.
    namespace: str = DEFAULT_NAMESPACE
    # Naming-service environment name.
    env_name: str = DEFAULT_ENV_NAME
    # Polaris set name; empty unless global.enable_set is true.
    set_name: str = DEFAULT_SET_NAME
    # RPC service name, e.g. trpc.app.server.Service.
    service_name: str = DEFAULT_SERVICE_NAME

    # Cron spec for a timer service; empty means not a timer.
    # Format: "second minute hour day month weekday", e.g. "0 */5 * * * ?".
    timer_spec: str = DEFAULT_TIMER_SPEC

    # Bind address as (host, port), or (unix_path, 0) for UNIX sockets.
    address: AddrType = DEFAULT_ADDRESS
    # Socket family/type from YAML (tcp / unix / ...).
    socket_type: str = DEFAULT_SOCKET_TYPE
    # Request timeout in milliseconds; also copied to transport idle timeout.
    timeout: int = DEFAULT_TIMEOUT
    # If True, do not inherit the upstream caller's remaining timeout.
    disable_request_timeout: bool = DEFAULT_DISABLE_REQUEST_TIMEOUT
    # Queue-wait timeout in milliseconds; 0 falls back to the merged handler
    # timeout, so the check is off only when that is unlimited too.
    queue_wait_timeout: int = DEFAULT_QUEUE_WAIT_TIMEOUT
    # Queue-wait circuit-breaker policy (0 = off; see YAML queue_wait_policy).
    queue_wait_policy: int = DEFAULT_QUEUE_WAIT_POLICY

    # Override serialization; -1 means use the protocol default.
    current_serialization_type: int = DEFAULT_SERIALIZATION_TYPE
    # Override compression; -1 means use the protocol default.
    current_compress_type: int = DEFAULT_COMPRESS_TYPE

    # Wire protocol name: "trpc", "http", ...
    protocol: str = DEFAULT_PROTOCOL
    # Whether a custom handler is set.
    handler_set: bool = DEFAULT_HANDLER_SET

    # Listen/transport knobs (address, network, parser, TLS, mode).
    # DefaultServerTransportOption is the same class as
    # BaseServerTransportOption; DefaultServerTransport() is a placeholder
    # that create_server() replaces with whatever sits in the plugin slot.
    transport_option: BaseServerTransportOption = field(default_factory=DefaultServerTransportOption)
    # Server transport implementation (default or protocol plugin).
    transport: BaseServerTransport = field(default_factory=DefaultServerTransport)

    # Service registry plugin; default_registry is a shared no-op.
    registry: Optional[BaseServiceRegistry] = default_registry
    # Delay before registering, in milliseconds.
    registry_delay: int = DEFAULT_REGISTRY_DELAY
    # Registry control: 0 = off, 1 = on.
    registry_option: int = DEFAULT_REGISTRY_OPTION
    # Server codec for this protocol; set by set_protocol().
    codec: Optional[TrpcServerCodec] = None

    # Server filter chain (True = server-side chain).
    filters: FilterChain = field(default_factory=lambda: FilterChain(True))

    # Streaming RPC handler; None if this service is unary-only.
    stream_handle: Optional[StreamDispatcher] = None
    # Streaming transport; None uses the default stream transport.
    stream_transport: Any = None
    # Stream flow-control window size; 0 means protocol default.
    max_window_size: int = DEFAULT_MAX_WINDOW_SIZE
    # If True, run stream filters on this service.
    enable_stream_filter: bool = DEFAULT_ENABLE_STREAM_FILTER

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

    def set_registry(self, registry_name: str) -> None:
        """Set the server service registry; one service supports only one registry.

        Args:
            registry_name: Plugin name passed to ``get_registry``.
        """
        self.registry = get_registry(registry_name)

    def set_protocol(self, protocol: str) -> None:
        """Set the service protocol name, e.g. trpc.

        Also installs the codec and the frame parser on the transport option.

        Args:
            protocol: Protocol name, e.g. ``trpc`` or ``http``.
        """
        self.protocol = protocol
        self.codec = get_server_codec(protocol)
        self.transport_option.set_parser(get_server_parser(protocol))

    def set_handler(self, handler: Any) -> None:
        """Set the server transport handle.

        Default is the server itself, which calls codec internally; this
        replaces that handle.

        Args:
            handler: Callable used as ``transport_option.rpc_handler``.
        """
        self.transport_option.rpc_handler = handler
        self.handler_set = True
