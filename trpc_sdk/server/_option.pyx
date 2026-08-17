# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

# cython: language_level=3, boundscheck=False, wraparound=False

"""Process-model and per-service options consumed by Server / TrpcServerEngine.

``trpc.new(yaml)`` copies YAML ``server:`` fields onto :class:`ServerOption`
and each ``server.service[]`` entry onto a :class:`ServiceOption`. The engine
reads ``ServerOption`` to decide dispatcher vs multi-listener and how many
workers/tasks to fork. Each ``Service`` keeps its ``ServiceOption`` for listen
address, codec, registry, and filters.
"""

from trpc import filter
from trpc import registry
from ..codec import codec as codec
from ..transport import server_transport as transport
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
from ._constants import NETWORK_HTTP
from ._constants import NETWORK_TCP
from ._constants import NETWORK_UDP


cdef class ServerOption:
    """Process-wide server options (YAML ``server:``, one per Server).

    Not a listen endpoint. Worker count, process mode, task-queue knobs, and
    child-restart policy live here. Per-port settings are :class:`ServiceOption`.

    Example: ``server.mode: multi-listener`` and ``server.worker_num: 4`` become
    ``mode="multi-listener"`` and ``worker_num=4``; the engine then skips the
    dispatcher and starts four executor processes that listen themselves.
    """

    def __cinit__(self):
        # "dispatcher" (one dispatcher + N workers) or "multi-listener".
        self.mode = DEFAULT_MODE
        # Worker (or executor) process count. YAML default is CPU core count.
        self.worker_num = DEFAULT_WORKER_NUM
        # Number of ServiceOption entries; filled as len(server.service).
        self.service_num = DEFAULT_SERVICE_NUM
        # Task-process batch wait, in milliseconds.
        self.task_wait_interval = DEFAULT_TASK_WAIT_INTERVAL
        # Max tasks pulled in one task-process batch.
        self.task_batch_size = DEFAULT_TASK_BATCH_SIZE
        # Task-process inbound queue capacity.
        self.task_queue_size = DEFAULT_TASK_QUEUE_SIZE
        # Consumer threads inside each task process.
        self.task_consumer_num = DEFAULT_TASK_CONSUMER_NUM
        # Optional callback for master -> worker message-pipe payloads.
        self.on_worker_message = None
        # If True, master restarts a crashed worker/task child.
        self.reboot_child = DEFAULT_REBOOT_CHILD
        # Graceful-shutdown wait on SIGTERM, in milliseconds.
        self.close_wait_time = DEFAULT_CLOSE_WAIT_TIME


cdef class ServiceOption:
    """One listen endpoint / named service (YAML ``server.service[]``).

    Holds naming (namespace, env, set, service_name), bind address, protocol
    codec, transport, registry, filters, and stream settings. Setters that
    affect I/O also update ``transport_option`` so the listen path stays in
    sync.

    Example: YAML ``name: trpc.test.helloworld.Greeter``, ``ip/port``,
    ``protocol: trpc`` becomes this object; ``set_protocol`` installs the
    server codec and the frame checker used by the dispatcher.
    """

    def __cinit__(self):
        # Owning Server instance; set by trpc.new().
        self.app = None

        # Naming-service namespace: Production vs Development.
        self.namespace = DEFAULT_NAMESPACE
        # Naming-service environment name.
        self.env_name = DEFAULT_ENV_NAME
        # Polaris set name; empty unless global.enable_set is Y.
        self.set_name = DEFAULT_SET_NAME
        # RPC service name, e.g. trpc.app.server.Service.
        self.service_name = DEFAULT_SERVICE_NAME

        # Cron spec for a timer service; empty means not a timer.
        # Format: "second minute hour day month weekday", e.g. "0 */5 * * * ?".
        self.timerSpec = DEFAULT_TIMER_SPEC

        # Bind address as (host, port), or (unix_path, 0) for UNIX sockets.
        self.address = DEFAULT_ADDRESS
        # Socket family/type from YAML (tcp / unix / ...).
        self.socket_type = DEFAULT_SOCKET_TYPE
        # Request timeout in milliseconds; also copied to transport idle timeout.
        self.timeout = DEFAULT_TIMEOUT
        # If True, do not inherit the upstream caller's remaining timeout.
        self.disable_request_timeout = DEFAULT_DISABLE_REQUEST_TIMEOUT
        # Queue-wait timeout in milliseconds before a request is dropped.
        self.queue_wait_timeout = DEFAULT_QUEUE_WAIT_TIMEOUT
        # Queue-wait circuit-breaker policy (0 = off; see YAML queue_wait_policy).
        self.queue_wait_policy = DEFAULT_QUEUE_WAIT_POLICY

        # Override serialization; -1 means use the protocol default.
        self.current_serialization_type = DEFAULT_SERIALIZATION_TYPE
        # Override compression; -1 means use the protocol default.
        self.current_compress_type = DEFAULT_COMPRESS_TYPE

        # Wire protocol name: "trpc", "http", ...
        self.protocol = DEFAULT_PROTOCOL
        # Whether a custom handler is set.
        self.handler_set = DEFAULT_HANDLER_SET

        # Listen/transport knobs (address, network, checker, TLS, mode).
        self.transport_option = transport.DefaultServerTransportOption()
        # Server transport implementation (default or protocol plugin).
        self.transport = transport.DefaultServerTransport()

        # Service registry plugin; defaultRegistry is a no-op.
        self.registry = registry.defaultRegistry
        # Delay before registering, in milliseconds.
        self.registry_delay = DEFAULT_REGISTRY_DELAY
        # Registry control: 0 = off, 1 = on.
        self.registry_option = DEFAULT_REGISTRY_OPTION
        # Server codec for this protocol; set by set_protocol().
        self.codec = None

        # Server filter chain (True = server-side chain).
        self.filters = filter.FilterChain(True)

        # Streaming RPC handler; None if this service is unary-only.
        self.stream_handle = None
        # Streaming transport; None uses the default stream transport.
        self.stream_transport = None
        # Stream flow-control window size; 0 means protocol default.
        self.max_window_size = DEFAULT_MAX_WINDOW_SIZE
        # If True, run stream filters on this service.
        self.enable_stream_filter = DEFAULT_ENABLE_STREAM_FILTER

    cpdef void set_namespace(self, str namespace):
        """Set the namespace.

        Naming-service namespace: Production vs Development.

        Args:
            namespace: Namespace string from YAML ``global.namespace``.
        """
        self.namespace = namespace

    cpdef void set_env_name(self, str env_name):
        """Set the current environment.

        Naming-service environment name from YAML ``global.env_name``.

        Args:
            env_name: Environment string from YAML ``global.env_name``.
        """
        self.env_name = env_name

    cpdef void set_setname(self, str set_name):
        """Set the set grouping.

        Polaris set name; typically ``global.full_set_name`` when enable_set is Y.

        Args:
            set_name: Set name; typically ``global.full_set_name`` when enable_set is Y.
        """
        self.set_name = set_name

    cpdef void set_service_name(self, str service_name):
        """Set the current service's service name.

        Also copied onto the transport option.

        Args:
            service_name: Full service name, e.g. ``trpc.test.helloworld.Greeter``.
        """
        self.service_name = service_name
        self.transport_option.set_service_name(service_name)

    cpdef void add_filter(self, object f):
        """Add a server interceptor that runs before and after the business handler.

        Args:
            f: Filter instance from ``get_server_filter``.
        """
        self.filters.add_filter(f)

    cpdef void set_address(self, object address):
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

    cpdef void set_socket_type(self, object socket_type):
        """Set the socket type.

        Also copied onto the transport option.

        Args:
            socket_type: YAML ``socket_type`` (e.g. TCP vs UNIX).
        """
        self.transport_option.set_socket_type(socket_type)
        self.socket_type = socket_type

    cpdef void set_server_tls(self, str cert_file, str key_file):
        """Set the server TLS file paths.

        Reserved; currently a no-op. Cert/key are applied on
        ``transport_option`` in ``trpc.new()``.

        Args:
            cert_file: Path to the server certificate.
            key_file: Path to the private key.
        """
        # TODO: self.transport_option.set_server_tls(cert_file, key_file)
        pass

    cpdef void set_network(self, str network):
        """Set the server listen network (tcp or udp); default is tcp.

        Applied on the transport option. HTTP is also a valid YAML value.

        Args:
            network: YAML ``network``; matched against NETWORK_TCP/UDP/HTTP.
        """
        self.transport_option.set_network(network)

    cpdef void set_timeout(self, int timeout):
        """Set the overall timeout for a single request; default is 1s.

        Also copied to the transport idle timeout. The field starts at 0 until
        YAML load fills it.

        Args:
            timeout: Timeout in milliseconds from YAML ``service.timeout``.
        """
        self.transport_option.set_idle_timeout(timeout)
        self.timeout = timeout

    cpdef void set_disable_request_timeout(self, bint disable_request_timeout):
        """Choose whether to ignore the upstream remaining timeout.

        Args:
            disable_request_timeout: True to use only this service's timeout.
        """
        self.disable_request_timeout = disable_request_timeout

    cpdef void set_enable_stream_filter(self, bint enable_stream_filter):
        """Enable or disable stream filters for this service.

        Args:
            enable_stream_filter: YAML ``enable_stream_filter``.
        """
        self.enable_stream_filter = enable_stream_filter

    cpdef void set_queue_wait_timeout(self, int queue_wait_timeout):
        """Set how long a request may wait in the server queue.

        Args:
            queue_wait_timeout: Timeout in milliseconds; 0 disables the check.
        """
        self.queue_wait_timeout = queue_wait_timeout

    cpdef void set_queue_wait_policy(self, int queue_wait_policy):
        """Set the queue-wait circuit-breaker policy.

        Args:
            queue_wait_policy: YAML ``queue_wait_policy``; 0 means off.
        """
        self.queue_wait_policy = queue_wait_policy

    cpdef void set_transport(self, object transport):
        """Replace the underlying server transport.

        Args:
            transport: ``BaseServerTransport`` instance (plugin or default).
        """
        self.transport = transport

    cpdef void set_registry(self, str registry_name):
        """Set the server service registry; one service supports only one registry.

        Args:
            registry_name: Plugin name passed to ``registry.get_registry``.
        """
        self.registry = registry.get_registry(registry_name)

    cpdef void set_registry_delay(self, int registry_delay):
        """Set how long to wait before registering this service.

        Args:
            registry_delay: Delay in milliseconds.
        """
        self.registry_delay = registry_delay

    cpdef void set_registry_option(self, int registry_option):
        """Enable or disable registry for this service.

        Args:
            registry_option: 0 = off, 1 = on.
        """
        self.registry_option = registry_option

    cpdef void set_protocol(self, str protocol):
        """Set the service protocol name, e.g. trpc.

        Also installs the codec and the frame checker on the transport option.

        Args:
            protocol: Protocol name, e.g. ``trpc`` or ``http``.
        """
        self.protocol = protocol
        self.codec = codec.get_server_codec(protocol)
        self.transport_option.set_checker(codec.get_server_checker(protocol))

    cpdef void set_handler(self, handler):
        """Set the server transport handle.

        Default is the server itself, which calls codec internally; this
        replaces that handle.

        Args:
            handler: Callable used as ``transport_option.handle``.
        """
        self.transport_option.handle = handler
        self.handler_set = True

    cpdef void set_timer(self, str spec):
        """Set timer service start spec: [second minute hour day month weekday].

        Example: ``"0 */5 * * * ?"`` runs every 5 minutes.

        Args:
            spec: ``second minute hour day month weekday``,
                e.g. ``"0 */5 * * * ?"`` (every 5 minutes).
        """
        self.timerSpec = spec

    cpdef void set_stream_handle(self, stream_handle):
        """Set the streaming transport handler.

        Args:
            stream_handle: Stream handler used by the stream transport.
        """
        self.stream_handle = stream_handle

    cpdef void set_stream_transport(self, stream_transport):
        """Set the streaming transport.

        Args:
            stream_transport: Stream transport instance.
        """
        self.stream_transport = stream_transport

    cpdef void set_max_window_size(self, int window_size):
        """Set the stream flow-control window size.

        Args:
            window_size: Window size; 0 means protocol default.
        """
        self.max_window_size = window_size

    cpdef void set_current_serialization_type(self, int current_serialization_type):
        """Set serialization_type.

        Override the serialization type for this service; -1 means default.

        Args:
            current_serialization_type: Codec serialization id; -1 = default.
        """
        self.current_serialization_type = current_serialization_type

    cpdef void set_current_compress_type(self, int current_compress_type):
        """Set compress_type.

        Override the compression type for this service; -1 means default.

        Args:
            current_compress_type: Codec compression id; -1 = default.
        """
        self.current_compress_type = current_compress_type
