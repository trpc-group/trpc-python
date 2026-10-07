# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Client option module.

Holds the per-call :class:`Options` container with chainable ``set_*``
methods, and builds a fresh instance from defaults, YAML, then caller
overrides for each request.
"""

from __future__ import annotations

import socket
import ssl
from typing import Any
from typing import Callable
from typing import List
from typing import Mapping
from typing import Optional
from typing import Sequence
from typing import Tuple
from typing import Union

from trpc_sdk.abc import BaseFilter
from trpc_sdk.abc import Node
from trpc_sdk.abc import SelectorOption
from trpc_sdk.abc import TrpcMessage
from trpc_sdk.codec import get_client_checker
from trpc_sdk.codec import get_client_codec
from trpc_sdk.compressor import CompressType
from trpc_sdk.config import SocketType
from trpc_sdk.filters import FilterChain
from trpc_sdk.filters import get_client_filter
from trpc_sdk.naming import TrpcSelector
from trpc_sdk.serialization import SerializationType
from trpc_sdk.transport import ClientTransport
from trpc_sdk.transport import ConnPoolOptions
from trpc_sdk.transport import RoundTripOptions
from trpc_sdk.transport import get_client_transport
from trpc_sdk.transport import get_default_client_transport
from ._config import get_client_service_config


# pylint: disable=too-many-instance-attributes,too-many-public-methods
class Options:
    """
    Client options for TRPC service invocation

    This class encapsulates all configuration parameters needed for making TRPC client calls,
    including service discovery, load balancing, circuit breaking, transport settings, and more.

    Use chainable ``set_*`` methods. An Options instance is also callable so it can be
    passed where a list of option callables is accepted (appliers replay onto the target).
    """

    def __init__(self):
        """Initialize client options with default values"""
        self._appliers: List[Callable[["Options"], None]] = []

        self.namespace = ""

        self.selector = None
        self.selector_options: SelectorOption = SelectorOption()

        self.filters: FilterChain = FilterChain(False)
        self.metadata: dict = {}

        self.service_name: str = ""
        self.caller_service_name: str = ""

        self.endpoint: str = ""
        self.target: str = ""
        self.socket_type: SocketType = SocketType.NET
        self.unix_path: str = ""
        self.network: str = ""

        self.transport: ClientTransport = None
        self.call_options = RoundTripOptions()

        self.codec = None

        self.current_serialization_type: int = -1
        self.serialization_type: int = -1
        self.current_compress_type: int = -1
        self.compress_type: int = -1

        self.req_head = None
        self.rsp_head = None

        self.node: Node = None
        self.timeout: int = 0

        # stream
        self.client_stream_queue_size: int = 0
        self.max_window_size: int = 65535

        self.s_control: Any = None  # Stream control sender - SendControl
        self.r_control: Any = None  # Stream control receiver - ReceiveControl
        self.enable_stream_filter: bool = False

    def __call__(self, opts: "Options") -> None:
        """Replay recorded ``set_*`` appliers onto another Options instance."""
        for apply in self._appliers:
            apply(opts)

    def _record(self, apply: Callable[["Options"], None]) -> "Options":
        self._appliers.append(apply)
        apply(self)
        return self

    def load_client_config(self, name: str):
        """
        Load client configuration from service name

        Retrieves backend configuration based on the service name (typically from proto file)

        Args:
            name: Service name from proto protocol file (callee service name)

        Returns:
            self: Returns the Options instance for method chaining
        """
        service = get_client_service_config(name)
        if service.target:
            self.target = service.target
        else:
            if service.discovery:
                self.selector_options.set_discovery(service.discovery)
            if service.load_balance:
                self.selector_options.set_loadbalance(service.load_balance)
            if service.circuit_breaker:
                self.selector_options.set_circuit_breaker(service.circuit_breaker)

        if service.timeout and service.timeout > 0:
            self.timeout = service.timeout

        if service.service_name:
            self.service_name = service.service_name
            self.endpoint = service.service_name

        if service.namespace:
            self.namespace = service.namespace
            self.selector_options.set_namespace(service.namespace)

        self.serialization_type = service.serialization
        self.compress_type = service.compression

        if service.protocol:
            # Apply without recording on self (config load is not a user overlay).
            Options().set_protocol(service.protocol)(self)

        if service.filters is not None:
            for filter_name in service.filters:
                filter_ins = get_client_filter(filter_name)
                self.filters.add_filter(filter_ins)

        if service.network:
            self.network = service.network
            self.call_options.set_network(service.network)

        if service.password:
            self.call_options.set_password(service.password)

        if service.disable_reuse_http_session is not None:
            self.call_options.set_disable_reuse_http_session(service.disable_reuse_http_session)

        if service.short_conn is not None:
            self.call_options.set_short_conn(service.short_conn)

        pool_opts = ConnPoolOptions(dial_timeout=service.dial_timeout,
                                    idle_timeout=service.idle_time,
                                    max_conn_lifetime=service.max_conn_lifetime,
                                    max_idle=service.max_idle_conn,
                                    max_active=service.max_active_conn,
                                    wait=service.is_wait_active_conn)
        self.call_options.set_pool_options(pool_opts)

        if service.set_name:
            self.selector_options.set_dst_setname(service.set_name)

        if service.env_name:
            self.selector_options.set_dst_env_name(service.env_name)

        self.enable_stream_filter = service.enable_stream_filter

        return self

    def load_node_config(self, node: Node):
        """
        Configure options using node information from registry center

        Sets parameters based on the node information returned by the service registry

        Args:
            node: Node information from service registry

        Returns:
            self: Returns the Options instance for method chaining
        """
        self.call_options.set_address((node.ip, node.port))
        if node.network:
            self.call_options.set_network(node.network)
        if node.protocol:
            Options().set_protocol(node.protocol)(self)
        return self

    def set_namespace(self, namespace: str) -> "Options":
        """Set namespace for backend service environment."""

        def apply(opts: Options):
            opts.namespace = namespace
            opts.selector_options.set_namespace(namespace)

        return self._record(apply)

    def set_service_name(self, service_name: str) -> "Options":
        """Set backend service name."""

        def apply(opts: Options):
            opts.service_name = service_name
            opts.endpoint = service_name

        return self._record(apply)

    def set_caller_service_name(self, caller_service_name: str) -> "Options":
        """Set caller service name."""

        def apply(opts: Options):
            opts.caller_service_name = caller_service_name
            opts.selector_options.set_src_servicename(caller_service_name)

        return self._record(apply)

    def set_caller_namespace(self, caller_namespace: str) -> "Options":
        """Set caller service namespace."""

        def apply(opts: Options):
            opts.selector_options.set_src_namespace(caller_namespace)

        return self._record(apply)

    def set_disable_service_router(self, disable_service_router: bool) -> "Options":
        """Disable or enable service router."""

        def apply(opts: Options):
            opts.selector_options.set_disable_service_router(disable_service_router)

        return self._record(apply)

    def set_env_key(self, env_key: str) -> "Options":
        """Set environment key for service routing."""

        def apply(opts: Options):
            opts.selector_options.set_env_key(env_key)

        return self._record(apply)

    def set_caller_setname(self, caller_setname: str) -> "Options":
        """Set caller set grouping."""

        def apply(opts: Options):
            opts.selector_options.set_src_setname(caller_setname)

        return self._record(apply)

    def set_caller_envname(self, caller_envname: str) -> "Options":
        """Set caller service environment name."""

        def apply(opts: Options):
            opts.selector_options.set_src_env_name(caller_envname)

        return self._record(apply)

    def set_callee_setname(self, callee_setname: str) -> "Options":
        """Set callee set grouping for targeted invocation."""

        def apply(opts: Options):
            opts.selector_options.set_dst_setname(callee_setname)

        return self._record(apply)

    def set_callee_envname(self, callee_envname: str) -> "Options":
        """Set callee service environment."""

        def apply(opts: Options):
            opts.selector_options.set_dst_env_name(callee_envname)

        return self._record(apply)

    def set_balancer_name(self, balancer_name: str) -> "Options":
        """Set load balancer by name."""

        def apply(opts: Options):
            opts.selector_options.set_loadbalance(balancer_name)

        return self._record(apply)

    def set_discovery_name(self, discovery_name: str) -> "Options":
        """Set discovery service by name."""

        def apply(opts: Options):
            opts.selector_options.set_discovery(discovery_name)

        return self._record(apply)

    def set_circuit_breaker_name(self, circuit_breaker_name: str) -> "Options":
        """Set circuit breaker by name."""

        def apply(opts: Options):
            opts.selector_options.set_circuit_breaker(circuit_breaker_name)

        return self._record(apply)

    def set_servicerouter_name(self, servicerouter_name: str) -> "Options":
        """Set service router by name."""

        def apply(opts: Options):
            opts.selector_options.set_service_router(servicerouter_name)

        return self._record(apply)

    def set_key(self, key: str) -> "Options":
        """Set stateful routing hash key."""

        def apply(opts: Options):
            opts.selector_options.set_key(key)

        return self._record(apply)

    def set_target(self, target: str) -> "Options":
        """Set target address schema (e.g. ip://ip:port)."""

        def apply(opts: Options):
            opts.target = target

        return self._record(apply)

    def set_socket_type(self, socket_type: str) -> "Options":
        """Set socket type ('net' or 'unix')."""

        def apply(opts: Options):
            opts.socket_type = socket_type

        return self._record(apply)

    def set_unix_path(self, unix_path: str) -> "Options":
        """Set Unix domain socket path."""

        def apply(opts: Options):
            opts.unix_path = unix_path

        return self._record(apply)

    def set_network(self, network: str) -> "Options":
        """Set peer service network type ('tcp' or 'udp')."""

        def apply(opts: Options):
            opts.network = network
            opts.call_options.set_network(network)

        return self._record(apply)

    def set_password(self, password: str) -> "Options":
        """Set peer service request password."""

        def apply(opts: Options):
            opts.call_options.set_password(password)

        return self._record(apply)

    def set_short_conn(self, short_conn: bool) -> "Options":
        """Set whether to use short connections."""

        def apply(opts: Options):
            opts.call_options.set_short_conn(short_conn)

        return self._record(apply)

    def set_disable_reuse_http_session(self, disable_reuse_http_session: bool) -> "Options":
        """Disable HTTP session reuse when True."""

        def apply(opts: Options):
            opts.call_options.set_disable_reuse_http_session(disable_reuse_http_session)

        return self._record(apply)

    def set_timeout(self, timeout: int) -> "Options":
        """Set backend request timeout in milliseconds."""

        def apply(opts: Options):
            opts.timeout = timeout

        return self._record(apply)

    def set_max_window_size(self, max_window_size: int) -> "Options":
        """Set stream control window size (0 disables flow control)."""

        def apply(opts: Options):
            opts.max_window_size = max_window_size

        return self._record(apply)

    def set_current_serialization_type(self, current_serialization_type: int) -> "Options":
        """Set serialization type for the current request."""

        def apply(opts: Options):
            opts.current_serialization_type = current_serialization_type

        return self._record(apply)

    def set_serialization_type(self, serialization_type: int) -> "Options":
        """Set backend protocol serialization type."""

        def apply(opts: Options):
            opts.serialization_type = serialization_type

        return self._record(apply)

    def set_current_compress_type(self, current_compress_type: int) -> "Options":
        """Set compression type for the current request."""

        def apply(opts: Options):
            opts.current_compress_type = current_compress_type

        return self._record(apply)

    def set_compress_type(self, compress_type: int) -> "Options":
        """Set backend protocol compression type."""

        def apply(opts: Options):
            opts.compress_type = compress_type

        return self._record(apply)

    def set_transport(self, client_transport: ClientTransport) -> "Options":
        """Replace underlying client transport layer."""

        def apply(opts: Options):
            opts.transport = client_transport

        return self._record(apply)

    def set_protocol(self, protocol: str) -> "Options":
        """Set backend service protocol name (e.g. 'trpc')."""

        def apply(opts: Options):
            codec = get_client_codec(protocol)
            if not codec:
                return
            opts.codec = get_client_codec(protocol)
            opts.call_options.set_checker(get_client_checker(protocol))
            trans = get_client_transport(protocol)
            if trans:
                opts.transport = trans

        return self._record(apply)

    def set_filter(self, filter_instance: BaseFilter, override: bool = False) -> "Options":
        """Add a client interceptor filter."""

        def apply(opts: Options):
            if override:
                opts.filters.set_filter([filter_instance])
            else:
                opts.filters.add_filter(filter_instance)

        return self._record(apply)

    def set_filter_list(self, filters: List[BaseFilter], override: bool = False) -> "Options":
        """Add a list of client interceptor filters."""

        def apply(opts: Options):
            if override:
                opts.filters.set_filter(filters)
            else:
                opts.filters.add_filter_list(filters)

        return self._record(apply)

    def set_req_head(self, req_head) -> "Options":
        """Set backend request header."""

        def apply(opts: Options):
            opts.req_head = req_head

        return self._record(apply)

    def set_rsp_head(self, rsp_head) -> "Options":
        """Set backend response header."""

        def apply(opts: Options):
            opts.rsp_head = rsp_head

        return self._record(apply)

    def set_metadata(self, key: str, val: bytes) -> "Options":
        """Set a transparent transmission parameter."""

        def apply(opts: Options):
            opts.metadata[key] = val

        return self._record(apply)

    def set_selector_node(self, node) -> "Options":
        """Set backend selector addressing node result saver."""

        def apply(opts: Options):
            opts.node = node

        return self._record(apply)

    def set_selector_metadata(self, metadata: Mapping[str, str]) -> "Options":
        """Set metadata for the service selector."""

        def apply(opts: Options):
            opts.selector_options.set_metadata(metadata)

        return self._record(apply)

    def set_tls(self, ca_cert: str, cert_file: str = "", key_file: str = "") -> "Options":
        """Set custom HTTPS certificate parameters for backend requests."""

        def apply(opts: Options):
            ssl_context = ssl.create_default_context(cafile=ca_cert)
            if cert_file != "" and key_file != "":
                ssl_context.load_cert_chain(certfile=cert_file, keyfile=key_file)
            opts.call_options.ssl_context = ssl_context

        return self._record(apply)

    def set_pool_opts(self, pool_opts: ConnPoolOptions) -> "Options":
        """Set connection pool configuration options."""

        def apply(opts: Options):
            opts.call_options.set_pool_options(pool_opts)

        return self._record(apply)

    def set_enable_stream_filter(self, enable_stream_filter: bool) -> "Options":
        """Enable or disable stream filter."""

        def apply(opts: Options):
            opts.enable_stream_filter = enable_stream_filter

        return self._record(apply)

    def set_local_address(self, local_addr: Union[Tuple[str, int], str]) -> "Options":
        """Set local address for client connections."""

        def apply(opts: Options):
            addr = local_addr
            if not isinstance(addr, (tuple, str)) or (isinstance(addr, tuple) and len(addr) != 2):
                raise ValueError(f"local_address: {addr} must be a tuple or string")
            if isinstance(addr, tuple):
                ip, port = addr
                if not isinstance(ip, str) or not isinstance(port, int):
                    raise ValueError(f"local_address: {addr} tuple must contain a string and an integer")
            else:
                addr = (addr, 0)
            opts.call_options.set_local_address(addr)

        return self._record(apply)

    def set_target_socket(self, target_socket: socket.socket) -> "Options":
        """Set target socket for client connections."""

        def apply(opts: Options):
            if not isinstance(target_socket, socket.socket):
                raise ValueError(f"target_socket: {target_socket} must be a socket.socket")
            sock_type = 'net'
            if target_socket.family == socket.AF_UNIX:
                sock_type = 'unix'
            network = 'tcp'
            if target_socket.type == socket.SOCK_DGRAM:
                network = 'udp'
            opts.call_options.set_socket_type(network)
            opts.call_options.set_network(sock_type)
            opts.call_options.set_sock(target_socket)

        return self._record(apply)


def _create_client_options(msg: TrpcMessage) -> Options:
    """Build default options before YAML and caller overrides are applied."""
    opts = Options()
    opts.transport = get_default_client_transport()
    opts.selector = TrpcSelector()
    opts.endpoint = msg.callee_service_name
    opts.serialization_type = SerializationType.PB
    opts.compress_type = CompressType.Noop
    return opts


def _update_namespace_of_option(msg: TrpcMessage, opts: Options):
    """Copy caller namespace / env fields from ``msg`` onto selector options."""
    if len(opts.selector_options.namespace) == 0:
        opts.selector_options.set_namespace(msg.namespace)
    opts.selector_options.set_src_namespace(msg.namespace)
    opts.selector_options.set_src_servicename(msg.caller_service_name)
    opts.selector_options.set_src_env_name(msg.env_name)
    opts.selector_options.set_env_transfer(msg.env_transfer)
    opts.selector_options.set_src_setname(msg.set_name)


def normalize_options(
        options: Optional[Union[Options, Callable, Sequence]] = None) -> List[Callable]:
    """Normalize user options into a list of callables.

    Accepts a single :class:`Options`, a single callable, a sequence of either,
    or ``None``. :class:`Options` instances are callable and replay their
    recorded ``set_*`` appliers.
    """
    if options is None:
        return []
    if isinstance(options, Options) or callable(options):
        return [options]
    return list(options)


def get_options(msg: TrpcMessage, options: Optional[Union[Options, Callable, Sequence]] = None) -> Options:
    """Build per-call options from defaults, YAML, then caller overrides.

    Caller-supplied ``options`` win last. If the caller set
    ``service_name``, that value is also written back onto ``msg`` as
    the callee service name.

    Args:
        msg: Outbound message; callee service name selects YAML config.
        options: One :class:`Options`, a callable, a sequence of either,
            or ``None``.

    Returns:
        A new :class:`Options` instance for this request.
    """
    opts = _create_client_options(msg)
    opts.load_client_config(msg.callee_service_name)

    if len(msg.namespace) > 0:
        _update_namespace_of_option(msg, opts)

    for func in normalize_options(options):
        func(opts)

    if opts.service_name:
        msg.set_callee_service_name(opts.service_name)

    return opts
