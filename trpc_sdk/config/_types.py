# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.
"""Configuration data models.

Every section of the framework config file is described here as a dataclass:
:class:`Config` is the top level, holding :class:`GlobalConfig`,
:class:`ServerConfig` and :class:`ClientConfig` sub-trees.

These classes carry no behaviour. Values are filled in by
:class:`~trpc_sdk.config._parser.TrpcConfigParser`, which maps config-file keys onto
attributes through type annotations and the ``:param attr: \\`config_key\\```
docstring aliases used below (for example ``global`` -> ``global_config``).
"""

from __future__ import annotations

from dataclasses import dataclass
from dataclasses import field
from enum import Enum
from enum import unique
from typing import List
from typing import Optional

from trpc_sdk.common import get_max_procs
from ._constants import DEFAULT_BIND_INFO_FILE
from ._constants import DEFAULT_CLIENT_DIAL_TIMEOUT_MS
from ._constants import DEFAULT_CLIENT_IDLE_TIME_S
from ._constants import DEFAULT_CLIENT_MAX_IDLE_CONN
from ._constants import DEFAULT_CLIENT_NETWORK
from ._constants import DEFAULT_CLIENT_PROTOCOL
from ._constants import DEFAULT_MAX_READ_BUF_SIZE
from ._constants import DEFAULT_MEMORY_INSPECTION_FILENAME_PREFIX
from ._constants import DEFAULT_MEMORY_INSPECTION_LINE_LIMIT
from ._constants import DEFAULT_QUEUE_CB_INTERVAL_MS
from ._constants import DEFAULT_QUEUE_CB_RATIO
from ._constants import DEFAULT_QUEUE_CB_THRESHOLD
from ._constants import DEFAULT_RPCZ_CACHE_EXPIRE_INTERVAL_S
from ._constants import DEFAULT_RPCZ_COLLECT_INTERVAL_MS
from ._constants import DEFAULT_RPCZ_HIGH_WATER_LEVEL
from ._constants import DEFAULT_RPCZ_LOWER_WATER_LEVEL
from ._constants import DEFAULT_RPCZ_PRINT_SPANS_NUM
from ._constants import DEFAULT_RPCZ_REMOVE_INTERVAL_MS
from ._constants import DEFAULT_RPCZ_SAMPLE_RATE
from ._constants import DEFAULT_STREAM_MAX_WINDOW_SIZE
from ._constants import DEFAULT_TASK_QUEUE_SIZE
from ._constants import DEFAULT_TASK_WAIT_INTERVAL_MS
from ._constants import DEFAULT_WORKER_NUM_FALLBACK
from ._constants import UNIX_TYPE


@unique
class SocketType(str, Enum):
    """Socket type constants for server binding."""

    NET: str = "net"
    UNIX: str = UNIX_TYPE


@dataclass
class SaveBindInfo:
    """Configuration for saving bind information to a file."""

    enable: bool = False
    file_name: str = DEFAULT_BIND_INFO_FILE


@dataclass
class Rpcz:
    """Rpcz sampling and caching configuration."""

    sample_rate: int = DEFAULT_RPCZ_SAMPLE_RATE  # 0 = no sampling, N = sample when span_id % N hits
    lower_water_level: int = DEFAULT_RPCZ_LOWER_WATER_LEVEL  # per-second sampling low water mark
    high_water_level: int = DEFAULT_RPCZ_HIGH_WATER_LEVEL  # per-second sampling high water mark
    sample_minimum_timecost: int = 0  # minimum time cost threshold for sampling (ms)
    collect_interval_ms: int = DEFAULT_RPCZ_COLLECT_INTERVAL_MS  # sampling data collection interval (ms)
    remove_interval_ms: int = DEFAULT_RPCZ_REMOVE_INTERVAL_MS  # expired data cleanup interval (ms)
    print_spans_num: int = DEFAULT_RPCZ_PRINT_SPANS_NUM  # default number of summary spans returned by admin
    cache_expire_interval: int = DEFAULT_RPCZ_CACHE_EXPIRE_INTERVAL_S  # span cache expiration time (s)
    service_whitelist: Optional[List[str]] = None  # empty means no filtering

    def __post_init__(self) -> None:
        if self.service_whitelist is None:
            self.service_whitelist = []


@dataclass
class ProcessModel:
    """Process and thread model (YAML ``global.processmodel``).

    Zero or empty means "not configured".
    :func:`~trpc_sdk.server._option_resolver.apply_process_model` translates
    this block into the ``mode`` / ``worker_num`` / ``io_handle_type`` /
    ``io_thread_num`` / ``handle_thread_num`` fields of ``ServerOption``.
    """

    mode: str = ''  # single_process | multi_process; '' means single_process with one thread
    # merge | separate; '' means merge for single_process and separate for multi_process.
    # multi_process also accepts peer as a spelling of merge.
    io_handle_type: str = ''
    # single_process: I/O threads (must equal handle_num under merge);
    # multi_process separate: I/O processes, only 1 is supported; peer: not read.
    io_num: int = 0
    # single_process: handle threads; multi_process: rpc tasks (separate) or
    # peer workers (merge), 0 meaning the CPU count.
    handle_num: int = 0
    task_num: int = 0  # processes of a task that sets no task_num itself; 0 means 1


@dataclass
class GlobalConfig:
    """Global framework configuration."""

    namespace: str = ''  # Peer service environment, production or testing.
    env_name: str = ''  # Peer service environment name, production or testing.
    container_name: str = ''  # Peer service container name.
    local_ip: str = ''  # Peer service local IP address.
    enable_set: bool = False  # Enable set routing; full_set_name is used when True.
    full_set_name: str = ''  # Peer service full set name.
    enable_uvloop: bool = False  # Use the uvloop event-loop policy when uvloop is installed.
    save_bind_info: Optional[SaveBindInfo] = None  # Save bind information to a file.
    rpcz: Optional[Rpcz] = None  # Rpcz sampling and caching configuration.
    processmodel: Optional[ProcessModel] = None  # Process and thread model of the server.

    def __post_init__(self) -> None:
        if self.save_bind_info is None:
            self.save_bind_info = SaveBindInfo()
        if self.rpcz is None:
            self.rpcz = Rpcz()
        if self.processmodel is None:
            self.processmodel = ProcessModel()


@dataclass
class ServerService:
    """Server-side service configuration.

    :param filters: `filter` service-level interceptors
    """

    disable_request_timeout: bool = False  # disable inheriting upstream timeout
    enable_stream_filter: bool = False  # enable stream filter
    name: str = ''  # service name for naming service: trpc.app.server.service
    socket_type: str = SocketType.NET  # "net" (network) or "unix" (unix domain socket)
    ip: str = ''  # listening IP address
    nic: str = ''  # NIC to bind; leave empty when IP/port is assigned by ops
    port: int = 0  # listening port
    unix_path: str = ''  # unix socket path (effective when socket_type=unix)
    network: str = ''  # network type: tcp / udp
    address: str = ''  # full listen address; overrides ip:port when set
    protocol: str = ''  # application protocol, e.g. trpc
    timeout: int = 0  # max handler processing time (ms)
    idletime: int = 0  # idle timeout for persistent connections (ms)
    registry: str = ""  # service registry name, e.g. polaris
    filters: Optional[List[str]] = None  # service-level interceptors
    tls_key: str = ''  # TLS private key path
    tls_cert: str = ''  # TLS certificate path
    target: str = ''  # custom target address
    max_window_size: int = DEFAULT_STREAM_MAX_WINDOW_SIZE  # stream flow-control receive window size

    def __post_init__(self) -> None:
        if self.filters is None:
            self.filters = []


@dataclass
class Admin:
    """Admin service configuration."""

    ip: str = ''  # NIC address to bind, e.g. 127.0.0.1
    nic: str = ''
    port: int = 0  # port to bind, default 9028
    read_timeout: int = 0  # request read timeout (ms), guards against slow clients
    write_timeout: int = 0  # response write timeout (ms)
    enable_tls: bool = False  # enable TLS


@dataclass
class QueueCircuitBreakerConfig:
    """Queue timeout circuit-breaker configuration."""

    ratio: float = DEFAULT_QUEUE_CB_RATIO  # timeout ratio threshold
    threshold: int = DEFAULT_QUEUE_CB_THRESHOLD  # minimum sample count for statistics
    interval: int = DEFAULT_QUEUE_CB_INTERVAL_MS  # statistics window size (ms)


@dataclass
class MemoryInspection:
    """Memory allocation inspection configuration."""

    enabled: bool = False  # enable memory inspection
    line_limit: int = DEFAULT_MEMORY_INSPECTION_LINE_LIMIT  # max debug info lines, -1 for unlimited
    filename_prefix: str = DEFAULT_MEMORY_INSPECTION_FILENAME_PREFIX  # debug dump filename prefix


def default_worker_num() -> int:
    """Return the process count ``global.processmodel.handle_num: 0`` resolves to under ``multi_process``.

    The CPU count the process may use (cgroup quota included), or
    :data:`DEFAULT_WORKER_NUM_FALLBACK` when it is unknown. Read at call
    time, never frozen at import.
    """
    return get_max_procs() or DEFAULT_WORKER_NUM_FALLBACK


def _default_max_read_buf_size() -> int:
    return DEFAULT_MAX_READ_BUF_SIZE


@dataclass
class ServerConfig:
    """Server configuration.

    :param filters: `filter` server-level interceptors
    """

    app: str = ''
    server: str = ''
    bin_path: str = ''
    conf_path: str = ''
    data_path: str = ''
    admin: Optional[Admin] = None
    service: Optional[List[ServerService]] = None
    filters: Optional[List[str]] = None
    network: str = ''
    protocol: str = ''
    handler_max_concurrency: int = 0  # handle threads only: unary requests in flight per handle thread; 0 = unlimited
    handler_queue_size: int = 1024  # handle threads only: shared overflow queue; used when handler_max_concurrency > 0
    tcp_reuse_port: bool = False  # SO_REUSEPORT on TCP listen sockets, shared by merge and separate
    ready_timeout: int = 0  # ms each thread / rpc task gets to start (on_start hooks included); 0 means 300000
    max_rbuf_size: int = field(default_factory=_default_max_read_buf_size)  # default: 8 MiB
    registry_delay: int = 0  # registry delay (ms)
    registry_option: int = 0  # registry control: 0=off, 1=on
    check_timeout: bool = False  # server-side timeout check; returns empty + error code directly
    check_idle_timeout: int = 0  # connection idle timeout check interval
    queue_wait_timeout: int = 0  # request queue wait timeout (ms)
    queue_wait_policy: int = 0  # request queue wait policy
    queue_cb: Optional[QueueCircuitBreakerConfig] = None  # queue timeout circuit-breaker config
    task_wait_interval: int = DEFAULT_TASK_WAIT_INTERVAL_MS  # task process batch wait duration (ms)
    task_batch_size: int = 1  # task process batch size
    task_queue_size: int = DEFAULT_TASK_QUEUE_SIZE  # task process queue size
    task_consumer_num: int = 1  # task process consumer thread count
    reboot_child: bool = False  # restart child process on failure
    encode_base64: bool = False  # use base64 encoding
    registry_after_fork: bool = False  # no effect: services register once the worker listens
    memory_inspection: Optional[MemoryInspection] = None  # memory inspection config
    heartbeat_interval: int = 0  # process heartbeat interval (s)
    close_wait_time: int = 0  # graceful shutdown wait time (ms)
    io_wait_count: int = 0  # inc_io_wait_count notifications to wait for before binding; 0 means no wait

    def __post_init__(self) -> None:
        if self.service is None:
            self.service = []
        if self.filters is None:
            self.filters = []
        if self.queue_cb is None:
            self.queue_cb = QueueCircuitBreakerConfig()
        if not self.memory_inspection:
            self.memory_inspection = MemoryInspection()


@dataclass
class ClientServiceConfig:
    """Client-side service configuration.

    :param callee: `callee` Callee service name from the peer's protocol file;
        the config file keys its parameters by this name.
    :param service_name: `name` Real naming-service name of the peer service.
    :param namespace: `namespace` Peer service environment, production or testing.
    :param target:
    :param password:
    :param discovery:
    :param load_balance: `loadbalance`
    :param circuit_breaker: `circuitbreaker`
    :param network:
    :param timeout:
    :param protocol:
    :param filters: `filter`
    :param disable_reuse_http_session:
    :param enable_stream_filter:
    :param short_conn:
    :param dial_timeout: `dial_timeout` Connection establishment timeout.
    :param idle_time: `idle_time` Idle connection timeout.
    :param max_conn_lifetime: `max_conn_lifetime` Maximum lifetime of a pooled connection.
    :param max_idle_conn: `max_idle_conn` Maximum number of idle connections in the pool.
    :param max_active_conn: `max_active_conn` Maximum number of active connections in the pool.
    :param is_wait_active_conn: `is_wait_active_conn` Whether to wait for an available
        connection once the pool's active connection limit is reached.
    :param set_name:
    :param env_name:
    """

    callee: Optional[str] = None
    service_name: Optional[str] = None
    namespace: Optional[str] = None
    # Defaults to Polaris and is usually left unset, e.g. cl5://servicename or l5://sid
    target: Optional[str] = None
    password: Optional[str] = None
    discovery: Optional[str] = None
    load_balance: Optional[str] = None
    circuit_breaker: Optional[str] = None
    network: str = DEFAULT_CLIENT_NETWORK
    timeout: int = 0
    protocol: str = DEFAULT_CLIENT_PROTOCOL
    serialization: int = 0
    compression: int = 0
    filters: Optional[List[str]] = None
    disable_reuse_http_session: Optional[bool] = None
    enable_stream_filter: bool = False
    short_conn: Optional[bool] = None
    dial_timeout: float = DEFAULT_CLIENT_DIAL_TIMEOUT_MS
    idle_time: float = DEFAULT_CLIENT_IDLE_TIME_S
    max_conn_lifetime: float = 0.0
    max_idle_conn: int = DEFAULT_CLIENT_MAX_IDLE_CONN
    max_active_conn: int = 0
    is_wait_active_conn: bool = False
    set_name: Optional[str] = None
    env_name: Optional[str] = None


@dataclass
class ClientConfig:
    """Client config

    :param load_balance: `loadbalance`
    :param circuit_breaker: `circuitbreaker`
    :param filters: `filter`
    """

    network: str = ''
    protocol: str = ''
    timeout: int = 0
    namespace: str = ''
    discovery: str = ''
    load_balance: str = ''
    circuit_breaker: str = ''
    filters: Optional[List[str]] = None
    service: Optional[List[ClientServiceConfig]] = None

    def __post_init__(self) -> None:
        if self.filters is None:
            self.filters = []
        if self.service is None:
            self.service = []


@dataclass
class Config:
    """Top-level config holding global, server and client configuration.

    :param global_config: `global` global
    :param server: `server` server
    :param client: `client` client
    """

    global_config: GlobalConfig = field(default_factory=GlobalConfig)
    server: ServerConfig = field(default_factory=ServerConfig)
    client: ClientConfig = field(default_factory=ClientConfig)
