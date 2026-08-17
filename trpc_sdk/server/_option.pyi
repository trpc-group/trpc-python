# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Type hints for server options."""

from asyncio import AbstractEventLoop
from typing import Any
from typing import Callable
from typing import Optional

from trpc.codec import ServerCodec
from trpc.stream.server import StreamDispatcher
from trpc.trpc_abc.registry import Registry
from trpc.trpc_abc.transport import BaseServerTransport
from trpc.trpc_abc.transport import BaseServerTransportOption
from trpc.utils import AddrType

from ._server import Server


class ServerOption:
    """Process-wide server options (YAML ``server:``, one per Server)."""

    mode: str
    worker_num: int
    service_num: int
    task_wait_interval: int
    task_batch_size: int
    task_queue_size: int
    task_consumer_num: int
    on_worker_message: Optional[Callable[[int, bytes], Any]]
    reboot_child: bool
    loop: Optional[AbstractEventLoop]
    close_wait_time: int


class ServiceOption:
    """One listen endpoint / named service (YAML ``server.service[]``)."""

    app: Optional[Server]
    namespace: str
    env_name: str
    set_name: str
    service_name: str
    timerSpec: str
    address: AddrType
    socket_type: str
    timeout: int
    disable_request_timeout: bool
    enable_stream_filter: bool
    queue_wait_timeout: int
    queue_wait_policy: int
    current_serialization_type: int
    current_compress_type: int
    protocol: str
    handler_set: bool
    transport_option: BaseServerTransportOption
    transport: BaseServerTransport
    registry: Optional[Registry]
    registry_delay: int
    registry_option: int
    codec: Optional[ServerCodec]
    filters: Any  # trpc.filter.FilterChain
    stream_handle: Optional[StreamDispatcher]
    stream_transport: Any  # stream send / store_stream_conn / delete_stream_conn
    max_window_size: int

    # Naming / identity.
    def set_namespace(self, namespace: str) -> None: ...

    def set_env_name(self, env_name: str) -> None: ...

    def set_setname(self, set_name: str) -> None: ...

    def set_service_name(self, service_name: str) -> None:
        """Also copied onto ``transport_option``."""
        ...

    # Interceptors.
    def add_filter(self, f: Any) -> None: ...

    # Listen / transport.
    def set_address(self, address: AddrType) -> None:
        """Also copied onto ``transport_option``.

        Raises:
            Exception: If ``address`` is not a 2-tuple or port is ``''``.
        """
        ...

    def set_socket_type(self, socket_type: str) -> None:
        """Also copied onto ``transport_option``."""
        ...

    def set_server_tls(self, cert_file: str, key_file: str) -> None:
        """Reserved no-op; ``trpc.new()`` applies cert/key on ``transport_option``."""
        ...

    def set_network(self, network: str) -> None: ...

    def set_timeout(self, timeout: int) -> None:
        """Also copied to the transport idle timeout."""
        ...

    def set_disable_request_timeout(self, disable_request_timeout: bool) -> None: ...

    def set_enable_stream_filter(self, enable_stream_filter: bool) -> None: ...

    def set_queue_wait_timeout(self, queue_wait_timeout: int) -> None: ...

    def set_queue_wait_policy(self, queue_wait_policy: int) -> None: ...

    def set_transport(self, transport: BaseServerTransport) -> None: ...

    # Registry.
    def set_registry(self, registry_name: str) -> None: ...

    def set_registry_delay(self, registry_delay: int) -> None: ...

    def set_registry_option(self, registry_option: int) -> None: ...

    # Protocol / handler / timer.
    def set_protocol(self, protocol: str) -> None:
        """Also installs the codec and the frame checker on ``transport_option``."""
        ...

    def set_handler(self, handler: Any) -> None:
        """Sets ``transport_option.handle`` and marks ``handler_set`` True."""
        ...

    def set_timer(self, spec: str) -> None: ...

    # Streaming RPC.
    def set_stream_handle(self, stream_handle: StreamDispatcher) -> None: ...

    def set_stream_transport(self, stream_transport: Any) -> None: ...

    def set_max_window_size(self, window_size: int) -> None: ...

    # Codec overrides.
    def set_current_serialization_type(self, current_serialization_type: int) -> None: ...

    def set_current_compress_type(self, current_compress_type: int) -> None: ...
