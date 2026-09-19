# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

from typing import Any
from typing import Callable
from typing import Dict
from typing import Optional

from trpc_sdk.abc import BaseService
from .._option import ServiceOption
from .._registry import RegistryThread
from .._types import QueueWindow


class Service(BaseService):
    """Per-service handler path after the transport has a complete request frame."""

    opt: ServiceOption
    unary_handlers: Dict[str, Any]  # rpc name -> trpc_sdk.abc.Method
    stream_handlers: Dict[str, Any]  # rpc name -> trpc_sdk.abc.StreamDesc
    queue_window: QueueWindow
    registry_thread: Optional[RegistryThread]

    def __init__(self, service_option: ServiceOption) -> None: ...

    async def handle(self, ctx: Any, req_buf: bytes) -> bytes:
        """Unary path: decode, run the filter chain, encode.

        Stream frames return from ``_handle_stream`` before timeout /
        queue-wait checks. Errors are encoded into a response frame rather
        than raised.
        ``ctx`` is ``trpc_sdk.abc.TrpcContext``.
        """
        ...

    async def handle_http(self, ctx: Any, route_func: Callable) -> Any:
        """HTTP route path; returns the filter-chain result, not a tRPC frame.

        Uses ``opt.filters`` directly: the request is already on
        ``ctx.get_meta_data("request")``, so ``_prepare_filter_chain``
        (decompress + deserialize) is skipped.
        ``ctx`` is ``trpc_sdk.abc.TrpcContext``.
        """
        ...

    def register(self, service_desc: Any, fix_rpc_name: bool) -> None:
        """Install methods / streams from ``ServiceDesc``.

        When ``fix_rpc_name`` is True, the service segment of each method name
        is rewritten to ``opt.service_name``. A stream handler also sets
        ``transport_option`` idle timeout to 0.
        ``service_desc`` is ``trpc_sdk.abc.ServiceDesc``.
        """
        ...

    def run(self) -> None:
        """Blocking ``listen_and_serve`` on this service's transport."""
        ...

    def start_registry(self) -> None:
        """No-op if ``opt.registry`` is None; otherwise start a ``RegistryThread``."""
        ...

    def close(self) -> None:
        """Stop the registry thread if one is running; always returns None."""
        ...


def create_service(service_name: str, service_option: ServiceOption) -> Service:
    """Build one :class:`Service` for a listen endpoint.

    Fills in the default transport and transport option when the caller
    omitted them, installs :meth:`Service.handle` unless a handler is
    already set, and binds the service onto ``transport_option``.
    """
    ...


def start_service_registry(server: Any) -> None:
    """Start naming-service registration for every service on ``server``.

    :func:`create_server` registers this as a fork hook. ``registry_after_fork``
    decides whether it runs in the parent before or after fork.
    """
    ...
