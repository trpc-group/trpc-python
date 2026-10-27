# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

from ._context import TrpcContext
from .._server import ServiceDesc

class BaseServiceAdapter:
    """Define request handling and service registration."""

    async def handle(self, context: TrpcContext, request: bytes) -> bytes:
        """Handle one request and return its encoded response.

        Args:
            context: Request-scoped RPC context.
            request: Encoded inbound request bytes.

        Returns:
            Encoded outbound response bytes.
        """
        ...

    def register(self, service_desc: ServiceDesc, fix_rpc_name: bool) -> None:
        """Register unary and streaming handlers for a service.

        Args:
            service_desc: Service methods and streams to publish.
            fix_rpc_name: Whether RPC names should be normalized on register.
        """
        ...

    def init_transport(self) -> None:
        """Resolve and bind this service's transport."""
        ...
