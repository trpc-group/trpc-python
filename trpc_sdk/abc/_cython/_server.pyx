# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

# cython: language_level=3, boundscheck=False, wraparound=False

"""Cython base for request handling and service registration.

``cdef class ServiceAdapter`` must inherit an extension type. This module is that
base; the public Python contract is re-exported from ``trpc_sdk.abc._server``.
"""

from ._context cimport TrpcContext


cdef class BaseServiceAdapter:
    """Define request handling and service registration."""

    async def handle(self, TrpcContext context, bytes request):
        """Handle one request and return its encoded response.

        Args:
            context: Request-scoped RPC context.
            request: Encoded inbound request bytes.

        Returns:
            Encoded outbound response bytes.
        """
        raise NotImplementedError("BaseServiceAdapter.handle is not implemented")

    cpdef void register(self, object service_desc, bint fix_rpc_name):
        """Register unary and streaming handlers for a service.

        Args:
            service_desc: Service methods and streams to publish.
            fix_rpc_name: Whether RPC names should be normalized on register.
        """
        raise NotImplementedError("BaseServiceAdapter.register is not implemented")

    cpdef void init_transport(self):
        """Resolve and bind this service's transport."""
        raise NotImplementedError("BaseServiceAdapter.init_transport is not implemented")
