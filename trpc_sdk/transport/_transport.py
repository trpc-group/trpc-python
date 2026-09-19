# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""tRPC server transport that registers listen endpoints on the engine.

This class does not accept connections itself and never has. It records one
service's listen address, checker, handler, timeout and transport options on
:class:`TrpcServerEngine`, index-aligned with every other service, and the
worker binds them later.

There used to be two registration paths here, one per process model. They are
gone: both merge and separate workers read the same nine tables, and the
choice they encoded — which children a master would fork — is no longer this
object's to make. ``server.mode`` selects a worker class instead, in
:mod:`trpc_sdk.server._modes`.
"""

from __future__ import annotations

from trpc_sdk.abc import BaseServerTransport
from trpc_sdk.abc import BaseServerTransportOption
from trpc_sdk.abc import BaseTrpcServerEngine


class TrpcServerTransport(BaseServerTransport):
    """Register one service listen address on the server engine.

    The engine, not this object, owns listen sockets. Binding happens later
    on the worker chosen by ``server.mode``.
    """

    def __init__(self) -> None:
        self.opts = BaseServerTransportOption()
        self.server_engine = None

    def listen_and_serve(self, opts: BaseServerTransportOption, server_engine: BaseTrpcServerEngine) -> None:
        """Record this service's listen endpoint on the engine.

        Nothing is bound here. Binding happens in the worker, after the
        children have been forked, because a listen socket created before the
        fork would be shared with every child.

        Args:
            opts: Listen address, protocol checker, handler, and timeouts.
            server_engine: Engine collecting the endpoint tables.
        """
        self.opts = opts
        self.server_engine = server_engine
        self.server_engine.add_listen_endpoint(self.opts, self.opts.network)

    def close(self) -> None:
        """Forward close to the engine so it can stop its children."""
        self.server_engine.close()
