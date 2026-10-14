# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""tRPC-Python server module: the service layer.

The application parses the YAML config into a :class:`Server`, which builds
one :class:`ServiceAdapter` per ``server.service[]`` entry and then hands the process
over to :mod:`trpc_sdk.runtime`. Everything about processes and threads —
the worker and its task children, ``merge`` / ``separate`` modes, serving
loops, signals and heartbeats — lives in :mod:`trpc_sdk.runtime`.

The package is split by responsibility:

- :mod:`~trpc_sdk.server._option` — ``ServerOption`` (process model) and
  ``ServiceOption`` (one listen endpoint), both filled from YAML
- :mod:`~trpc_sdk.server._cython` — ``ServiceAdapter``: method registry, filter chain,
  the per-request handler path, and :func:`create_service`
- :mod:`~trpc_sdk.server._registry` — ``ServiceRegistrar`` /
  ``start_service_registry``: per-service naming-service register /
  heartbeat / deregister on a ``RegistryThread`` with its own event loop
- :mod:`~trpc_sdk.server._server` — ``Server``: config, service table,
  :func:`create_server`, and the startup order that forks children before
  creating a loop
- :mod:`~trpc_sdk.server._engine` — ``TrpcServerEngine`` and its
  listen-endpoint tables
- :mod:`~trpc_sdk.server._utils` — the bind-info file written by
  :func:`create_server`
- :mod:`~trpc_sdk.server._constants`, :mod:`~trpc_sdk.server._types` —
  server constants and the queue-wait window used by ``ServiceAdapter``

Application code normally reaches this package through :func:`create_server`,
which returns a :class:`Server`. Generated stub code uses :class:`Server`,
:class:`Method`, :class:`ServiceDesc` and :class:`StreamDesc` to register
handlers.
"""

from trpc_sdk.abc import Method
from trpc_sdk.abc import ServiceDesc
from trpc_sdk.abc import StreamDesc
from trpc_sdk.abc import TrpcContext
from trpc_sdk.runtime import Network
from trpc_sdk.runtime import register_timeout_callback
from trpc_sdk.transport import register_protocol_handle

from ._engine import TrpcServerEngine
from ._option import ServerOption
from ._option import ServiceOption
from ._server import Server
from ._server import create_server
from ._cython import ServiceAdapter
from ._cython import create_service

__all__ = [
    # Entry points
    "Server",
    "ServerOption",
    "ServiceAdapter",
    "ServiceOption",
    "TrpcServerEngine",
    "create_server",
    "create_service",
    # Handler-facing types, used by generated stub code
    "TrpcContext",
    "Method",
    "ServiceDesc",
    "StreamDesc",
    # Server configuration and extension hooks
    "Network",
    "register_protocol_handle",
    "register_timeout_callback",
]
