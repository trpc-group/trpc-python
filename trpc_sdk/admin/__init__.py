# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""tRPC-Python admin plugin.

The package is split by responsibility:

- :mod:`~trpc_sdk.admin._constants` — WS path, User-Agent prefix and other
  constants shared by both sides
- :mod:`~trpc_sdk.admin._loglevel` — log level accessors for the admin
  ``loglevel`` commands
- :mod:`~trpc_sdk.admin._hooks` — user callbacks run when the admin
  endpoints start
- :mod:`~trpc_sdk.admin._server` — admin HTTP server, run in the root process
- :mod:`~trpc_sdk.admin._client` — WS client each worker uses to connect back
  to the admin server
- :mod:`~trpc_sdk.admin._plugin` — :class:`~trpc_sdk.admin.AdminPlugin`,
  registered as the ``admin`` plugin on import

Importing this package registers the plugin and two process hooks;
``Server.serve`` imports it when ``server.admin`` is configured.
:meth:`~trpc_sdk.admin.AdminPlugin.on_master_start` runs from the
after-fork-in-parent hook, once in the root process after every child is
forked. :meth:`~trpc_sdk.admin.AdminPlugin.on_worker_start` runs from the
``ProcessType.WORKER`` child callback in every worker process; task processes
do not run it.
"""

from __future__ import annotations

from ._hooks import on_master_start
from ._hooks import on_master_starts
from ._hooks import on_worker_start
from ._hooks import on_worker_starts
from ._loglevel import get_log_level
from ._loglevel import set_log_level
from ._plugin import AdminPlugin


__all__ = [
    "AdminPlugin",
    "get_log_level",
    "on_master_start",
    "on_master_starts",
    "on_worker_start",
    "on_worker_starts",
    "set_log_level",
]
