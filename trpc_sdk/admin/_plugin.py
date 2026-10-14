# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Admin plugin that starts the admin HTTP server and worker WS clients."""

from __future__ import annotations

import asyncio
import threading
from typing import Tuple

from trpc_sdk.abc import BasePlugin
from trpc_sdk.abc import PluginInitPos
from trpc_sdk.abc import PluginType
from trpc_sdk.common import ProcessType
from trpc_sdk.common import register_at_fork
from trpc_sdk.common import register_child_callback
from trpc_sdk.config import TrpcConfig
from trpc_sdk.plugin import register_plugin
from trpc_sdk.utils import get_async_lib
from ._hooks import on_master_starts
from ._hooks import on_worker_starts


@register_plugin(PluginType.ADMIN, 'admin', init_pos=PluginInitPos.WORKER)
class AdminPlugin(BasePlugin):
    """Run the admin endpoints on daemon threads with their own event loops.

    There is no master process: the process that calls ``Server.serve`` is the
    root worker. The two sides are driven by process hooks registered at the
    bottom of this module, not by the plugin manager:

    - :meth:`on_master_start` serves the admin HTTP API from ``_server``. It
      runs once, in the root process, from the after-fork-in-parent hook, so
      no child inherits the server thread or its listen socket.
    - :meth:`on_worker_start` connects back to that API through the WS client
      in ``_client``. It runs from the ``ProcessType.WORKER`` child callback in
      every worker process; task processes do not connect.
    """

    def get_config(self, master: bool) -> Tuple[str, int]:
        """Return the admin address from ``server.admin``.

        Args:
            master: ``True`` for the HTTP server bind address. ``False`` for
                the address a worker dials; ``0.0.0.0`` is then rewritten to
                ``127.0.0.1``.

        Returns:
            ``(ip, port)``, or ``('', 0)`` when ``server.admin`` is absent.
        """
        cfg = TrpcConfig.server_config().admin
        if not cfg:
            return '', 0
        ip, port = cfg.ip, cfg.port
        if not master and ip == '0.0.0.0':
            ip = '127.0.0.1'
        return ip, port

    def on_master_start(self) -> None:
        """Run user master callbacks, then serve the admin HTTP API on a daemon thread."""
        ip, port = self.get_config(True)
        if not ip or not port:
            return

        from . import _server  # pylint: disable=import-outside-toplevel
        for func in on_master_starts:
            func(_server.routes, _server.configer)

        def run():
            asynclib = get_async_lib(TrpcConfig.global_config().enable_uvloop)
            asyncio.set_event_loop(asynclib.new_event_loop())
            _server.run(ip, port, print=None, access_log=None, handle_signals=False)

        threading.Thread(target=run, daemon=True).start()

    def on_worker_start(self) -> None:
        """Run user worker callbacks, then connect the admin WS client on a daemon thread."""
        ip, port = self.get_config(False)
        if not ip or not port:
            return

        from . import _client  # pylint: disable=import-outside-toplevel
        for func in on_worker_starts:
            func(_client.configer)

        def run():
            asynclib = get_async_lib(TrpcConfig.global_config().enable_uvloop)
            loop = asynclib.new_event_loop()
            asyncio.set_event_loop(loop)
            loop.run_until_complete(_client.run(ip, port))

        threading.Thread(target=run, daemon=True).start()


def _run_master_start() -> None:
    AdminPlugin().on_master_start()


def _run_worker_start() -> None:
    AdminPlugin().on_worker_start()


register_at_fork(after_in_parent=_run_master_start)
register_child_callback(_run_worker_start, ProcessType.WORKER)
