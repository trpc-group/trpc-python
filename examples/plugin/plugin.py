# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""User-defined plugin read from ``plugins.business``.

Import this module before ``create_server``. ``@register_plugin`` only
stores a spec. ``init`` runs later in the worker, after the YAML is loaded.
The framework does not build this plugin from the ``plugins:`` key.
"""

from __future__ import annotations

from trpc_sdk.abc import BasePlugin
from trpc_sdk.abc import PluginType
from trpc_sdk.config import get_plugins_config
from trpc_sdk.plugin import register_plugin


@register_plugin(PluginType.USER_DEFINED, "business")
class BusinessPlugin(BasePlugin):
    """Hold ``plugins.business.token`` and format the SayHello reply."""

    def __init__(self) -> None:
        super().__init__()
        self.token = ""

    def init(self) -> None:
        """Copy ``plugins.business.token`` onto this instance.

        ``get_plugins_config`` is ``None`` when the file has no ``plugins``
        key. A missing token stays ``""``.
        """
        plugins = get_plugins_config() or {}
        cfg = plugins.get("business") or {}
        self.token = cfg.get("token", "")

    def apply(self, msg: str) -> str:
        """Return the SayHello reply text.

        Args:
            msg: ``HelloRequest.msg``.

        Returns:
            ``hello: {msg} token={token}``.
        """
        return f"hello: {msg} token={self.token}"
