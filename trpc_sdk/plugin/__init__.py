# Tencent is pleased to support the open source community by making tRPC-Agent-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Agent-Python is licensed under Apache-2.0.
"""tRPC-Agent-Python plugin module"""

from __future__ import annotations

from ._builder import init_plugins
from ._builder import init_worker_plugins
from ._builder import init_task_plugins
from ._exceptions import PluginInitError
from ._registry import DEFAULT_PLUGIN_MANAGER
from ._registry import register_plugin
from ._registry import register_task
from ._registry import get_plugin
from ._registry import get_plugin_class
from ._registry import set_plugin_order

__all__ = [
    "init_plugins",
    "init_worker_plugins",
    "init_task_plugins",
    "PluginInitError",
    "register_plugin",
    "register_task",
    "get_plugin",
    "get_plugin_class",
    "set_plugin_order",
    "DEFAULT_PLUGIN_MANAGER",
]
