# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.
"""plugin builder"""

from __future__ import annotations

from typing import Optional

from trpc_sdk.abc import PluginInitPos
from trpc_sdk.abc import PluginType

from ._exceptions import PluginInitError
from ._registry import DEFAULT_PLUGIN_MANAGER


def init_plugins(pos: Optional[PluginInitPos] = None, without_user_define: bool = False) -> None:
    """init plugins
    Args:
        pos: plugin init position
        without_user_define: whether to init user defined plugins
    Returns:
        None
    """
    for plugin_spec in DEFAULT_PLUGIN_MANAGER.all_plugin_specs():
        plugin_type = plugin_spec.plugin_type
        if without_user_define and plugin_type == PluginType.USER_DEFINED:
            continue
        DEFAULT_PLUGIN_MANAGER.adjust_plugin_order(plugin_type)
        seq_plugin_specs = DEFAULT_PLUGIN_MANAGER.all_plugin_specs_by_type(plugin_type)
        for plugin_info in seq_plugin_specs:
            if pos is None or plugin_info.init_pos == pos:
                if plugin_info.cls is None:
                    continue
                try:
                    plugin_instance = plugin_info.cls()
                    plugin_instance.init()
                    if not plugin_instance.name:
                        plugin_instance._name = plugin_info.plugin_name
                    if plugin_instance.type == PluginType.UNKNOWN:
                        plugin_instance._type = plugin_info.plugin_type
                    if plugin_instance.init_pos == PluginInitPos.UNKNOWN:
                        plugin_instance._init_pos = plugin_info.init_pos
                    plugin_instance.start()
                    DEFAULT_PLUGIN_MANAGER.set_instance(plugin_instance)
                except PluginInitError:
                    # if plugin init error, raise exception
                    raise
                except Exception as e:  # pylint: disable=broad-except
                    # if other error, print error and remove plugin spec
                    print(f"Fail to setup err: {e}, plugin type: {plugin_info.plugin_type},"
                          f"name: {plugin_info.plugin_name}")
                    DEFAULT_PLUGIN_MANAGER.remove_plugin_spec(plugin_info)


def init_worker_plugins():
    """init worker plugins
    """
    init_plugins(pos=PluginInitPos.WORKER, without_user_define=False)


def init_task_plugins():
    """init task plugins"""
    init_plugins(pos=PluginInitPos.TASK, without_user_define=True)
