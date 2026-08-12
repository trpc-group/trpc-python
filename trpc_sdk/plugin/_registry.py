# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.
"""plugin interface"""


from __future__ import annotations


from typing import List
from typing import Callable
from typing import Optional

from trpc_sdk.abc import BasePlugin
from trpc_sdk.abc import PluginInitPos
from trpc_sdk.abc import PluginType
from trpc_sdk.abc import TaskInfo

from ._plugin import DEFAULT_PLUGIN_MANAGER
from ._plugin import PluginSpec



def register_plugin(plugin_type: PluginType,
                    plugin_name: str,
                    init_pos: PluginInitPos = PluginInitPos.WORKER,
                    version: str = "",
                    )->Callable[[type[BasePlugin]], type[BasePlugin]]:
    """
    Register plugin decorator

    Args:
        plugin_type: plugin type
        plugin_name: plugin name
        init_pos: init position
        version: plugin version
    Returns:
        None
    """

    def decorator(cls):
        plugin_spec = PluginSpec(cls=cls, init_pos=init_pos, plugin_type=plugin_type, 
                                 plugin_name=plugin_name, version=version, task_info=None)
        DEFAULT_PLUGIN_MANAGER.register(plugin_spec)

        return cls

    return decorator

def register_task(task_name: str, task_info: TaskInfo):
    """Register task
    Args:
        task_name: task name
        task_info: task info
    Returns:
        None
    """
    plugin_spec = PluginSpec(cls=None, init_pos=PluginInitPos.TASK, plugin_type=PluginType.TASK, 
                            plugin_name=task_name, version="", task_info=task_info)
    plugin_spec.task_info.name = task_name
    DEFAULT_PLUGIN_MANAGER.register(plugin_spec)



def get_plugin(plugin_type: PluginType, plugin_name: str)->Optional[BasePlugin]:
    """
    Get plugin, note: this method must be called after setup

    Args:
        plugin_type: plugin type
        plugin_name: plugin name
    Returns:
        Plugin instance
    """
    if not DEFAULT_PLUGIN_MANAGER.find_instance(plugin_type, plugin_name):
        plugin_spec = DEFAULT_PLUGIN_MANAGER.get_plugin_spec(plugin_type, plugin_name)
        if not plugin_spec or plugin_spec.cls is None:
            return None
        plugin_instance = plugin_spec.cls()
        plugin_instance.init()
        if not plugin_instance.name:
            plugin_instance._name = plugin_name
        if plugin_instance.type == PluginType.UNKNOWN:
            plugin_instance._type= plugin_type
        plugin_instance.start()
        DEFAULT_PLUGIN_MANAGER.set_instance(plugin_instance)

    return DEFAULT_PLUGIN_MANAGER.get_instance(plugin_type, plugin_name)


def set_plugin_order(plugin_type: PluginType, plugin_name_sequence: List[str]):
    """Set plugin order
    Set the order of plugins of the same type
    Args:
        plugin_type: plugin type
        plugin_name_sequence: plugin name sequence
    Returns:
        None
    """
    DEFAULT_PLUGIN_MANAGER.set_plugin_order(plugin_type, plugin_name_sequence)


