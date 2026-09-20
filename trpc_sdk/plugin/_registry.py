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
from typing import TypeVar

from trpc_sdk.abc import BasePlugin
from trpc_sdk.abc import PluginInitPos
from trpc_sdk.abc import PluginType
from trpc_sdk.abc import TaskInfo
from ._plugin import DEFAULT_PLUGIN_MANAGER
from ._plugin import PluginSpec


PluginFactoryType = TypeVar("PluginFactoryType", bound=Callable[[], object])
"""Anything ``register_plugin`` accepts: a plugin class or a factory."""


def register_plugin(
    plugin_type: PluginType,
    plugin_name: str,
    init_pos: PluginInitPos = PluginInitPos.WORKER,
    version: str = "",
) -> Callable[[PluginFactoryType], PluginFactoryType]:
    """
    Register plugin decorator

    The decorated object only has to be callable with no arguments. A
    :class:`~trpc_sdk.abc.BasePlugin` subclass is the common case and is
    the only one that gets the ``init`` / ``start`` lifecycle, but a slot
    may also be filled by a factory function or a Cython extension type.

    Args:
        plugin_type: plugin type
        plugin_name: plugin name
        init_pos: init position
        version: plugin version
    Returns:
        None
    """

    def decorator(cls):
        plugin_spec = PluginSpec(cls=cls,
                                 init_pos=init_pos,
                                 plugin_type=plugin_type,
                                 plugin_name=plugin_name,
                                 version=version,
                                 task_info=None)
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
    plugin_spec = PluginSpec(cls=None,
                             init_pos=PluginInitPos.TASK,
                             plugin_type=PluginType.TASK,
                             plugin_name=task_name,
                             version="",
                             task_info=task_info)
    plugin_spec.task_info.name = task_name
    DEFAULT_PLUGIN_MANAGER.register(plugin_spec)


def get_plugin(plugin_type: PluginType, plugin_name: str) -> Optional[object]:
    """
    Get plugin, building it on first use if no eager pass covered it

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
        # Mirrors ``init_plugins``: the lifecycle only applies to a
        # BasePlugin. Factory functions and Cython extension types fill
        # their slot as soon as they are built.
        if isinstance(plugin_instance, BasePlugin):
            plugin_instance.init()
            if not plugin_instance.name:
                plugin_instance._name = plugin_name
            if plugin_instance.type == PluginType.UNKNOWN:
                plugin_instance._type = plugin_type
            if plugin_instance.init_pos == PluginInitPos.UNKNOWN:
                plugin_instance._init_pos = plugin_spec.init_pos
            plugin_instance.start()
        DEFAULT_PLUGIN_MANAGER.set_instance(plugin_type, plugin_name, plugin_instance)

    return DEFAULT_PLUGIN_MANAGER.get_instance(plugin_type, plugin_name)


def get_plugin_class(plugin_type: PluginType, plugin_name: str) -> Optional[Callable[[], object]]:
    """
    Get plugin class
    Args:
        plugin_type: plugin type
        plugin_name: plugin name
    Returns:
        Plugin class
    """
    plugin_spec = DEFAULT_PLUGIN_MANAGER.get_plugin_spec(plugin_type, plugin_name)
    if not plugin_spec or plugin_spec.cls is None:
        return None
    return plugin_spec.cls


def get_registered_tasks() -> List[TaskInfo]:
    """Return ``task_info`` for every registered TASK plugin.

    :mod:`trpc_sdk.server._task` imports this name at module load even
    when nothing has registered a task.

    Returns:
        TaskInfo records from TASK plugin specs. Specs without
        ``task_info`` are skipped.
    """
    specs = DEFAULT_PLUGIN_MANAGER.all_plugin_specs_by_type(PluginType.TASK)
    return [spec.task_info for spec in specs if spec.task_info is not None]


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
