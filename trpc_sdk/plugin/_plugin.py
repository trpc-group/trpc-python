# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.
"""plugin interface"""

from __future__ import annotations

from collections import OrderedDict
from collections import defaultdict
from dataclasses import dataclass
from typing import Dict
from typing import List
from typing import Optional

from trpc_sdk.abc import BasePlugin
from trpc_sdk.abc import PluginInitPos
from trpc_sdk.abc import PluginType
from trpc_sdk.abc import TaskInfo


@dataclass
class PluginSpec:
    """Plugin specification"""

    cls: Optional[type[BasePlugin]]
    init_pos: PluginInitPos
    plugin_type: PluginType
    plugin_name: str
    task_info: Optional[TaskInfo] = None
    version: str = ""


class _PluginManager:
    """Plugin manager"""

    def __init__(self):
        self._plugin_specs: Dict[PluginType, Dict[str, PluginSpec]] = defaultdict(dict)
        self._plugin_instances: Dict[PluginType, Dict[str, BasePlugin]] = defaultdict(dict)
        self._plugin_spec_sequence: Dict[PluginType, List[str]] = defaultdict(list)

    def set_plugin_order(self, plugin_type: PluginType, plugin_name_sequence: List[str]):
        """Set plugin order"""
        self._plugin_spec_sequence[plugin_type] = plugin_name_sequence

    def adjust_plugin_order(self, plugin_type: PluginType):
        """Adjust plugin order"""
        if plugin_type not in self._plugin_spec_sequence or plugin_type not in self._plugin_specs:
            return
        plugin_specs = self._plugin_specs[plugin_type]
        plugin_name_sequence = self._plugin_spec_sequence[plugin_type]
        ordered_dict = OrderedDict()

        for name, spec in plugin_specs.items():
            if name not in plugin_name_sequence:
                ordered_dict[name] = spec

        for name in plugin_name_sequence:
            if name in plugin_specs:
                ordered_dict[name] = plugin_specs[name]
        self._plugin_specs[plugin_type] = ordered_dict

    def register(self, plugin_spec: PluginSpec):
        """
        Register plugin

        Args:
            plugin_spec: plugin specification
        """
        self._plugin_specs[plugin_spec.plugin_type][plugin_spec.plugin_name] = plugin_spec

    def get_plugin_spec(self, plugin_type: PluginType, plugin_name: str) -> Optional[PluginSpec]:
        """
        Get plugin specification by plugin type and plugin name

        Args:
            plugin_type: plugin type
            plugin_name: plugin name
        Returns:
            Plugin specification
        """
        return self._plugin_specs[plugin_type].get(plugin_name, None)

    def find_instance(self, plugin_type: PluginType, plugin_name: str) -> bool:
        """
        Check if plugin instance exists by plugin type and plugin name

        Args:
            plugin_type: plugin type
            plugin_name: plugin name
        Returns:
            True if plugin instance exists, False otherwise
        """
        return plugin_type in self._plugin_instances and plugin_name in self._plugin_instances[plugin_type]

    def get_instance(self, plugin_type: PluginType, plugin_name: str) -> Optional[BasePlugin]:
        """
        Get plugin instance by plugin type and plugin name

        Args:
            plugin_type: plugin type
            plugin_name: plugin name
        Returns:
            Plugin instance
        """
        return self._plugin_instances[plugin_type].get(plugin_name, None)

    def set_instance(self, instance: BasePlugin) -> None:
        """
        Set plugin instance

        Args:
            instance: plugin instance
        """
        self._plugin_instances[instance.type][instance.name] = instance

    def remove_plugin_spec(self, plugin_spec: PluginSpec) -> None:
        """
        Remove plugin specification

        Args:
            plugin_spec: plugin specification
        """
        del self._plugin_specs[plugin_spec.plugin_type][plugin_spec.plugin_name]

    def all_plugin_specs(self) -> List[PluginSpec]:
        """Get all plugin specifications"""
        specs = [spec for v in self._plugin_specs.values() for spec in v.values()]
        return specs

    def all_plugin_instances(self) -> List[BasePlugin]:
        """Get all plugin instances"""
        instances = [instance for v in self._plugin_instances.values() for instance in v.values()]
        return instances

    def all_plugin_specs_by_type(self, plugin_type: PluginType) -> List[PluginSpec]:
        """Get all plugin specifications by plugin type
        Args:
            plugin_type: plugin type
        Returns:
            List of plugin specifications
        """
        return [spec for spec in self._plugin_specs[plugin_type].values()]

    def all_plugin_instances_by_type(self, plugin_type: PluginType) -> List[BasePlugin]:
        """Get all plugin instances by plugin type
        Args:
            plugin_type: plugin type
        Returns:
            List of plugin instances
        """
        return [instance for instance in self._plugin_instances[plugin_type].values()]


DEFAULT_PLUGIN_MANAGER = _PluginManager()
