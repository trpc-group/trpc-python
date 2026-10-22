# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Service-registry lookup.

:func:`get_registry` is the naming counterpart of :func:`get_selector`:
``get_plugin(PluginType.NAMING_REGISTRY, name)``. :data:`default_registry`
is the no-op default that :class:`~trpc_sdk.server._option.ServiceOption`
starts with when YAML does not name a registry plugin.
"""

from __future__ import annotations

from typing import Optional

from trpc_sdk.abc import BaseServiceRegistry
from trpc_sdk.abc import PluginType
from trpc_sdk.plugin import get_plugin

# No-op default used by ServiceOption until set_registry() loads a plugin.
default_registry: Optional[BaseServiceRegistry] = None


def get_registry(name: str) -> Optional[BaseServiceRegistry]:
    """Return the instantiated registry plugin registered as ``name``.

    Instantiates the plugin on first lookup if it is registered but not
    yet in the instance map.

    Args:
        name: Name passed to ``@register_plugin`` for
            ``PluginType.NAMING_REGISTRY``.

    Returns:
        The registry instance, or ``None`` if no plugin is registered
        under that name.
    """
    return get_plugin(PluginType.NAMING_REGISTRY, name)
