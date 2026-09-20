# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Shared lookup helpers for naming selector plugins.

:func:`get_selector` used to be copied in ``ip_selector``,
``dsn_selector``, and ``trpc_selector``. All three were
``get_plugin(PluginType.NAMING_SELECTOR, name)``. The public modules
re-export these functions so existing imports keep working.
"""

from __future__ import annotations

from typing import Optional

from trpc_sdk.abc import BaseSelector
from trpc_sdk.abc import PluginType
from trpc_sdk.plugin import get_plugin
from ._constants import DEFAULT_SELECTOR_NAME


def get_selector(selector_name: str) -> Optional[BaseSelector]:
    """Return the instantiated selector plugin registered as ``selector_name``.

    Instantiates the plugin on first lookup if it is registered but not
    yet in the instance map. Callers such as
    :func:`trpc_sdk.client._client.select_node` use this after splitting a
    ``scheme://endpoint`` target.

    Args:
        selector_name: Name passed to ``@register_plugin`` for
            ``PluginType.NAMING_SELECTOR`` (``ip``, ``dns``, ``dsn``, ...).

    Returns:
        The selector instance, or ``None`` if no plugin is registered
        under that name.
    """
    return get_plugin(PluginType.NAMING_SELECTOR, selector_name)


def get_default_selector() -> Optional[BaseSelector]:
    """Return the DSN selector, the framework's built-in default resolver.

    Equivalent to ``get_selector(DEFAULT_SELECTOR_NAME)``.
    :mod:`trpc_sdk.naming._dsn_selector` re-exports this as
    ``default_selector`` for historical callers.
    """
    return get_selector(DEFAULT_SELECTOR_NAME)
