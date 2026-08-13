# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.
"""Config data provider plugins.

Providers load raw config bytes from a data source. They are registered as
``CONFIG_PROVIDER`` plugins and are selected by name through
:class:`~trpc_sdk.config._parser.TrpcConfigParser` or
:func:`~trpc_sdk.config._options.with_provider`.

Named providers used by :func:`~trpc_sdk.config._options.with_provider` are kept in
this module's registry via :func:`set_provider` / :func:`get_provider`.
"""

from __future__ import annotations

from typing import Callable
from typing import Dict
from typing import Optional

from trpc_sdk.abc import BaseConfigProvider
from trpc_sdk.abc import PluginType
from trpc_sdk.plugin import register_plugin

from ._exceptions import ProviderError


@register_plugin(PluginType.CONFIG_PROVIDER, 'file')
class FileConfigProvider(BaseConfigProvider):
    """Reads raw config data from a local file.

    This provider is reachable under three different names: the class itself,
    the ``'FileConfigProvider'`` plugin name used by :class:`TrpcConfigParser`, and the
    ``'file'`` key via ``set_provider`` used by ``with_provider('file')``.
    """

    def __init__(self):
        super().__init__()
        self._options: Optional[Dict] = None

    def load(self, file_path: str, options: Optional[Dict] = None) -> bytes:
        """Load raw file from file on binary mode."""
        if file_path is None or file_path == '':
            file_path = options.get('file_path', None)
        if not file_path:
            raise ProviderError(data_source='FileProvider', msg='File datasource options should contain file_path')
        with open(file_path, 'rb') as f:
            return f.read()

    def hot_reload(self, key: str, callback: Callable, options: Optional[Dict] = None) -> None:
        """Hot-reload the config file and invoke ``callback`` when it changes.

        Can be used to hot-reload configuration at runtime.

        Args:
            key: Path or identifier of the configuration resource.
            callback: Callable invoked with the reloaded raw config bytes.
            options: Optional provider-specific options. May include
                ``interval`` (polling seconds) and ``event_loop``.
        """
        return

    def set_options(self, options: Dict) -> None:
        self._options = options
