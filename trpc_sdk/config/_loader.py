# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Loader for configuration files.

This is separate from the framework's own startup config (see
:class:`~trpc_sdk.config.TrpcConfig`): it is what business code uses to load its own
files. A :class:`ConfigLoader` binds one config path to a data provider (which
reads raw bytes) and a decoder (which turns those bytes into a dict). Loaders
are normally built through :class:`ConfigLoaderFactory` and the option helpers
in :mod:`~trpc_sdk.config._options`::

    from trpc_sdk.config import ConfigLoaderFactory, with_codec, with_provider

    cfg = ConfigLoaderFactory.sync_load(
        "my.yaml", with_provider("file"), with_codec("YamlDecoder")
    )
    cfg.get("server", {})
"""

from __future__ import annotations

from inspect import iscoroutinefunction
from typing import Any
from typing import Callable
from typing import Dict
from typing import Optional

from trpc_sdk.abc import BaseDataProvider
from trpc_sdk.abc import BaseDecoder
from trpc_sdk.utils import get_or_create_event_loop
from ._exceptions import ConfigError


class ConfigLoader:
    """Loads one config path through a data provider and a decoder.

    Args:
        path: Path (or key) identifying the config within the provider.
        provider: Data provider returning the raw config bytes.
        decoder: Decoder turning the raw bytes into a dict.
    """

    def __init__(
        self,
        path: str,
        provider: Optional[BaseDataProvider] = None,
        decoder: Optional[BaseDecoder] = None,
    ):
        self.path = path
        self.provider = provider
        self.decoder = decoder
        self.config_data: Optional[Dict] = None

    async def load(self) -> Optional[Dict]:
        """Read the raw config through the provider and decode it.

        Returns:
            The decoded config data.
        """
        if iscoroutinefunction(self.provider.load):
            data = await self.provider.load(self.path)
        else:
            data = self.provider.load(self.path)
        self.config_data = self.decoder.decode(data)
        return self.config_data

    def get(self, key: str, default: Any) -> Any:
        """Get a decoded config value by key.

        Args:
            key: The config key to read.
            default: Value returned when the key is absent.

        Returns:
            The config value, or ``default``.
        """
        return self.config_data.get(key, default)

    def hot_reload(self, callback: Optional[Callable] = None) -> None:
        """Re-decode the config whenever the provider reports a change.

        Args:
            callback: Invoked with the newly decoded config data.
        """

        def _on_change(raw_data):
            self.config_data = self.decoder.decode(raw_data)
            if callback is not None:
                callback(self.config_data)

        self.provider.hot_reload(self.path, _on_change)


class ConfigLoaderFactory:
    """Entry points that build and load a :class:`ConfigLoader`.

    State lives nowhere; every entry point is a classmethod. Callers use the
    class itself rather than an instance::

    """

    def __init__(self) -> None:
        raise TypeError(
            "ConfigLoaderFactory only exposes classmethods; call them on the class."
        )

    @classmethod
    def create(cls, path: str, *options: Callable) -> ConfigLoader:
        """Create a config loader and apply the given options.

        Args:
            path: Path (or key) identifying the config within the provider.
            options: Options such as
                :func:`~trpc_sdk.config._options.with_provider` /
                :func:`~trpc_sdk.config._options.with_codec`.

        Returns:
            A ready-to-load config loader.

        Raises:
            ConfigError: If no decoder or no provider was selected.
        """
        config = ConfigLoader(path)
        for option in options:
            option(config)

        if config.decoder is None:
            raise ConfigError(f'No decoder selected for config {path}')

        if config.provider is None:
            raise ConfigError(f'No data provider selected for config {path}')

        return config

    @classmethod
    async def load(cls, path: str, *options: Callable) -> ConfigLoader:
        """Create a config loader and load it.

        Args:
            path: Path (or key) identifying the config within the provider.
            options: Options such as
                :func:`~trpc_sdk.config._options.with_provider` /
                :func:`~trpc_sdk.config._options.with_codec`.

        Returns:
            The loaded config loader.
        """
        config_loader = cls.create(path, *options)
        await config_loader.load()
        return config_loader

    @classmethod
    def sync_load(cls, path: str, *options: Callable) -> ConfigLoader:
        """Synchronous wrapper around :meth:`load`.

        Args:
            path: Path (or key) identifying the config within the provider.
            options: Options such as
                :func:`~trpc_sdk.config._options.with_provider` /
                :func:`~trpc_sdk.config._options.with_codec`.

        Returns:
            The loaded config loader.
        """
        return get_or_create_event_loop().run_until_complete(cls.load(path, *options))
