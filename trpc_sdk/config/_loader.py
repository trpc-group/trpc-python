# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.
"""Load application-owned configuration through a provider and a decoder.

This module is intended for configuration files owned by application code. It
is independent of the tRPC framework startup configuration managed by
:class:`~trpc_sdk.config.TrpcConfig`.

The loading pipeline has two replaceable components:

1. A data provider decides where configuration data comes from. It receives
   ``path`` and returns raw bytes. A provider may read a local file, a remote
   configuration service, a database, or any other data source.
2. A decoder decides how the raw bytes are interpreted. It converts YAML, JSON,
   or another serialization format into a dictionary.

Keeping these responsibilities separate allows the same provider to be paired
with different decoders and the same decoder to be reused with different data
sources.

The following example loads a YAML file directly::

    import asyncio

    from trpc_sdk.config import ConfigLoader
    from trpc_sdk.config import FileDataProvider
    from trpc_sdk.config import YamlDecoder

    loader = ConfigLoader(
        "application.yaml",
        provider=FileDataProvider(),
        decoder=YamlDecoder(),
    )
    config = asyncio.run(loader.load())
    server_config = loader.get("server", {})

After named providers have been registered during plugin startup, applications
can use :class:`ConfigLoaderFactory` with the option helpers from
:mod:`trpc_sdk.config._options`::

    from trpc_sdk.config import ConfigLoaderFactory
    from trpc_sdk.config import with_codec
    from trpc_sdk.config import with_provider

    loader = ConfigLoaderFactory.sync_load(
        "application.yaml",
        with_provider("file"),
        with_codec("YamlDecoder"),
    )
    server_config = loader.get("server", {})

Custom integrations implement :class:`~trpc_sdk.abc.BaseConfigProvider` and
:class:`~trpc_sdk.abc.BaseConfigDecoder`. Providers may implement ``load`` either
synchronously or asynchronously; :meth:`ConfigLoader.load` handles both forms.
"""

from __future__ import annotations

from inspect import iscoroutinefunction
from typing import Any
from typing import Callable
from typing import Dict
from typing import Optional

from trpc_sdk.abc import BaseConfigProvider
from trpc_sdk.abc import BaseConfigDecoder
from trpc_sdk.common import get_or_create_event_loop

from ._exceptions import ConfigError


class ConfigLoader:
    """Load one application configuration resource.

    ``ConfigLoader`` coordinates a provider and a decoder but does not impose a
    particular storage location or serialization format. Calling :meth:`load`
    performs the following steps:

    1. Pass ``path`` to the provider and obtain raw configuration bytes.
    2. Pass those bytes to the decoder and obtain decoded configuration data.
    3. Cache the decoded result in :attr:`config_data` for :meth:`get`.

    Args:
        path: Provider-specific resource identifier. For a file provider this
            is a file path; remote providers may interpret it as a key.
        provider: Data provider that returns raw configuration bytes.
        decoder: Decoder that converts the raw bytes into configuration data.
    """

    def __init__(
        self,
        path: str,
        provider: Optional[BaseConfigProvider] = None,
        decoder: Optional[BaseConfigDecoder] = None,
    ):
        self._path = path
        self._provider = provider
        self._decoder = decoder
        self._config_data: Optional[Dict] = None

    @property
    def path(self) -> str:
        """Return the path of the config."""
        return self._path

    @property
    def provider(self) -> BaseConfigProvider:
        """Return the provider of the config."""
        return self._provider

    def set_provider(self, provider: BaseConfigProvider) -> None:
        """Set the provider of the config."""
        self._provider = provider

    @property
    def decoder(self) -> BaseConfigDecoder:
        """Return the decoder of the config."""
        return self._decoder

    def set_decoder(self, decoder: BaseConfigDecoder) -> None:
        """Set the decoder of the config."""
        self._decoder = decoder

    @property
    def config_data(self) -> Optional[Dict]:
        """Return the decoded config data."""
        return self._config_data

    async def load(self) -> Optional[Dict]:
        """Load and decode the configured resource.

        Synchronous providers are called directly. Asynchronous providers are
        awaited automatically. The resulting raw bytes are passed unchanged to
        the decoder, and the decoded value is cached in :attr:`config_data`.

        Returns:
            The decoded configuration data.
        """
        if iscoroutinefunction(self.provider.load):
            data = await self._provider.load(self._path)
        else:
            data = self._provider.load(self._path)
        self._config_data = self._decoder.decode(data)
        return self._config_data

    def get(self, key: str, default: Any) -> Any:
        """Return a value from the most recently decoded configuration.

        Call :meth:`load` before using this method.

        Args:
            key: The config key to read.
            default: Value returned when the key is absent.

        Returns:
            The config value, or ``default``.
        """
        return self._config_data.get(key, default)

    def hot_reload(self, callback: Optional[Callable] = None, options: Optional[Dict] = None) -> None:
        """Watch the resource and decode every update reported by the provider.

        Hot-reload behavior is provider-specific. The provider watches ``path``
        and invokes an internal callback with new raw bytes. The loader decodes
        those bytes, replaces :attr:`config_data`, and then invokes the optional
        application callback.

        Args:
            callback: Function invoked with the newly decoded configuration.
            options: Provider-specific watch or polling options.
        """

        def _on_change(raw_data):
            self._config_data = self._decoder.decode(raw_data)
            if callback is not None:
                callback(self._config_data)

        self._provider.hot_reload(self._path, _on_change, options)


class ConfigLoaderFactory:
    """Build configured loaders through composable option functions.

    The factory has no instance state. Use its class methods with options such
    as :func:`~trpc_sdk.config.with_provider` and
    :func:`~trpc_sdk.config.with_codec`.
    """

    def __init__(self) -> None:
        raise TypeError("ConfigLoaderFactory only exposes classmethod; call it on the class.")

    @classmethod
    def create(cls, path: str, *options: Callable) -> ConfigLoader:
        """Create a configured loader without reading the resource.

        Options are applied in the order provided. A valid loader requires
        exactly one selected provider and decoder, although later options may
        replace values set by earlier options.

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

        if config._decoder is None:
            raise ConfigError(f'No decoder selected for config {path}')

        if config._provider is None:
            raise ConfigError(f'No data provider selected for config {path}')

        return config

    @classmethod
    async def load(cls, path: str, *options: Callable) -> ConfigLoader:
        """Create a loader and asynchronously load its configuration.

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
        """Create and load a configuration from synchronous application code.

        This method runs :meth:`load` on the current event loop and returns the
        loader so callers can use both :attr:`ConfigLoader.config_data` and
        :meth:`ConfigLoader.get`.

        Args:
            path: Path (or key) identifying the config within the provider.
            options: Options such as
                :func:`~trpc_sdk.config._options.with_provider` /
                :func:`~trpc_sdk.config._options.with_codec`.

        Returns:
            The loaded config loader.
        """
        return get_or_create_event_loop().run_until_complete(cls.load(path, *options))
